#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import os, re, pickle

from loguru import logger

# ORM model specs

import sqlalchemy
from sqlalchemy import Column, String, Integer, Binary
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class Record(Base):
    __tablename__ = 'records'
    __table_args__ = {'schema':'marc'}

    type = Column(String(4), primary_key=True, nullable=False)
    ctrlno = Column(Integer, primary_key=True, nullable=False)
    record = Column(Binary, nullable=False)

    def __repr__(self):
        return f'<Record: {self.type} {self.ctrlno}>'

class HoldingsLink(Base):
    __tablename__ = 'holdings_links'
    __table_args__ = {'schema':'marc'}

    hdg_ctrlno = Column(String(80), primary_key=True, nullable=False)
    bib_ctrlno = Column(String(80), nullable=False)

    def __repr__(self):
        return f'<HoldingsLink {self.hdg_ctrlno} -> {self.bib_ctrlno}>'

class Version(Base):
    __tablename__ = 'version'
    __table_args__ = {'schema':'marc'}

    version = Column(String(13), primary_key=True, nullable=False)

    def __repr__(self):
        return f'<Version {self.version}>'

from .VoyagerAPI import VoyagerAPI
from .LaneMARCRecord import LaneMARCRecord

from .config import SQLALCHEMY_DATABASE_URI

# create engine and session maker
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)
Session = sqlalchemy.orm.sessionmaker(bind=engine)

class LMLDB:
    """
    Interface for creating/accessing a postgres-based mirror of the Lane MARC catalog
    """
    def __init__(self, mode='r', version=0, cache_bibmfhd_links=True) -> None:
        assert mode in 'rwa', f"invalid mode: {mode}"
        self.mode = mode
        if mode == 'r' and version != 0:
            logger.warning(f"read mode, version ({version}) ignored")
        assert isinstance(version, int) or version.isdigit(), \
            f"version must be int: {version}"
        self.version = str(int(version))
        # establish session
        self.session = Session()
        if mode != 'w' and not self.__check_integrity():
            logger.error("lmldb integrity check failed, changing mode to w")
            self.mode = 'w'
        if self.mode == 'w':
            self.__init_db()
        self.cache_bibmfhd_links = cache_bibmfhd_links

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.mode != 'r':
            self.__update_version()
        self.session.close()

    def __check_integrity(self) -> bool:
        # all tables exist and there is at least one row in each
        try:
            for obj in (Version, Record, HoldingsLink):
                if self.session.query(obj).first() is None:
                    return False
        except:
            return False
        else:
            return True

    def __init_db(self) -> None:
        logger.info("reinitializing lmldb")
        # Create schema
        engine.execute("CREATE SCHEMA IF NOT EXISTS marc")
        # Create tables
        Base.metadata.create_all(engine)

    def __update_version(self) -> None:
        self.session.query(Version).delete()
        self.session.add(Version(version=self.version))
        self.session.commit()

    def get_version(self) -> int:
        try:
            v = self.session.query(Version).first()
            return int(v.version)
        except:
            return 0

    BIB, AUT, HDG = VoyagerAPI.BIB, VoyagerAPI.AUT, VoyagerAPI.HDG
    def populate(self, record_type, marc_reader) -> list:
        """insert record, return list of ctrlnos"""
        ctrlnos = []
        # expects pymarc MARCReader
        add_function = { self.BIB : self.__add_bib,
                         self.AUT : self.__add_aut,
                         self.HDG : self.__add_hdg }.get(record_type)
        if add_function is None:
            raise ValueError(f'invalid record_type: {record_type}')
        for record in marc_reader:
            add_function(record)
            ctrlnos.append(record['001'].data)
        self.session.commit()
        # logger.debug(f"{ctrlnos}")
        return ctrlnos

    def __add_bib(self, bib_record) -> None:
        bib_record.__class__ = LaneMARCRecord
        ctrlno = bib_record['001'].data
        record_row = Record(type=self.BIB,
                            ctrlno=ctrlno,
                            record=pickle.dumps(bib_record))
        self.session.merge(record_row)
    def __add_aut(self, aut_record) -> None:
        aut_record.__class__ = LaneMARCRecord
        ctrlno = aut_record['001'].data
        record_row = Record(type=self.AUT,
                            ctrlno=ctrlno,
                            record=pickle.dumps(aut_record))
        self.session.merge(record_row)
    def __add_hdg(self, hdg_record) -> None:
        hdg_record.__class__ = LaneMARCRecord
        hdg_ctrlno = hdg_record['001'].data
        bib_ctrlno = hdg_record['004'].data
        record_row = Record(type=self.HDG,
                            ctrlno=hdg_ctrlno,
                            record=pickle.dumps(hdg_record))
        self.session.merge(record_row)
        hdglink_row = HoldingsLink(hdg_ctrlno=hdg_ctrlno,
                                   bib_ctrlno=bib_ctrlno)
        self.session.merge(hdglink_row)

    def get_records(self, record_type=None, ctrlnos: list=[], batch_size: int=0):
        assert record_type in (None, self.BIB, self.AUT, self.HDG), \
            f"invalid record type: {record_type}"
        query = self.session.query(Record).order_by(Record.ctrlno)
        if record_type is not None:
            query = query.filter_by(type=record_type)
        if ctrlnos:
            query = query.filter(Record.ctrlno.in_(ctrlnos))
        if batch_size > 0:
            # return lists of size batch_size, of tuples
            query = query.limit(batch_size)
            current_offset = 0
            records = [(record.ctrlno, pickle.loads(record.record)) for record in query]
            while len(records) > 0:
                yield records
                current_offset += batch_size
                query = query.offset(current_offset)
                records = [(record.ctrlno, pickle.loads(record.record)) for record in query]
        else:
            # return tuples
            for record in query:
                yield record.ctrlno, pickle.loads(record.record)

    def get_bibs(self, ctrlnos: list=[], batch_size: int=0):
        return self.get_records(self.BIB, ctrlnos, batch_size)
    def get_auts(self, ctrlnos: list=[], batch_size: int=0):
        return self.get_records(self.AUT, ctrlnos, batch_size)
    def get_hdgs(self, ctrlnos: list=[], batch_size: int=0):
        return self.get_records(self.HDG, ctrlnos, batch_size)

    def get_bibs_for_hdg(self, hdg_ctrlno: str) -> list:
        if self.cache_bibmfhd_links:
            if self.hdg_to_bib_map is None:
                self.fetch_and_cache_bib_hdg_maps()
            return self.hdg_to_bib_map.get(hdg_ctrlno)
        hdg_ctrlno = re.sub(r'\D', '', hdg_ctrlno)
        query = self.session.query(HoldingsLink).filter_by(hdg_ctrlno=hdg_ctrlno)
        return [result.bib_ctrlno for result in query]

    def get_hdgs_for_bib(self, bib_ctrlno: str) -> list:
        if self.cache_bibmfhd_links:
            if self.bib_to_hdg_map is None:
                self.fetch_and_cache_bib_hdg_maps()
            return self.bib_to_hdg_map.get(bib_ctrlno)
        bib_ctrlno = re.sub(r'\D', '', bib_ctrlno)
        query = self.session.query(HoldingsLink).filter_by(bib_ctrlno=bib_ctrlno)
        return [result.hdg_ctrlno for result in query]

    bib_to_hdg_map, hdg_to_bib_map, prefixed_hdg_to_bib_map = None, None, None
    def fetch_and_cache_bib_hdg_maps(self):
        self.bib_to_hdg_map, self.hdg_to_bib_map, self.prefixed_hdg_to_bib_map = {}, {}, {}
        for hdg_link in self.session.query(HoldingsLink):
            bib_ctrlno, hdg_ctrlno = hdg_link.bib_ctrlno, hdg_link.hdg_ctrlno
            self.hdg_to_bib_map[hdg_ctrlno] = [bib_ctrlno]
            prefixed_bib_ctrlno = self.get_prefixed_bib_control_number(bib_ctrlno)
            if prefixed_bib_ctrlno is not None:
                self.prefixed_hdg_to_bib_map[f"(CStL)H{hdg_ctrlno}"] = prefixed_bib_ctrlno
            if bib_ctrlno not in self.bib_to_hdg_map:
                self.bib_to_hdg_map[bib_ctrlno] = []
            self.bib_to_hdg_map[bib_ctrlno].append(hdg_ctrlno)

    # numerical (voyager) id to full prefixed control number
    def get_prefixed_bib_control_number(self, bibid: str) -> str:
        if self.bib_id_to_ctrlno_letter_map is None:
            self.__fetch_and_cache_bib_ctrlno_letters()
        ctrlno_letter = self.bib_id_to_ctrlno_letter_map.get(bibid)
        if ctrlno_letter is None:
            return None
        return f"(CStL){ctrlno_letter}{bibid}"

    bib_id_to_ctrlno_letter_map = None
    def __fetch_and_cache_bib_ctrlno_letters(self):
        self.bib_id_to_ctrlno_letter_map = {}
        for bibid, record in self.get_bibs():
            prefixed_ctrlno = record.get_control_number()
            if prefixed_ctrlno is not None:
                self.bib_id_to_ctrlno_letter_map[str(bibid)] = prefixed_ctrlno[6]