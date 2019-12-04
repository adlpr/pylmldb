#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import os, re, json, pickle

from loguru import logger

# ORM model specs

import sqlalchemy
from sqlalchemy import Column, String, Integer, BigInteger, Binary
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

# use /secrets file if exists, else this dir
if os.path.exists("/secrets/creds.json"):
    db_creds_filename = "/secrets/creds.json" 
else:
    db_creds_filename = os.path.join(os.path.dirname(__file__), "creds.json")
# create engine and session maker
with open(db_creds_filename, 'r') as inf:
    engine = sqlalchemy.create_engine(json.load(inf).get("pg"))
Session = sqlalchemy.orm.sessionmaker(bind=engine)


from .VoyagerAPI import VoyagerAPI
from .LaneMARCRecord import LaneMARCRecord


class LMLDB:
    """
    Interface for creating/accessing a postgres-based mirror of the Lane MARC catalog
    """
    def __init__(self, mode='r', version=0):
        assert mode in 'rwa', f"invalid mode: {mode}"
        self.mode = mode
        if mode == 'r' and version != 0:
            logger.warning(f"read mode, version ({version}) ignored")
        assert isinstance(version, int) or version.isdigit(), \
            f"version must be int: {version}"
        self.version = str(int(version))
        # establish session
        self.session = Session()
        if mode == 'a' and not self.__check_integrity():
            logger.error("lmldb integrity check failed, changing mode to w")
            self.mode = 'w'
        if self.mode == 'w':
            self.__init_db()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.mode != 'r':
            self.__update_version()
        self.session.close()

    def __check_integrity(self):
        # all tables exist and there is at least one row in each
        try:
            for obj in (Version, Record, HoldingsLink):
                if self.session.query(obj).first() is None:
                    return False
            return True
        except:
            return False

    def __init_db(self):
        logger.debug("reinitializing lmldb")
        # Create schema
        engine.execute("CREATE SCHEMA IF NOT EXISTS marc")
        # Create tables
        Base.metadata.create_all(engine)

    def __update_version(self):
        self.session.merge(Version(version=self.version))
        self.session.commit()

    def get_version(self) -> int:
        try:
            v = self.session.query(Version).first()
            return int(v.version)
        except:
            return 0

    BIB, AUT, HDG = VoyagerAPI.BIB, VoyagerAPI.AUT, VoyagerAPI.HDG
    def populate(self, record_type, marc_reader):
        # expects pymarc MARCReader
        add_function = { self.BIB : self.__add_bib,
                         self.AUT : self.__add_aut,
                         self.HDG : self.__add_hdg }.get(record_type)
        if add_function is None:
            raise ValueError(f'invalid record_type: {record_type}')
        for record in marc_reader:
            add_function(record)
        self.session.commit()

    def __add_bib(self, bib_record):
        bib_record.__class__ = LaneMARCRecord
        ctrlno = bib_record['001'].data
        record_row = Record(type=self.BIB,
                            ctrlno=ctrlno,
                            record=pickle.dumps(bib_record))
        self.session.merge(record_row)
    def __add_aut(self, aut_record):
        aut_record.__class__ = LaneMARCRecord
        ctrlno = aut_record['001'].data
        record_row = Record(type=self.AUT,
                            ctrlno=ctrlno,
                            record=pickle.dumps(aut_record))
        self.session.merge(record_row)
    def __add_hdg(self, hdg_record):
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

    def get_records(self, record_type=None, ctrlnos: list=[]):
        assert record_type in (None, self.BIB, self.AUT, self.HDG), \
            f"invalid record type: {record_type}"
        query = self.session.query(Record)
        if record_type is not None:
            query = query.filter_by(type=record_type)
        if ctrlnos:
            query = query.filter(Record.ctrlno.in_(ctrlnos))
        for record in query.all():
            yield record.ctrlno, pickle.loads(record.record)

    def get_bibs(self, ctrlnos: list=[]):
        return self.get_records(self.BIB, ctrlnos)
    def get_auts(self, ctrlnos: list=[]):
        return self.get_records(self.AUT, ctrlnos)
    def get_hdgs(self, ctrlnos: list=[]):
        return self.get_records(self.HDG, ctrlnos)

    def get_bibs_for_hdg(self, hdg_ctrlno: str) -> list:
        hdg_ctrlno = re.sub(r'\D', '', hdg_ctrlno)
        query = self.session.query(HoldingsLink).filter_by(hdg_ctrlno=hdg_ctrlno)
        return [result.bib_ctrlno for result in query.all()]

    def get_hdgs_for_bib(self, bib_ctrlno: str) -> list:
        bib_ctrlno = re.sub(r'\D', '', bib_ctrlno)
        query = self.session.query(HoldingsLink).filter_by(bib_ctrlno=bib_ctrlno)
        return [result.hdg_ctrlno for result in query.all()]
