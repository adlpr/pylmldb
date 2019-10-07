#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import os, shutil, re, pickle, sqlite3

from .VoyagerAPI import VoyagerAPI
from .LaneMARCRecord import LaneMARCRecord


class LMLDB:
    """
    Interface for creating/accessing a (local) sqlite mirror of the Lane MARC catalog

    records:
    | type [BIB|AUT|HDG] | ctrlno [int w no prefix] | record [LaneMARCRecord pickled bytes] |

    holdings_links:
    | hdg_ctrlno | bib_ctrlno |

    version:
    | version |
    """
    def __init__(self, version=-1, reinit=False):
        assert isinstance(version, int) or version.isdigit()
        self.filename = os.path.join(os.path.dirname(__file__), "..", "lml.db")
        # if version is default (-1), this is a "read-only" session
        # if version is specified and reinit is False, this is an "update" session
        # if version is specified and reinit is True, this is a "reinit" session
        self.version = int(version)
        self.read_only = self.version < 0
        self.reinit = reinit
        assert not (self.read_only and self.reinit), \
            "cannot re-initialize without specified version"
        # if requested (reinit flag set) or needed (lml.db missing),
        #   re-initialize db
        if self.reinit or not os.path.exists(self.filename):
            self.__init_db()
        self.conn = sqlite3.connect(self.filename)
        self.cur = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.read_only:
            self.__update_version()
        self.conn.close()
        if not self.read_only:
            self.__make_backup()

    def __init_db(self):
        try:
            os.remove(self.filename)
        except:
            pass
        # Create tables
        with sqlite3.connect(self.filename) as conn:
            c = conn.cursor()
            # Create tables
            c.execute("""CREATE TABLE records (
                          type TEXT, ctrlno TEXT, record BLOB,
                          PRIMARY KEY (type, ctrlno)
                         );""")
            c.execute("""CREATE TABLE holdings_links
                         (hdg_ctrlno TEXT PRIMARY KEY, bib_ctrlno TEXT);""")
            c.execute("""CREATE TABLE version
                         (version INT);""")
            # not bothering with FK constraints
            conn.commit()

    def __update_version(self):
        self.cur.execute("DELETE FROM version;")
        self.cur.execute("INSERT OR REPLACE INTO version VALUES (?);",
                          (self.version,))
        self.conn.commit()

    def get_version(self):
        self.cur.execute("SELECT version FROM version LIMIT 1;")
        return self.cur.fetchone()[0]

    def __make_backup(self):
        shutil.copyfile(f"{self.filename}", f"{self.filename}.{self.version}")

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
        self.conn.commit()

    def __add_bib(self, bib_record):
        bib_record.__class__ = LaneMARCRecord
        ctrlno = bib_record['001'].data
        self.cur.execute("INSERT OR REPLACE INTO records VALUES (?, ?, ?);",
                          (self.BIB, ctrlno, pickle.dumps(bib_record)))
    def __add_aut(self, aut_record):
        aut_record.__class__ = LaneMARCRecord
        ctrlno = aut_record['001'].data
        self.cur.execute("INSERT OR REPLACE INTO records VALUES (?, ?, ?);",
                          (self.AUT, ctrlno, pickle.dumps(aut_record)))
    def __add_hdg(self, hdg_record):
        hdg_record.__class__ = LaneMARCRecord
        hdg_ctrlno = hdg_record['001'].data
        bib_ctrlno = hdg_record['004'].data
        self.cur.execute("INSERT OR REPLACE INTO records VALUES (?, ?, ?);",
                          (self.HDG, hdg_ctrlno, pickle.dumps(hdg_record)))
        self.cur.execute("INSERT OR REPLACE INTO holdings_links VALUES (?, ?);",
                          (hdg_ctrlno, bib_ctrlno))

    def get_records(self, record_type=None, ctrlnos=[]):
        assert record_type in (None, self.BIB, self.AUT, self.HDG), \
            f"invalid record type: {record_type}"
        query = "SELECT ctrlno, record FROM records"
        query_where = []
        params = ()
        if record_type is not None:
            query_where.append("type = ?")
            params += (record_type,)
        if ctrlnos:
            query_where.append(f"ctrlno IN ({','.join('?'*len(ctrlnos))})")
            params += (*ctrlnos,)
        if query_where:
            query += " WHERE " + " AND ".join(query_where)
        self.cur.execute(query, params)
        for ctrlno, record_blob in self.cur.fetchall():
            yield ctrlno, pickle.loads(record_blob)

    def get_bibs(self, ctrlnos=[]):
        return self.get_records(self.BIB, ctrlnos)
    def get_auts(self, ctrlnos=[]):
        return self.get_records(self.AUT, ctrlnos)
    def get_hdgs(self, ctrlnos=[]):
        return self.get_records(self.HDG, ctrlnos)

    def get_bibs_for_hdg(self, hdg_ctrlno):
        hdg_ctrlno = re.sub(r'\D', '', hdg_ctrlno)
        self.cur.execute("""SELECT bib_ctrlno FROM holdings_links
                            WHERE hdg_ctrlno = ?""",
                            (hdg_ctrlno,))
        return [result[0] for result in self.cur.fetchall()]

    def get_hdgs_for_bib(self, bib_ctrlno):
        bib_ctrlno = re.sub(r'\D', '', bib_ctrlno)
        self.cur.execute("""SELECT hdg_ctrlno FROM holdings_links
                            WHERE bib_ctrlno = ?""",
                            (bib_ctrlno,))
        return [result[0] for result in self.cur.fetchall()]
