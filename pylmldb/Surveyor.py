#!/usr/bin/python3
# -*- coding: UTF-8 -*-

"""
abstract surveyor (marc catalog based report-generating) architecture for lmldb
"""

import csv

from loguru import logger

from .LmlDb import LMLDB


class Surveyor:
    """
    Abstracted marc lmldb report generator
    """
    BIB, AUT, HDG = LMLDB.BIB, LMLDB.AUT, LMLDB.HDG
    def __init__(self, primary_record_type: str,
                       filters: list=[],
                       columns: dict={'id':(lambda c,p,s,t: c)},
                       use_crossreferencing: bool=False,
                       use_items: bool=False) -> None:
        assert primary_record_type in (self.BIB, self.AUT, self.HDG), \
            f"invalid primary_record_type: {primary_record_type} (must be in: ({self.BIB}, {self.AUT}, {self.HDG}))"
        self.primary_record_type = primary_record_type
        self.filters = filters
        self.columns = columns
        self.use_crossreferencing = use_crossreferencing
        self.use_items = use_items

    def set_filters(self, filters: list) -> None:
        self.filters = filters

    def add_filter(self, f) -> None:
        self.filters.append(f)

    def set_columns(self, columns: dict) -> None:
        self.columns.update(column)

    def add_column(self, title, f) -> None:
        self.columns[title] = f

    def run_report(self, outf_name: str="surveyor_out.csv") -> None:
        # check write permissions for output file before running the whole thing
        with open(outf_name, 'w', encoding='utf-8-sig') as outf:
            pass
        #
        with LMLDB() as db:
            primary_id_to_secondary_records = {}
            secondary_id_to_secondary_record = {}
            if self.use_crossreferencing and self.primary_record_type != db.AUT:
                logger.info("building crossreferences")
                if self.primary_record_type == db.HDG:
                    secondary_record_type, get_secondary_record_ids = db.BIB, db.get_bibs_for_hdg
                else:
                    secondary_record_type, get_secondary_record_ids = db.HDG, db.get_hdgs_for_bib
                logger.info("mapping secondary record ids to secondary records")
                for ctrlno, record in db.get_records(secondary_record_type):
                    secondary_id_to_secondary_record[str(ctrlno)] = record
                logger.info("mapping primary record ids to lists of secondary records")
                for ctrlno, record in db.get_records(self.primary_record_type):
                    primary_id = str(ctrlno)
                    primary_id_to_secondary_records[primary_id] = [secondary_id_to_secondary_record.get(secondary_id) for secondary_id in (get_secondary_record_ids(primary_id) or ())]
                del secondary_id_to_secondary_record
                if self.use_items:
                    # load item vw table
                    # @@@@@@@@@@@@@@@@@@@@@@@@@
                    logger.info("pull item record info")
                    with open("surveyordata/ITEM_VW.csv", encoding='windows-1251') as inf:
                        reader = csv.reader(inf, dialect='excel')
                        header = list(next(reader))
                        item_vw = [dict(zip(header,line)) for line in reader]

            # pull records and filter
            logger.info("pull primary records and filter")
            filtered_record_sets = []
            for ctrlno, primary_record in db.get_records(self.primary_record_type):
                primary_id = str(ctrlno)
                secondary_records = primary_id_to_secondary_records.get(primary_id, [])
                tertiary_records = []
                if all(f(primary_id, primary_record, secondary_records, tertiary_records) for f in self.filters):
                    filtered_record_sets.append((primary_id, primary_record, secondary_records, tertiary_records))

        # build columns
        # header
        results = [tuple(self.columns.keys())]
        for record_set in filtered_record_sets:
            results.append([col_func(*record_set) for col_func in self.columns.values()])

        # output
        logger.info("outputting")
        with open(outf_name, 'w', encoding='utf-8-sig') as outf:
            writer = csv.writer(outf, dialect=csv.excel, quoting=csv.QUOTE_ALL)
            for result in results:
                writer.writerow(result)



if __name__ == "__main__":
    main()
