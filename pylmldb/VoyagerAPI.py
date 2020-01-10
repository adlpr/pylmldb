#!/usr/bin/python3
# -*- coding: UTF-8 -*-

"""
https://lane-local.stanford.edu/voyager-api/

status.txt
records/{recordType}/{recordId}
records/{recordType}?time={long}

(str) recordType ∈ (auth|bib|mfhd)
(long) time = number of *milli*seconds since 1970-01-01T00:00:00Z
"""

import os, json, requests

from .config import VOYAGER_API_ENDPOINT, VOYAGER_API_USERNAME, VOYAGER_API_PASSWORD


class VoyagerAPI:
    """
    Interface for pulling current MARC data from the Lane Voyager HTTPS API
    """
    @staticmethod
    def get_status():
        r = requests.get(f"{VOYAGER_API_ENDPOINT}/status.txt", auth=(VOYAGER_API_USERNAME, VOYAGER_API_PASSWORD))
        return r.content

    BIB, AUT, HDG = 'bib', 'auth', 'mfhd'
    @staticmethod
    def get_record(record_type, record_id):
        r = requests.get(f"{VOYAGER_API_ENDPOINT}/records/{record_type}/{record_id}", auth=(VOYAGER_API_USERNAME, VOYAGER_API_PASSWORD))
        return r.content

    @staticmethod
    def get_records(record_type, time=0):
        r = requests.get(f"{VOYAGER_API_ENDPOINT}/records/{record_type}?time={time}", auth=(VOYAGER_API_USERNAME, VOYAGER_API_PASSWORD))
        return r.content
