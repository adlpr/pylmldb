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

# use /secrets file if it exists, else this dir
if os.path.exists("/secrets/creds.json"):
    CREDS_FILE_LOC = "/secrets/creds.json" 
else:
    CREDS_FILE_LOC = os.path.join(os.path.dirname(__file__), "creds.json")


class VoyagerAPI:
    """
    Interface for pulling current MARC data from the Lane Voyager HTTPS API
    """
    with open(CREDS_FILE_LOC,'r') as inf:
        url_base, auth = json.load(inf).get("voy")
    auth = tuple(auth)

    @classmethod
    def get_status(cls):
        r = requests.get(f"{cls.url_base}/status.txt", auth=cls.auth)
        return r.content

    BIB, AUT, HDG = 'bib', 'auth', 'mfhd'
    @classmethod
    def get_record(cls, record_type, record_id):
        r = requests.get(f"{cls.url_base}/records/{record_type}/{record_id}", auth=cls.auth)
        return r.content

    @classmethod
    def get_records(cls, record_type, time=0):
        r = requests.get(f"{cls.url_base}/records/{record_type}?time={time}", auth=cls.auth)
        return r.content
