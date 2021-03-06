#!/usr/bin/python3
# -*- coding: UTF-8 -*-

# copy secrets file into this dir if it exists
import os
if os.environ.get('FLASK_ENV') == 'docker':
    from shutil import copyfile
    copyfile("/secrets/config.py", os.path.join(os.path.dirname(__file__), "config.py"))

from .VoyagerAPI import VoyagerAPI
from .LaneMARCRecord import LaneMARCRecord
from .LmlDb import LMLDB
# from .LmlDbSQLite import LMLDBSQLite
from .Surveyor import Surveyor
