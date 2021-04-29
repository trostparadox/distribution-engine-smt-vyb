# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import bytes
from builtins import object
from beemgraphenebase.py23 import py23_bytes, bytes_types
from sqlalchemy.dialects.postgresql import insert as pg_insert
import shutil
import time
import os
import json
import sqlite3
from appdirs import user_data_dir
from datetime import datetime, timedelta
from beem.utils import formatTimeString, addTzInfo
import logging
from binascii import hexlify
import random
import hashlib
import dataset
from sqlalchemy import and_
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"

HIVED = 1
ENGINE_SIDECHAIN = 2

class ConfigurationDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'configuration'

    def __init__(self, db):
        self.db = db


    def get(self):
        table = self.db[self.__tablename__]
        return table.find_one(id=HIVED)

    def get_engine(self):
        table = self.db[self.__tablename__]
        return table.find_one(id=ENGINE_SIDECHAIN)
    
    def upsert(self, data):
        data["id"]= HIVED
        table = self.db[self.__tablename__]
        table.upsert(data, ['id'])

    def upsert_engine(self, data):
        data["id"]= ENGINE_SIDECHAIN
        table = self.db[self.__tablename__]
        table.upsert(data, ['id'])
