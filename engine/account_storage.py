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


class AccountsDB(object):
    """ This is the accounts storage class
    """
    __tablename__ = 'accounts'

    def __init__(self, db):
        self.db = db

    def find(self, symbol):
        table = self.db[self.__tablename__]
        return table.find(symbol=symbol)

    def get(self, name, symbol):
        table = self.db[self.__tablename__]
        return table.find_one(name=name, symbol=symbol)

    def get_all_token(self, name):
        table = self.db[self.__tablename__]
        account = {}
        for data in table.find(name=name):
            account[data["symbol"]]=data
        return account

    def upsert(self, data):
        """ Add a new data set
    
        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["name", "symbol"])

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        #self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["name", "symbol"])
        else:
            for d in data:
                table.update(data[d], ["name", "symbol"])            
        #self.db.commit()

    def update(self, data):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        table.update(data, ['name', 'symbol'])
