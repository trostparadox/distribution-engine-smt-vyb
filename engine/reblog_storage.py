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


class ReblogsDB(object):
    """ This is the accounts storage class
    """
    __tablename__ = 'reblogs'

    def __init__(self, db):
        self.db = db

    def exists_table(self):
        """ Check if the database table exists
        """
        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False

    def upsert(self, data):
        """ Add a new data set
    
        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["account", "authorperm"])

    def delete(self, account, authorperm):
        table = self.db[self.__tablename__]
        table.delete(account=account, authorperm=authorperm)

    def get_earliest_authorperm_reblog_timestamp(self, account, authorperm, use_follows=False):
        result = None
        if use_follows:
            result = self.db.query("WITH following_table AS (SELECT following from follows where follower = :account AND state = 1) SELECT * FROM reblogs WHERE account in (SELECT * FROM following_table) AND authorperm = :authorperm ORDER BY timestamp ASC LIMIT 1", account=account, authorperm=authorperm)
        else:
            result = self.db.query("SELECT * FROM reblogs WHERE account = :account AND authorperm = :authorperm ORDER BY timestamp ASC LIMIT 1", account=account, authorperm=authorperm)
        result = next(result, None)
        return result['timestamp'] if result is not None else None


    def wipe(self, sure=False):
        """Purge the entire database. No data set will survive this!"""
        if not sure:
            log.error(
                "You need to confirm that you are sure "
                "and understand the implications of "
                "wiping your wallet!"
            )
            return
        else:
            table = self.db[self.__tablename__]
            table.drop
