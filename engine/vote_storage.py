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


class VotesTrx(object):
    """ This is the vote storage class
    """
    __tablename__ = 'votes'

    def __init__(self, db):
        self.db = db

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm", "voter", "token"])

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        
        if isinstance(data, list):
            #self.db.begin()
            #table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                # ensure=False, require all indexes be created up front to not waste space
                # Vote primary key lookup doesn't need a redundant index.
                table.upsert(d, ["authorperm", "voter", "token"], ensure=False)
        else:
            #self.db.begin()
            for d in data:
                table.upsert(data[d], ["authorperm", "voter", "token"])            
            
        #self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        #self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["authorperm", "voter", "token"])
        else:
            for d in data:
                table.update(data[d], ["authorperm", "voter", "token"])            
        #self.db.commit()

    def update(self, data):
        table = self.db[self.__tablename__]
        table.update(data, ['authorperm', "voter", "token"])

    def get(self, authorperm, voter, token):
        table = self.db[self.__tablename__]
        return table.find_one(authorperm=authorperm, voter=voter, token=token)

    def get_token_vote(self, authorperm, token):
        table = self.db[self.__tablename__]
        votes = []
        for vote in table.find(authorperm=authorperm, token=token, order_by="timestamp"):
            votes.append(vote)
        return votes

