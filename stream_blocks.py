#!/usr/bin/python
# -*- coding: utf-8 -*-
from beem.utils import formatTimeString, resolve_authorperm, construct_authorperm, addTzInfo
from beem.nodelist import NodeList
from beem.comment import Comment
from beem import Hive
from datetime import datetime, timedelta
from beem.instance import set_shared_steem_instance
from beem.blockchain import Blockchain
from beem.block import Block
from beem.account import Account
from beem.amount import Amount
from beem.memo import Memo
import time 
import json
import os
import sys
import math
import dataset
import random
import logging
import logging.config
from datetime import date, datetime, timedelta
from dateutil.parser import parse
from decimal import Decimal
from beem.constants import STEEM_100_PERCENT, STEEM_VOTE_REGENERATION_SECONDS
from engine.follow_storage import FollowsDB
from engine.post_storage import PostsTrx
from engine.config_storage import ConfigurationDB
from engine.token_config_storage import TokenConfigDB
from engine.reblog_storage import ReblogsDB
from engine.version import version as engineversion
from engine.utils import setup_logging, initialize_config, initialize_token_metadata
from processors.comment_processor_for_engine import CommentProcessorForEngine
from processors.custom_json_follow_processor import FollowProcessor
from processors.custom_json_set_tribe_settings import SetTribeSettingsProcessor
from processors.custom_json_reblog_processor import ReblogProcessor
from processors.custom_json_processor import extract_json_data
from steemengine.tokenobject import Token
from steemengine.tokens import Tokens
from steemengine.wallet import Wallet
from steemengine.api import Api
from beem.block import Block
import hashlib
import random
import base36
import dataset
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()


if __name__ == "__main__":
    
    setup_logging('logger.json')
    
    config_file = 'config.json'
    config_data = initialize_config(config_file)

    databaseConnector = config_data["databaseConnector"]
    engine_api = Api(url=config_data["engine_api"])
    engine_id = config_data["engine_id"]

    start_prep_time = time.time()

    # ensure_schema False, require all indexes be created up front to not waste space
    # (e.g. vote primary key lookup doesn't need a redundant index)
    db = dataset.connect(databaseConnector, ensure_schema=False)
    # Create keyStorage
    
    postTrx = PostsTrx(db)
    confStorage = ConfigurationDB(db)
    tokenConfigStorage = TokenConfigDB(db)
    reblogsStorage = ReblogsDB(db)
    followsDb = FollowsDB(db)
   
    max_batch_size = None
    threading = False

    node_list = ["https://api.deathwing.me", "https://api.hive.blog"]
    hived = Hive(node=node_list, num_retries=5, call_num_retries=3, timeout=15) 
    print("using node %s" % hived.rpc.url)
    b = Blockchain(mode="head", max_block_wait_repetition=27, steem_instance = hived)
    current_block_num = b.get_current_block_num()

    conf_setup = confStorage.get()
    if conf_setup is None:
        confStorage.upsert({"last_streamed_block": 0})
        last_streamed_block = current_block_num
        last_streamed_timestamp = None
    else:       
        last_streamed_block = conf_setup["last_streamed_block"]
        last_streamed_timestamp = conf_setup["last_streamed_timestamp"]

    
    last_engine_streamed_timestamp = None
    engine_conf = confStorage.get_engine()
    if engine_conf:
        last_engine_streamed_timestamp = engine_conf["last_engine_streamed_timestamp"]
    
    if last_streamed_block == 0:
        start_block = current_block_num
        confStorage.upsert({"last_streamed_block": start_block})
    else:
        start_block = last_streamed_block + 1
  
    stop_block = current_block_num
    print("processing blocks %d - %d" % (start_block, stop_block))
 
    last_block_print = start_block

    token_config = tokenConfigStorage.get_all()
    token_metadata = initialize_token_metadata(token_config, engine_api)

    start_prep_time = time.time()
    current_block_num = start_block - 1
    block_processing_time = time.time()

    # Processors
    comment_processor_for_engine = CommentProcessorForEngine(db, hived, token_metadata)
    reblog_processor = ReblogProcessor(db, token_metadata)
    follow_processor = FollowProcessor(db, token_metadata)
    set_tribe_settings_processor = SetTribeSettingsProcessor(db, token_metadata)

    for ops in b.stream(start=start_block, stop=stop_block, opNames=["comment", "custom_json", "delete_comment"], max_batch_size=max_batch_size, threading=threading, thread_num=8):
        if ops["block_num"] - current_block_num > 1:
            print("Skip block last block %d - now %d" % (current_block_num, ops["block_num"]))
        elif ops["block_num"] - current_block_num == 1:
            print("Current block %d" % ops["block_num"])

        current_block_num = ops["block_num"]
        timestamp = ops["timestamp"].replace(tzinfo=None)
        
        delay_min = (datetime.utcnow() - timestamp).total_seconds() / 60
        delay_sec = int((datetime.utcnow() - timestamp).total_seconds())

        if delay_sec < 15:
            print(f"Blocks too recent {delay_sec} ago, waiting.")
            break
        if not last_engine_streamed_timestamp or timestamp >= last_engine_streamed_timestamp:
            print(f"Waiting for engine refblock to catch up to {last_engine_streamed_timestamp}")
            break

        if delay_min < 1:
            delay_string = ("(+ %d s): " % (delay_sec))
        else:
            delay_string = ("(+ %.1f min)" % ( delay_min))
        
        
        last_streamed_timestamp = timestamp
        if ops["block_num"] > last_streamed_block:
            if len(token_config) > 0:
                print("%s: Block processing took %.2f s" % (delay_string, time.time() - block_processing_time))
            block_processing_time = time.time()

            confStorage.upsert({"last_streamed_block": last_streamed_block,
                                "last_streamed_timestamp": last_streamed_timestamp })

            # this is end of a block. end tx here
            if start_block < last_streamed_block + 1:
                db.commit()
            db.begin()
       
        last_streamed_block = ops["block_num"]

        if ops["block_num"] - last_block_print > 20:
            last_block_print = ops["block_num"]
            print("%s: %d - %s" % (delay_string, ops["block_num"], str(ops["timestamp"])))
            print("blocks left %d" % (ops["block_num"] - stop_block))
        
        if ops["type"] == "custom_json":
            custom_json_start_time = time.time()
            json_data = extract_json_data(ops)
            if json_data and ops['id'] == "follow" and isinstance(json_data, list) and len(json_data) == 2 and json_data[0] == "reblog" and isinstance(json_data[1], dict):
                reblog_processor.process(ops, json_data)
            elif json_data and ops['id'] == "reblog" and isinstance(json_data, list) and len(json_data) == 2 and json_data[0] == "reblog" and isinstance(json_data[1], dict):
                reblog_processor.process(ops, json_data)
            elif json_data and ops['id'] == "follow":
                follow_processor.process(ops, json_data)
                print("follow op took %.2f s" % (time.time() - custom_json_start_time))
            elif json_data and ops['id'] == "scot_set_tribe_settings":
                set_tribe_settings_processor.process(ops, json_data)
        elif ops["type"] == "delete_comment":
            try:
                authorperm = construct_authorperm(ops["author"], ops["permlink"])
                postTrx.delete_posts(authorperm)
            except:
                print("Could not process %s" % authorperm)
        else:
            comment_processor_for_engine.process(ops)
       
    if stop_block >= start_block:
        confStorage.upsert({"last_streamed_block": last_streamed_block, 
                            "last_streamed_timestamp": last_streamed_timestamp })
        db.commit()
    
    print("stream posts script run %.2f s" % (time.time() - start_prep_time))
