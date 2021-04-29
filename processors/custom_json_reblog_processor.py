# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from beem.constants import STEEM_100_PERCENT
from beem.utils import resolve_authorperm, construct_authorperm
from engine.account_storage import AccountsDB
from engine.post_storage import PostsTrx
from engine.vote_storage import VotesTrx
from engine.utils import _score
from processors.custom_json_processor import CustomJsonProcessor, extract_user
import time

import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

class ReblogProcessor(CustomJsonProcessor):
    """ Processor for reblog operations.
    """

    def __init__(self, db, token_metadata):
        super().__init__(db, token_metadata)

    def process(self, ops, json_data):
        """ Main process method.
        """
        token_config = self.token_metadata["config"]
        timestamp = ops["timestamp"].replace(tzinfo=None)

        user = extract_user(ops, json_data)
        if user is None:
            return

        # ["reblog",{"account":"xx","author":"yy","permlink":"zz", "delete":"delete"}]
        if "account" not in json_data[1] or user != json_data[1]["account"]:
            return
        if "author" not in json_data[1] or "permlink" not in json_data[1]:
            return

        authorperm = construct_authorperm(json_data[1]["author"], json_data[1]["permlink"])
        posts = self.postTrx.get_post(authorperm)
        if len(posts) > 0 and posts[0]["parent_author"] == "":
            if "delete" in json_data[1] and json_data[1]["delete"] == "delete":
              self.reblogsStorage.delete(user, authorperm)
            else:
              self.reblogsStorage.upsert({"account": user, "authorperm": authorperm, "timestamp": timestamp})

