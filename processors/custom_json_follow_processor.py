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

class FollowProcessor(CustomJsonProcessor):
    """ Processor for follow operations.
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

        if isinstance(json_data, list) and len(json_data) == 2 and json_data[0] == "follow" and isinstance(json_data[1], dict):
            if "following" in json_data[1] and "follower" in json_data[1] and user == json_data[1]["follower"]:
                following = f"{json_data[1]['following']}"
                muted = json_data[1]["what"] == ["ignore"]
                blog_follow = json_data[1]["what"] == ["blog"]
                follow_state = 2 if muted else 1 if blog_follow else 0
                if len(user) > 20 or len(following) > 20:
                    return
                self.followsDb.upsert({"follower": user, "following": following, "state": follow_state})


