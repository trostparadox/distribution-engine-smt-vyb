# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from beem.constants import STEEM_100_PERCENT
from beem.utils import resolve_authorperm, construct_authorperm
from datetime import datetime, timedelta
from decimal import Decimal
from decimal import Decimal
from engine.account_storage import AccountsDB
from engine.utils import _score
from processors.custom_json_processor import CustomJsonProcessor
from steemengine.tokenobject import Token
import json
import re
import time
import traceback
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

class CommentsContractProcessor(CustomJsonProcessor):
    """ Processor for comments contract operations.
    """

    def __init__(self, db, api, token_metadata):
        super().__init__(db, token_metadata)
        self.api = api


    def process(self, op, contractPayload, timestamp):
        """ Main process method.
        """
        token_config = self.token_metadata["config"]
        token_objects = self.token_metadata["objects"]

#{'refHiveBlockNumber': 52775960, 'transactionId': 'bfde3ff17dafabe17e58f790fe716d9b062a4367', 'sender': 'null', 'contract': 'comments', 'action': 'comment', 'payload': '{"author":"ebingo","permlink":"nigerian-cybercrime-who-is-the-nigerian-prince-1-3","rewardPools":[1]}', 'executedCodeHash': '0a2ff958d88fa0594dfdd1afa9dd63057a94ffc240658161609352b91f9e969f', 'hash': '6dce35097920fcc67cdfaa32cd0e89f8bf637ad2eecba59d7888577800d83c27', 'databaseHash': 'd6ad7bd101e4c6376ea9a0bbd78f7a643ce6009c4154f4d878c2a42c822808ef', 'logs': '{"events":[{"contract":"comments","event":"newComment","data":{"rewardPoolId":1,"symbol":"PAL"}}]}'}
#{'refHiveBlockNumber': 52775960, 'transactionId': '15e4bf5a2c3c8f2f9c1c04f3aa905de1ad3dc38f', 'sender': 'null', 'contract': 'comments', 'action': 'vote', 'payload': '{"voter":" toni.curation","author":"cryptopoints","weight":400,"permlink":"wleo-is-not-right-on-track-wleo-price-and-market-analysis-deeply"}', 'executedCodeHash': '0a2ff958d88fa0594dfdd1 afa9dd63057a94ffc240658161609352b91f9e969f', 'hash': 'c6221ceda70b6da352d4c71f8b41202651d48c7d948a86d3b7944b902364dd85', 'databaseHash': '514d29b2cba0a4831edfa075c138a6f20c3d39d253d9a4ce75deed076102f11d', 'logs': '{"events":[{"contract":"comments","event":"newVote","data":{"rewardPoolId":1,"symbol":"PAL","rshares":"0.0000000000"}}]}'}
        logs = json.loads(op["logs"])
        if "events" in logs:
            events = logs["events"]
            paid_out_posts = {}
            for event in events:
                if event["contract"] == "comments":
                    if event["event"] == "newComment":
                        token = event['data']['symbol']
                        cashout_window_days = token_config[token]["cashout_window_days"]
                        authorperm = f"@{contractPayload['author']}/{contractPayload['permlink']}"
                        self.postTrx.upsert({"authorperm": authorperm, "author": contractPayload["author"], "created": timestamp, "token": token, "cashout_time": timestamp + timedelta(cashout_window_days), "main_post": False})
                    elif event["event"] == "newVote" or event["event"] == "updateVote":
                        token = event['data']['symbol']
                        authorperm = f"@{contractPayload['author']}/{contractPayload['permlink']}"
                        voter = contractPayload["voter"]
                        rshares = Decimal(event["data"]["rshares"])
                        old_rshares = Decimal(0)
                        old_vote = self.voteTrx.get(authorperm, voter, token)
                        if old_vote:
                            old_rshares = old_vote["rshares"]
                        self.voteTrx.add_batch([{"authorperm": authorperm, "voter": voter, "token": token, "timestamp": timestamp, "rshares": rshares, "percent": contractPayload["weight"]}])
                        voted_post = self.postTrx.get_token_post(token, authorperm)
                        if voted_post:
                            updated_vote_rshares = voted_post["vote_rshares"] + rshares - old_rshares
                            sc_trend = _score(updated_vote_rshares, timestamp.timestamp(), 480000)
                            sc_hot = _score(updated_vote_rshares, timestamp.timestamp(), 10000)
                            self.postTrx.upsert({"authorperm": authorperm, "token": token, "vote_rshares": updated_vote_rshares, "score_trend": sc_trend, "score_hot": sc_hot})
                    elif event["event"] == "curationReward":
                        #{"contract":"comments","event":"curationReward","data":{"rewardPoolId":1,"authorperm":"@globalcurrencies/not-everything-that-is-dirty-is-coal-no-todo-lo-que-es-sucio-es-carbon ","symbol":"PAL","account":"javb","quantity":"0.000"}}
                        token = event['data']['symbol']
                        authorperm = event["data"]["authorperm"]
                        if authorperm not in paid_out_posts:
                            paid_out_posts[authorperm] = { "token": token, "authorperm": authorperm, "last_payout": timestamp, "total_payout_value": 0, "curator_payout_value": 0 }
                        try:
                            curation_share = Decimal(event["data"]["quantity"])
                            paid_out_posts[authorperm]["curator_payout_value"] += curation_share
                            paid_out_posts[authorperm]["total_payout_value"] += curation_share
                            if curation_share > 0:
                                self.accountHistoryTrx.add({"account": event["data"]["account"], "token": token, "timestamp": timestamp, "quantity": curation_share, "type": "curation_reward", "authorperm": authorperm, "trx": op["transactionId"]})
                        except Exception as e:
                            print(e)

                    elif event["event"] == "beneficiaryReward":
                        token = event['data']['symbol']
                        authorperm = event["data"]["authorperm"]
                        if authorperm not in paid_out_posts:
                            paid_out_posts[authorperm] = { "token": token, "authorperm": authorperm, "last_payout": timestamp, "total_payout_value": 0, "curator_payout_value": 0 }
                        try:
                            beneficiary_share = Decimal(event["data"]["quantity"])
                            paid_out_posts[authorperm]["curator_payout_value"] += beneficiary_share
                            paid_out_posts[authorperm]["total_payout_value"] += beneficiary_share
                            if beneficiary_share > 0:
                                self.accountHistoryTrx.add({"account": event["data"]["account"], "token": token, "timestamp": timestamp, "quantity": beneficiary_share, "type": "curation_reward", "authorperm": authorperm, "trx": op["transactionId"]})
                        except Exception as e:
                            print(e)

                    elif event["event"] == "authorReward":
                        #{"contract" :"comments","event":"authorReward","data":{"rewardPoolId":1,"authorperm":"@globalcurrencies/not-everything-that-is-dirty-is-coal-no-todo-lo-que-es-sucio-es-carbon","symbol":"PAL","account":"globalcurrencies","quantity":"2.571"}}
                        token = event['data']['symbol']
                        authorperm = event["data"]["authorperm"]
                        if authorperm not in paid_out_posts:
                            paid_out_posts[authorperm] = { "token": token, "authorperm": authorperm, "last_payout": timestamp, "total_payout_value": 0, "curator_payout_value": 0 }
                        author_share = Decimal(event["data"]["quantity"])
                        paid_out_posts[authorperm]["total_payout_value"] += author_share
                        paid_out_posts[authorperm]["vote_rshares"]  = 0 
                        paid_out_posts[authorperm]["score_hot"]  = 0 
                        paid_out_posts[authorperm]["score_trend"]  = 0 
                        if author_share > 0:
                            self.accountHistoryTrx.add({"account": event["data"]["account"], "token": token, "timestamp": timestamp, "quantity": author_share, "type": "author_reward", "authorperm": authorperm, "trx": op["transactionId"]})
                    elif event["event"] == "createRewardPool" or event["event"] == "updateRewardPool":
                        # const config = { "postRewardCurve": "power", "postRewardCurveParameter": "1.05", "curationRewardCurve": "power", "curationRewardCurveParameter": "0.5", "curationRewardPercentage": 50, "cashoutWindowDays": 7, "rewardPerBlock": "0.375", "voteRegenerationDays": 5, "downvoteRegenerationDays": 5, "stakedRewardPercentage": 50, "votePowerConsumption": 200, "downvotePowerConsumption": 2000, "tags": ["palnet"]};
                        # this.transactions.push(new Transaction(this.blockNumber, 'FAKETX__SMT_11', 'minnowsupport', 'comments', 'updateRewardPool', `{ "symbol": "PAL", "config": ${JSON.stringify(config)}, "isSignedWithActiveKey": true }`));
                        token = contractPayload['symbol']
                        reward_pool_id = event['data']['_id']
                        self.tokenConfigStorage.upsert({ "token": token, "author_curve_exponent": contractPayload["config"]["postRewardCurveParameter"], "curation_curve_exponent": contractPayload["config"]["curationRewardCurveParameter"], "curation_reward_percentage": contractPayload["config"]["curationRewardPercentage"], "cashout_window_days": contractPayload["config"]["cashoutWindowDays"], "reward_pool_id": reward_pool_id, "reward_per_interval": contractPayload["config"]["rewardPerInterval"], "reward_interval_seconds": contractPayload["config"]["rewardIntervalSeconds"], "vote_regeneration_days": contractPayload["config"]["voteRegenerationDays"], "downvoteRegenerationDays": contractPayload["config"]["downvoteRegenerationDays"], "staked_reward_percentage": contractPayload["config"]["stakedRewardPercentage"], "vote_power_consumption": contractPayload["config"]["votePowerConsumption"], "downvote_power_consumption": contractPayload["config"]["downvotePowerConsumption"], "tags": contractPayload["config"]["tags"]})
                        token_config[token] = self.tokenConfigStorage.get(token)
                        token_objects[token] = Token(token, api=self.api)
                    else:
                        print(op)
                        print(event)
                        raise "Error"
            for paid_out_post in paid_out_posts.values():
                self.postTrx.update({"token": paid_out_post["token"], "authorperm": paid_out_post["authorperm"], "last_payout": paid_out_post["last_payout"], "total_payout_value": paid_out_post["total_payout_value"], "curator_payout_value": paid_out_post["curator_payout_value"]})

