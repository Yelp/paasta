# Copyright 2015-2017 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import os

from slackclient import SlackClient

from paasta_tools.utils import optionally_load_system_paasta_config

log = logging.getLogger(__name__)


class PaastaSlackClient(SlackClient):
    def __init__(self, token):
        super().__init__(self)
        if token is None:
            log.warning("No slack token available, will only log")
            self.sc = None
        else:
            self.sc = SlackClient(token)
        self.token = token

    def post(self, channels, message=None, blocks=None, thread_ts=None):
        responses = []
        if self.token is not None:
            for channel in channels:
                log.info(f"Slack notification [{channel}]: {message}")
                response = self.sc.api_call(
                    "chat.postMessage",
                    channel=channel,
                    text=message,
                    blocks=blocks,
                    thread_ts=thread_ts,
                )
                if response["ok"] is not True:
                    log.error("Posting to slack failed: {}".format(response["error"]))
                responses.append(response)
        else:
            log.info(f"(not sent to Slack) {channels}: {message}")
        return responses

    def post_single(self, channel, message=None, blocks=None, thread_ts=None):
        if self.token is not None:
            log.info(f"Slack notification [{channel}]: {message}")
            response = self.sc.api_call(
                "chat.postMessage",
                channel=channel,
                text=message,
                blocks=blocks,
                thread_ts=thread_ts,
            )
            if response["ok"] is not True:
                log.error("Posting to slack failed: {}".format(response["error"]))
            return response
        else:
            log.info(f"(not sent to Slack) {channel}: {message}")
            return {"ok"}


def get_slack_client():
    token = os.environ.get("SLACK_API_TOKEN", None)
    if token is None:
        token = optionally_load_system_paasta_config().get_slack_token()
    return PaastaSlackClient(token=token)
