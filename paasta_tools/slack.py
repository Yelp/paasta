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

from slack_bolt import App

from paasta_tools.utils import optionally_load_system_paasta_config

log = logging.getLogger(__name__)


class PaastaSlackClient(App):
    def __init__(self, bot_token, app_token):
        if bot_token is None or app_token is None:
            log.warning("No slack tokens available, will only log")
            self.slack_app = None
            super().__init__()
        else:
            self.slack_app = App(token=bot_token)
        self.bot_token = bot_token
        # Will be used alongside SocketModeHandler from sticht
        self.app_token = app_token

    def post(self, channels, message=None, blocks=None, thread_ts=None):
        responses = []
        if self.bot_token is not None:
            for channel in channels:
                log.info(f"Slack notification [{channel}]: {message}")
                response = self.slack_app.client.api_call(
                    api_method="chat.postMessage",
                    params={
                        "channel": channel,
                        "text": message,
                        "blocks": blocks,
                        "thread_ts": thread_ts,
                    },
                )
                if response["ok"] is not True:
                    log.error("Posting to slack failed: {}".format(response["error"]))
                responses.append(response)
        else:
            log.info(f"(not sent to Slack) {channels}: {message}")
        return responses

    def post_single(self, channel, message=None, blocks=None, thread_ts=None):
        if self.bot_token is not None:
            log.info(f"Slack notification [{channel}]: {message}")
            response = self.slack_app.client.api_call(
                api_method="chat.postMessage",
                params={
                    "channel": channel,
                    "text": message,
                    "blocks": blocks,
                    "thread_ts": thread_ts,
                },
            )
            if response["ok"] is not True:
                log.error("Posting to slack failed: {}".format(response["error"]))
            return response
        else:
            log.info(f"(not sent to Slack) {channel}: {message}")
            return {"ok"}


def get_slack_client():
    bot_token = os.environ.get("SLACK_API_TOKEN", None)
    app_token = os.environ.get("SLACK_APP_TOKEN", None)
    if bot_token is None:
        bot_token = optionally_load_system_paasta_config().get_slack_token()
    if app_token is None:
        app_token = optionally_load_system_paasta_config().get_slack_token()
    return PaastaSlackClient(bot_token=bot_token, app_token=app_token)
