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
# We just want to test that task_processing is available in the virtualenv
import mock
from slackclient import SlackClient

from paasta_tools.slack import PaastaSlackClient


@mock.patch("slackclient.SlackClient", autospec=True)
def test_slack_client_doesnt_post_with_no_token(mock_SlackClient):
    psc = PaastaSlackClient(token=None)
    assert psc.post(channels=["foo"], message="bar") == []
    assert mock_SlackClient.api_call.call_count == 0


def test_slack_client_posts_to_multiple_channels():
    fake_sc = mock.create_autospec(SlackClient)
    fake_sc.api_call.side_effect = ({"ok": True}, {"ok": False, "error": "blah"})
    with mock.patch(
        "paasta_tools.slack.SlackClient", autospec=True, return_value=fake_sc
    ):
        psc = PaastaSlackClient(token="fake_token")
        assert psc.post(channels=["1", "2"], message="bar") == [
            {"ok": True},
            {"ok": False, "error": "blah"},
        ]
        assert fake_sc.api_call.call_count == 2, fake_sc.call_args
