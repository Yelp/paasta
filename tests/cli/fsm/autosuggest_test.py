# Copyright 2015 Yelp Inc.
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
from contextlib import nested

import mock
from pytest import raises

from paasta_tools.cli.fsm import autosuggest


class TestGetSmartstackProxyPortFromFile:
    def test_multiple_stanzas_per_file(self):
        with nested(
            mock.patch("__builtin__.open", autospec=True),
            mock.patch("paasta_tools.cli.fsm.autosuggest.yaml", autospec=True),
        ) as (
            mock_open,
            mock_yaml,
        ):
            mock_yaml.load.return_value = {
                "main": {
                    "proxy_port": 1,
                },
                "foo": {
                    "proxy_port": 2,
                },
            }
            actual = autosuggest._get_smartstack_proxy_port_from_file(
                "fake_root",
                "smartstack.yaml",
            )
            assert actual == 2


# Shamelessly copied from TestSuggestPort
class TestSuggestSmartstackProxyPort:
    def test_suggest_smartstack_proxy_port(self):
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        walk_return = [
            ("fake_root1", "fake_dir1", ["service.yaml"]),
            ("fake_root2", "fake_dir2", ["smartstack.yaml"]),
            ("fake_root3", "fake_dir3", ["service.yaml"]),
        ]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_smartstack_proxy_port_from_file_returns = [
            20001,
            20002,
            55555,  # bogus out-of-range value
        ]

        def get_smarstack_proxy_port_from_file_side_effect(*args):
            return get_smartstack_proxy_port_from_file_returns.pop(0)
        mock_get_smartstack_proxy_port_from_file = mock.Mock(side_effect=get_smarstack_proxy_port_from_file_side_effect)
        with nested(
            mock.patch("os.walk", mock_walk),
            mock.patch("paasta_tools.cli.fsm.autosuggest._get_smartstack_proxy_port_from_file",
                       mock_get_smartstack_proxy_port_from_file),
        ):
            actual = autosuggest.suggest_smartstack_proxy_port(yelpsoa_config_root, range_min=20001, range_max=20003)
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        assert mock_get_smartstack_proxy_port_from_file.call_count == 3

        # What we came here for: the actual output of the function under test
        assert actual == 20003  # The only available integer in [20001, 20003]

    def test_suggest_smartstack_proxy_port_too_many_services(self):
        """If all the ports are taken, we should raise an error"""
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        walk_return = [
            ("fake_root1", "fake_dir1", ["service.yaml"]),
            ("fake_root2", "fake_dir2", ["smartstack.yaml"]),
            ("fake_root3", "fake_dir3", ["service.yaml"]),
        ]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_smartstack_proxy_port_from_file_returns = [
            20001,
            20002,
            55555,  # bogus out-of-range value
        ]

        def get_smarstack_proxy_port_from_file_side_effect(*args):
            return get_smartstack_proxy_port_from_file_returns.pop(0)
        mock_get_smartstack_proxy_port_from_file = mock.Mock(side_effect=get_smarstack_proxy_port_from_file_side_effect)
        with nested(
            mock.patch("os.walk", mock_walk),
            mock.patch("paasta_tools.cli.fsm.autosuggest._get_smartstack_proxy_port_from_file",
                       mock_get_smartstack_proxy_port_from_file),
        ):
            with raises(Exception) as exc:
                autosuggest.suggest_smartstack_proxy_port(yelpsoa_config_root, range_min=20001,
                                                          range_max=20002)
            assert "There are no more ports available in the range [20001, 20002]" == str(exc.value)
