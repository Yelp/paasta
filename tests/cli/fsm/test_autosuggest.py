# Copyright 2015-2016 Yelp Inc.
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
import io

import mock
from pytest import raises

from paasta_tools.cli.fsm import autosuggest


class TestGetSmartstackProxyPortFromFile:
    @mock.patch("paasta_tools.cli.fsm.autosuggest.read_etc_services", autospec=True)
    def test_multiple_stanzas_per_file(self, mock_read_etc_services):
        with mock.patch("builtins.open", autospec=True):
            with mock.patch(
                "paasta_tools.cli.fsm.autosuggest.yaml", autospec=True
            ) as mock_yaml:
                mock_yaml.load.return_value = {
                    "main": {"proxy_port": 1},
                    "foo": {"proxy_port": 2},
                }
                actual = autosuggest._get_smartstack_proxy_ports_from_file(
                    "fake_root", "smartstack.yaml"
                )
                assert actual == {1, 2}


# Shamelessly copied from TestSuggestPort
class TestSuggestSmartstackProxyPort:
    @mock.patch("paasta_tools.cli.fsm.autosuggest.read_etc_services", autospec=True)
    def test_suggest_smartstack_proxy_port(self, mock_read_etc_services):
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        walk_return = [
            ("fake_root1", "fake_dir1", ["smartstack.yaml"]),
            ("fake_root2", "fake_dir2", ["smartstack.yaml"]),
            ("fake_root3", "fake_dir3", ["smartstack.yaml"]),
        ]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_smartstack_proxy_ports_from_file_returns = [
            {20001, 20003},
            {20002},
            {55555},  # bogus out-of-range value
        ]

        def get_smarstack_proxy_ports_from_file_side_effect(*args):
            return get_smartstack_proxy_ports_from_file_returns.pop(0)

        mock_get_smartstack_proxy_ports_from_file = mock.Mock(
            side_effect=get_smarstack_proxy_ports_from_file_side_effect
        )
        with mock.patch("os.walk", mock_walk, autospec=None):
            with mock.patch(
                "paasta_tools.cli.fsm.autosuggest._get_smartstack_proxy_ports_from_file",
                mock_get_smartstack_proxy_ports_from_file,
                autospec=None,
            ):
                actual = autosuggest.suggest_smartstack_proxy_port(
                    yelpsoa_config_root, range_min=20001, range_max=20004
                )
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        assert mock_get_smartstack_proxy_ports_from_file.call_count == 3

        # What we came here for: the actual output of the function under test
        assert actual == 20004  # The only available integer in [20001, 20004]

    @mock.patch("paasta_tools.cli.fsm.autosuggest.read_etc_services", autospec=True)
    def test_suggest_smartstack_proxy_port_too_many_services(
        self, mock_read_etc_services
    ):
        """If all the ports are taken, we should raise an error"""
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        walk_return = [
            ("fake_root1", "fake_dir1", ["smartstack.yaml"]),
            ("fake_root2", "fake_dir2", ["smartstack.yaml"]),
            ("fake_root3", "fake_dir3", ["smartstack.yaml"]),
        ]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_smartstack_proxy_ports_from_file_returns = [
            {20001, 20003},
            {20002},
            {55555},  # bogus out-of-range value
        ]

        def get_smarstack_proxy_ports_from_file_side_effect(*args):
            return get_smartstack_proxy_ports_from_file_returns.pop(0)

        mock_get_smartstack_proxy_ports_from_file = mock.Mock(
            side_effect=get_smarstack_proxy_ports_from_file_side_effect
        )
        with mock.patch("os.walk", mock_walk, autospec=None):
            with mock.patch(
                "paasta_tools.cli.fsm.autosuggest._get_smartstack_proxy_ports_from_file",
                mock_get_smartstack_proxy_ports_from_file,
                autospec=None,
            ):
                with raises(Exception) as exc:
                    autosuggest.suggest_smartstack_proxy_port(
                        yelpsoa_config_root, range_min=20001, range_max=20003
                    )
                assert (
                    "There are no more ports available in the range [20001, 20003]"
                    == str(exc.value)
                )


@mock.patch("paasta_tools.cli.fsm.autosuggest.read_etc_services", autospec=True)
def test_get_inuse_ports_from_etc_services_parses_correctly(mock_read_etc_services):
    input_services = """
# by IANA and used in the real-world or are needed by a debian package.
# If you need a huge list of used numbers please install the nmap package.

tcpmux		1/tcp				# TCP port service multiplexer
echo		7/tcp
echo		7/udp
discard		9/tcp		sink null
discard		9/udp		sink null
systat		11/tcp		users
daytime		13/tcp
"""
    mock_read_etc_services.return_value = io.StringIO(input_services)
    actual = autosuggest.get_inuse_ports_from_etc_services()
    expected = {1, 7, 9, 11, 13}
    assert actual == expected
