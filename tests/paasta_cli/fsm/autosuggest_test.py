from contextlib import nested

import mock
from pytest import yield_fixture

from paasta_tools.paasta_cli.fsm import autosuggest
from paasta_tools.paasta_cli.fsm import config


class TestSuggestPort:
    def test_suggest_port(self):
        # mock.patch was very confused by the config module, so I'm doing it
        # this way. One more reason to disapprove of this global config module
        # scheme.
        config.YELPSOA_CONFIG_ROOT = "fake_yelpsoa_config_root"

        walk_return = [(
            "fake_root",
            "fake_dir",
            [
                "fake_file",  # ignored
                "repl_delay_reporter.yaml",  # contains 'port' but ignored
                "port",
                "status_port",
                "weird_port",  # has bogus out-of-range value
            ]
        )]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_port_from_file_returns = [
            13001,
            13002,
            55555,  # bogus out-of-range value
        ]

        def get_port_from_file_side_effect(*args):
            return get_port_from_file_returns.pop(0)
        mock_get_port_from_file = mock.Mock(side_effect=get_port_from_file_side_effect)
        with nested(
            mock.patch("os.walk", mock_walk),
            mock.patch("paasta_tools.paasta_cli.fsm.autosuggest._get_port_from_file", mock_get_port_from_file),
        ):
            actual = autosuggest.suggest_port()
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        assert mock_get_port_from_file.call_count == 3

        # What we came here for: the actual output of the function under test
        assert actual == 13002 + 1  # highest port + 1


class TestGetSmartstackProxyPortFromFile:
    def test_multiple_stanzas_per_file(self):
        with nested(
            mock.patch("__builtin__.open", autospec=True),
            mock.patch("paasta_tools.paasta_cli.fsm.autosuggest.yaml", autospec=True),
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
            mock.patch("paasta_tools.paasta_cli.fsm.autosuggest._get_smartstack_proxy_port_from_file",
                       mock_get_smartstack_proxy_port_from_file),
        ):
            actual = autosuggest.suggest_smartstack_proxy_port(yelpsoa_config_root)
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        assert mock_get_smartstack_proxy_port_from_file.call_count == 3

        # What we came here for: the actual output of the function under test
        assert actual == 20002 + 1  # highest port + 1


class TestSuggestRunsOn:
    @yield_fixture
    def mock_load_service_yamls(self):
        with mock.patch(
            "paasta_tools.paasta_cli.fsm.service_configuration.load_service_yamls",
            autospec=True,
        ) as mock_load_service_yamls:
                yield mock_load_service_yamls

    @yield_fixture
    def mock_other_stuff(self):
        """Since we don't actually use any of these mocks, I'll patch them
        together and not yield any of them!
        """
        with nested(
            mock.patch(
                "paasta_tools.paasta_cli.fsm.service_configuration.collate_service_yamls",
                autospec=True,
            ),
            mock.patch(
                "paasta_tools.paasta_cli.fsm.autosuggest.suggest_all_hosts",
                autospec=True,
                return_value=""
            ),
            mock.patch(
                "paasta_tools.paasta_cli.fsm.autosuggest.suggest_hosts_for_habitat",
                autospec=True,
                return_value=""
            ),
        ) as (_, _, _):
                yield

    def test_returns_original_if_no_munging_occurred(self):
        expected = "things,not,needing,munging"
        actual = autosuggest.suggest_runs_on(expected)
        assert expected == actual

    def test_does_not_load_yamls_if_no_munging_occurred(self, mock_load_service_yamls):
        runs_on = "things,not,needing,munging"
        autosuggest.suggest_runs_on(runs_on)
        assert 0 == mock_load_service_yamls.call_count

    def test_loads_yamls_if_auto(self, mock_load_service_yamls):
        runs_on = "AUTO"
        autosuggest.suggest_runs_on(runs_on)
        assert 1 == mock_load_service_yamls.call_count

    def test_loads_yamls_if_HABITAT(self, mock_load_service_yamls):
        runs_on = "FAKE_HABITAT1"
        autosuggest.suggest_runs_on(runs_on)
        assert 1 == mock_load_service_yamls.call_count


class TestDiscoverHabitats:
    def test_stage(self):
        collated_service_yamls = {
            "stagex": {
                "stagexhost": 1,
            },
            "xstage": {
                "should_not_be_included": 1,
            },
        }
        habitats = autosuggest.discover_habitats(collated_service_yamls)
        assert "stagex" in habitats
        assert "xstage" not in habitats

    def test_prod(self):
        # Prod values are hardcoded, so even with no discovered habitats they
        # should appear.
        collated_service_yamls = {}
        habitats = autosuggest.discover_habitats(collated_service_yamls)
        assert "iad1" in habitats

    def test_dev(self):
        collated_service_yamls = {
            "devx": {
                "devxhost": 1,
            },
            "xdev": {
                "should_not_be_included": 1,
            },
        }
        habitats = autosuggest.discover_habitats(collated_service_yamls)
        assert "devx" in habitats
        assert "xdev" not in habitats


class SuggestHostsForHabitat:
    def test_habitat_not_in_collated_service_yamls(self):
        collated_service_yamls = {}
        actual = autosuggest.suggest_hosts_for_habitat(collated_service_yamls, "nonexistent")
        assert "" == actual

    def test_not_prod(self):
        """All non-prod habitats have the same workflow, so just test one."""
        expected = "stagexservices1"
        collated_service_yamls = {
            "stagex": {
                expected: 5,
                "stagexservices2": 10,
                "stagex-ineligibile-non-services-box3": 1,
            },
        }
        actual = autosuggest.suggest_hosts_for_habitat(collated_service_yamls, "stagex")
        assert expected == actual

    def test_prod(self):
        expected = "host1,host2"
        collated_service_yamls = {
            "hab1": {
                "host1": 99,
                "host2": 99,
                "host3": 1,
                "host4": 1,
            },
            # All these machines will be filtered out since they're not general service machines.
            "hab2": {
                "host5": 10,
                "host6": 1,
                "host7": 1,
            },
        }
        actual = autosuggest.suggest_hosts_for_habitat(collated_service_yamls, "hab1")
        assert expected == actual


class TestSuggestAllHosts:
    @yield_fixture
    def mock_suggest_hosts_for_habitat(self):
        with mock.patch(
            "paasta_tools.paasta_cli.fsm.autosuggest.suggest_hosts_for_habitat",
            return_value="fake_list_of_hosts",
            autospec=True,
        ) as mock_suggest_hosts_for_habitat:
            yield mock_suggest_hosts_for_habitat

    def test_calls_suggest_hosts_for_habitat(self, mock_suggest_hosts_for_habitat):
        autosuggest.suggest_all_hosts({"unused": "dict"})
        # Make sure we try to suggest at least one habitat.
        assert mock_suggest_hosts_for_habitat.call_count > 0
