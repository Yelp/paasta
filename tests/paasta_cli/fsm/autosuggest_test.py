from contextlib import nested

import mock
import testify as T

from service_wizard import autosuggest
from service_wizard import config


class SuggestPortTestCase(T.TestCase):
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
            mock.patch("service_wizard.autosuggest._get_port_from_file", mock_get_port_from_file),
        ):
            actual = autosuggest.suggest_port()
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        T.assert_equal(mock_get_port_from_file.call_count, 3)

        # What we came here for: the actual output of the function under test
        T.assert_equal(actual, 13002 + 1)  # highest port + 1


# Shamelessly copied from SuggestPortTestCase
class SuggestSmartstackProxyPortTestCase(T.TestCase):
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
            mock.patch("service_wizard.autosuggest._get_smartstack_proxy_port_from_file",
                       mock_get_smartstack_proxy_port_from_file),
        ):
            actual = autosuggest.suggest_smartstack_proxy_port(yelpsoa_config_root)
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        T.assert_equal(mock_get_smartstack_proxy_port_from_file.call_count, 3)

        # What we came here for: the actual output of the function under test
        T.assert_equal(actual, 20002 + 1)  # highest port + 1


class SuggestRunsOnTestCase(T.TestCase):
    @T.setup_teardown
    def mock_service_configuration_lookups(self):
        with nested(
            mock.patch("service_wizard.service_configuration.load_service_yamls"),
            mock.patch("service_wizard.service_configuration.collate_service_yamls"),
            mock.patch("service_wizard.autosuggest.suggest_all_hosts", return_value=""),
            mock.patch("service_wizard.autosuggest.suggest_hosts_for_habitat", return_value=""),
        ) as (self.mock_load_service_yamls, _, _, _):
                yield

    def test_returns_original_if_no_munging_occurred(self):
        expected = "things,not,needing,munging"
        actual = autosuggest.suggest_runs_on(expected)
        T.assert_equal(expected, actual)

    def test_does_not_load_yamls_if_no_munging_occurred(self):
        runs_on = "things,not,needing,munging"
        autosuggest.suggest_runs_on(runs_on)
        T.assert_equal(0, self.mock_load_service_yamls.call_count)

    def test_loads_yamls_if_auto(self):
        runs_on = "AUTO"
        autosuggest.suggest_runs_on(runs_on)
        T.assert_equal(1, self.mock_load_service_yamls.call_count)

    def test_loads_yamls_if_HABITAT(self):
        runs_on = "FAKE_HABITAT1"
        autosuggest.suggest_runs_on(runs_on)
        T.assert_equal(1, self.mock_load_service_yamls.call_count)


class DiscoverHabitatsTestCase(T.TestCase):
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
        T.assert_in("stagex", habitats)
        T.assert_not_in("xstage", habitats)

    def test_prod(self):
        # Prod values are hardcoded, so even with no discovered habitats they
        # should appear.
        collated_service_yamls = {}
        habitats = autosuggest.discover_habitats(collated_service_yamls)
        T.assert_in("iad1", habitats)

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
        T.assert_in("devx", habitats)
        T.assert_not_in("xdev", habitats)


class SuggestHostsForHabitat(T.TestCase):
    def test_habitat_not_in_collated_service_yamls(self):
        collated_service_yamls = {}
        actual = autosuggest.suggest_hosts_for_habitat(collated_service_yamls, "nonexistent")
        T.assert_equal("", actual)

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
        T.assert_equal(expected, actual)

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
        T.assert_equal(expected, actual)


class SuggestAllHostsTestCase(T.TestCase):
    @T.setup_teardown
    def mock_suggest_hosts_for_habitat(self):
        with mock.patch(
            "service_wizard.autosuggest.suggest_hosts_for_habitat",
            return_value="fake_list_of_hosts",
        ) as self.mock_suggest_hosts_for_habitat:
            yield

    def test_calls_suggest_hosts_for_habitat(self):
        autosuggest.suggest_all_hosts({"unused": "dict"})
        # Make sure we try to suggest at least one habitat.
        T.assert_gt(self.mock_suggest_hosts_for_habitat.call_count, 0)
