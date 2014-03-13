from contextlib import contextmanager
from contextlib import nested

import mock
import testify as T

import wizard
from service_wizard import autosuggest
from service_wizard import config


class SrvReaderWriterTestCase(T.TestCase):
    """I bailed out of this test, but I'll leave this here for now as an
    example of how to interact with the Srv* classes."""
    @T.setup
    def init_service(self):
        paths = wizard.paths.SrvPathBuilder("fake_srvpathbuilder")
        self.srw = wizard.SrvReaderWriter(paths)

class ValidateOptionsTestCase(T.TestCase):
    def test_enable_yelpsoa_config_requires_yelpsoa_config_root(self):
        parser = mock.Mock()
        options = mock.Mock()
        options.enable_yelpsoa_config = True
        options.yelpsoa_config_root = None
        options.enable_puppet = False # Disable checks we don't care about
        options.enable_nagios = False # Disable checks we don't care about
        with T.assert_raises(SystemExit):
            wizard.validate_options(parser, options)

    def test_enable_puppet_requires_puppet_root(self):
        parser = mock.Mock()
        options = mock.Mock()
        options.enable_puppet = True
        options.puppet_root = None
        options.enable_yelpsoa_config = False # Disable checks we don't care about
        options.enable_nagios = False # Disable checks we don't care about
        with T.assert_raises(SystemExit):
            wizard.validate_options(parser, options)

    def test_enable_nagios_requires_nagios_root(self):
        parser = mock.Mock()
        options = mock.Mock()
        options.enable_nagios = True
        options.nagios_root = None
        options.enable_yelpsoa_config = False # Disable checks we don't care about
        options.enable_puppet = False # Disable checks we don't care about
        with T.assert_raises(SystemExit):
            wizard.validate_options(parser, options)

class AutosuggestTestCase(T.TestCase):
    def test_suggest_port(self):
        # mock.patch was very confused by the config module, so I'm doing it
        # this way. One more reason to disapprove of this global config module
        # scheme.
        config.YELPSOA_CONFIG_ROOT = "fake_puppet_root"

        walk_return = [(
            "fake_root",
            "fake_dir",
            [
                "fake_file", # ignored
                "repl_delay_reporter.yaml", # contains 'port' but ignored
                "port",
                "status_port",
                "weird_port", # has bogus out-of-range value
            ]
        )]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_port_from_file_returns = [
            13001,
            13002,
            55555, # bogus out-of-range value
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
        T.assert_equal(actual, 13002 + 1) # highest port + 1

class ParseHostnamesStringTestCase(T.TestCase):
    @T.setup_teardown
    def mock_get_fqdn(self):
        """This test case only cares about the logic in
        parse_hostnames_string(), so patch get_fqdn() to just return what we
        give it.
        """
        def fake_get_fqdn(hostname):
            return hostname
        with mock.patch("wizard.get_fqdn", new=fake_get_fqdn):
            yield

    def test_empty(self):
        runs_on = ""
        expected = [runs_on]
        actual = wizard.parse_hostnames_string(runs_on)
        T.assert_equal(expected, actual)

    def test_one_runs_on(self):
        runs_on = "runs_on1"
        expected = [runs_on]
        actual = wizard.parse_hostnames_string(runs_on)
        T.assert_equal(expected, actual)

    def test_two_runs_on(self):
        runs_on = "runs_on1,runs_on2"
        expected = ["runs_on1", "runs_on2"]
        actual = wizard.parse_hostnames_string(runs_on)
        T.assert_equal(expected, actual)

    def test_two_runs_on_with_space(self):
        runs_on = "runs_on1, runs_on2"
        expected = ["runs_on1", "runs_on2"]
        actual = wizard.parse_hostnames_string(runs_on)
        T.assert_equal(expected, actual)

class GetServiceYamlContentsTestCase(T.TestCase):
    def test_empty(self):
        runs_on = []
        deploys_on = []
        actual = wizard.get_service_yaml_contents(runs_on, deploys_on)

        # Verify entire lines to make sure that e.g. '---' appears as its own
        # line and not as part of 'crazy---service----name'.
        T.assert_in("---\n", actual)
        # I think a blank line would be better but I can't figure out how to
        # get pyyaml to emit that.
        T.assert_in("runs_on: []", actual)
        T.assert_in("deployed_to: []", actual)

    def test_one_runs_on(self):
        runs_on = ["runs_on1"]
        deploys_on = []
        actual = wizard.get_service_yaml_contents(runs_on, deploys_on)

        expected = "runs_on:\n- %s" % "runs_on1"
        T.assert_in(expected, actual)

    def test_two_runs_on(self):
        runs_on = ["runs_on1", "runs_on2"]
        deploys_on = []
        actual = wizard.get_service_yaml_contents(runs_on, deploys_on)

        expected = "runs_on:\n- %s\n- %s" % ("runs_on1", "runs_on2")
        T.assert_in(expected, actual)

class GetEcosystemFromFqdnTestCase(T.TestCase):
    def test_unknown(self):
        fqdn = "unknownhost.unknownsubdomain.yelpcorp.com"
        expected = None
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_hostname_only(self):
        """Exercises the case where the fqdn doesn't match our expectations to
        make sure it doesn't blow up *and* that it returns a miss.
        """
        fqdn = "short-host-only"
        expected = None
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_stagea(self):
        fqdn = "stageaservices1.sldev.yelpcorp.com"
        expected = "stagea"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_stagespam_is_not_stage(self):
        """I'm not sure what to do with stagespam yet (I think it gets its own
        Nagios file) but the point of this test is to make sure STAGE_RE
        doesn't match stagespam so we'll only assert about that fact.
        """
        fqdn = "stagespam1sv.sldev.yelpcorp.com"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_not_equal("stage", actual)

    def test_devb(self):
        fqdn = "srv2-devb.dev.yelpcorp.com"
        expected = "devb"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_sfo1(self):
        fqdn = "srv3-r1-sfo1.prod.yelpcorp.com"
        expected = "sfo1"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_iad1(self):
        fqdn = "srv4-r4-iad1.prod.yelpcorp.com"
        expected = "iad1"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_365(self):
        fqdn = "srv1.365.yelpcorp.com"
        expected = "sfo1"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_sldev(self):
        fqdn = "devsearch2sv.sldev.yelpcorp.com"
        expected = "sldev"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_slwdc(self):
        fqdn = "app3sw.slwdc.yelpcorp.com"
        expected = "slwdc"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)

    def test_sj(self):
        fqdn = "devsearch2sj.sjc.yelpcorp.com"
        expected = "sjc"
        actual = wizard.get_ecosystem_from_fqdn(fqdn)
        T.assert_equal(expected, actual)


class CollateHostsByEcosystem(T.TestCase):
    @contextmanager
    def patch_get_ecosystem_from_fqdn(self, return_value=None):
        def fake_get_ecosystem_from_fqdn(fqdn):
            return return_value or "%s-ecosystem" % fqdn
        with mock.patch("wizard.get_ecosystem_from_fqdn", fake_get_ecosystem_from_fqdn):
            yield

    def test_no_fqdns(self):
        expected = {}
        fqdns = []
        actual = wizard.collate_hosts_by_ecosystem(fqdns)
        T.assert_equal(expected, actual)

    def test_bad_fqdn_is_dropped(self):
        expected = {}
        fqdns = ["bad_fqdn"]
        actual = wizard.collate_hosts_by_ecosystem(fqdns)
        T.assert_equal(expected, actual)

    def test_one_good_fqdn(self):
        fqdn = "fakehost1.fakeecosystem.yelpcorp.com"
        expected = {"%s-ecosystem" % fqdn: ["fakehost1"]}
        fqdns = [fqdn]
        with self.patch_get_ecosystem_from_fqdn():
            actual = wizard.collate_hosts_by_ecosystem(fqdns)
        T.assert_equal(expected, actual)

    def test_two_good_fqdns_different_ecosystem(self):
        fqdn1 = "fakehost1.fakeecosystem.yelpcorp.com"
        fqdn2 = "fakehost2.fakeecosystem.yelpcorp.com"
        expected = {
            "%s-ecosystem" % fqdn1: ["fakehost1"],
            "%s-ecosystem" % fqdn2: ["fakehost2"],
        }
        fqdns = [fqdn1, fqdn2]
        with self.patch_get_ecosystem_from_fqdn():
            actual = wizard.collate_hosts_by_ecosystem(fqdns)
        T.assert_equal(expected, actual)

    def test_two_good_fqdns_same_ecosystem(self):
        fqdn1 = "fakehost1.sameecosystem.yelpcorp.com"
        fqdn2 = "fakehost2.sameecosystem.yelpcorp.com"
        ecosystem = "sameecosystem"
        expected = {ecosystem: ["fakehost1", "fakehost2"]}
        fqdns = [fqdn1, fqdn2]
        with self.patch_get_ecosystem_from_fqdn(ecosystem):
            actual = wizard.collate_hosts_by_ecosystem(fqdns)
        T.assert_equal(expected, actual)


#class GetEcosystemOverrides(T.TestCase):
#    @T.setup_teardown
#    def patch_template(self):
#        with mock.patch("wizard.Template") as self.mock_template:
#            with mock.patch.object(self.mock_template, "substitute") as self.mock_substitute:
#                yield
#
#    def test_empty(self):
#        srvname = "fake_srvname"
#        host_by_ecosystem = {}
#        expected = {}
#        actual = wizard.get_ecosystem_overrides(host_by_ecosystem, srvname)
#        T.assert_equal(expected, actual)
#
#    def test_good_host_by_ecosystem(self):
#        srvname = "fake_srvname"
#        host_by_ecosystem = {"fake_ecosystem": ["fakehost1", "fakehost2"]}
#        wizard.get_ecosystem_overrides(host_by_ecosystem, srvname)
#
#        import ipdb; ipdb.set_trace()
#        assert self.mock_substitute.called
#        template_dict = self.mock_substitute.call_args[1]
#        print template_dict


if __name__ == "__main__":
    T.run()
