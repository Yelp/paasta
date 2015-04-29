import mock
from pytest import raises
from pytest import yield_fixture

import paasta_tools.paasta_cli.cmds.fsm as fsm
from paasta_tools.paasta_cli.fsm.questions import _yamlize
from paasta_tools.paasta_cli.fsm.service import SrvReaderWriter


class ValidateOptionsTest:
    @yield_fixture
    def mock_exists(self):
        with mock.patch("paasta_tools.paasta_cli.cmds.fsm.exists") as mock_exists:
            # Happpy path by default
            mock_exists.return_value = True
            yield mock_exists

    def test_yelpsoa_config_root_exists(self, mock_exists):
        parser = mock.Mock()
        args = mock.Mock()
        args.yelpsoa_config_root = "non-existent thing"

        mock_exists.return_value = False
        with raises(SystemExit) as err:
            fsm.validate_args(parser, args)
        assert ("I'd Really Rather You Didn't Use A Non-Existent --yelpsoa-config-root"
                "Like %s" % args.yelpsoa_config_root) in err


class TestGetPaastaConfig:
    @yield_fixture
    def mock_get_srvname(self):
        with (
            mock.patch("paasta_tools.paasta_cli.cmds.fsm.get_srvname", autospec=True)
        ) as mock_get_srvname:
            yield mock_get_srvname

    @yield_fixture
    def mock_get_service_stanza(self):
        with (
            mock.patch("paasta_tools.paasta_cli.cmds.fsm.get_service_stanza", autospec=True)
        ) as mock_get_service_stanza:
            yield mock_get_service_stanza

    @yield_fixture
    def mock_get_smartstack_stanza(self):
        with (
            mock.patch("paasta_tools.paasta_cli.cmds.fsm.get_smartstack_stanza", autospec=True)
        ) as mock_get_smartstack_stanza:
            yield mock_get_smartstack_stanza

    @yield_fixture
    def mock_get_marathon_stanza(self):
        with (
            mock.patch("paasta_tools.paasta_cli.cmds.fsm.get_marathon_stanza", autospec=True)
        )as mock_get_marathon_stanza:
            yield mock_get_marathon_stanza

    @yield_fixture
    def mock_get_monitoring_stanza(self):
        with (
            mock.patch("paasta_tools.paasta_cli.cmds.fsm.get_monitoring_stanza", autospec=True)
        ) as mock_get_monitoring_stanza:
            yield mock_get_monitoring_stanza

    @yield_fixture
    def mock_get_deploy_stanza(self):
        with (
            mock.patch("paasta_tools.paasta_cli.cmds.fsm.get_deploy_stanza", autospec=True)
        ) as mock_get_deploy_stanza:
            yield mock_get_deploy_stanza

    def test_everything_specified(
        self,
        mock_get_srvname,
        mock_get_service_stanza,
        mock_get_smartstack_stanza,
        mock_get_marathon_stanza,
        mock_get_monitoring_stanza,
        mock_get_deploy_stanza
    ):
        """A sort of happy path test because we don't care about the logic in
        the individual get_* methods, just that all of them get called as
        expected.
        """
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        srvname = "services/fake_srvname"
        auto = "UNUSED"
        port = 12345
        team = "america world police"
        description = 'a thing'
        external_link = 'http://bla'
        fsm.get_paasta_config(yelpsoa_config_root, srvname, auto, port, team, description, external_link)

        mock_get_srvname.assert_called_once_with(srvname, auto)
        mock_get_service_stanza.assert_called_once_with(description, external_link, auto)
        mock_get_smartstack_stanza.assert_called_once_with(yelpsoa_config_root, auto, port)
        mock_get_marathon_stanza.assert_called_once_with()
        mock_get_monitoring_stanza.assert_called_once_with(auto, team)
        mock_get_deploy_stanza.assert_called_once_with()


class TestWritePaastaConfig:
    @yield_fixture
    def mock_srv(self):
        mock_srv = mock.Mock()
        mock_srv.io = mock.Mock(spec_set=SrvReaderWriter)
        yield mock_srv

    @yield_fixture
    def mock_get_clusternames_from_deploy_stanza(self):
        with mock.patch(
            "paasta_tools.paasta_cli.cmds.fsm.get_clusternames_from_deploy_stanza", autospec=True,
        ) as mock_get_clusternames_from_deploy_stanza:
            yield mock_get_clusternames_from_deploy_stanza

    def test(self, mock_srv, mock_get_clusternames_from_deploy_stanza):
        service_stanza = {"foo": "bar"}
        smartstack_stanza = {"stack": "smrt"}
        monitoring_stanza = {"team": "homer"}
        deploy_stanza = {"otto": "dude"}
        marathon_stanza = {"springfield": "2015-04-20"}
        clusternames = set([
            "flanders",
            "van-houten",
            "wiggum",
        ])

        mock_get_clusternames_from_deploy_stanza.return_value = clusternames
        fsm.write_paasta_config(
            mock_srv,
            service_stanza,
            smartstack_stanza,
            monitoring_stanza,
            deploy_stanza,
            marathon_stanza,
        )

        mock_srv.io.write_file.assert_any_call(
            "smartstack.yaml",
            _yamlize(smartstack_stanza),
        )
        mock_srv.io.write_file.assert_any_call(
            "service.yaml",
            _yamlize(service_stanza),
        )
        mock_srv.io.write_file.assert_any_call(
            "monitoring.yaml",
            _yamlize(monitoring_stanza),
        )
        mock_srv.io.write_file.assert_any_call(
            "deploy.yaml",
            _yamlize(deploy_stanza),
        )
        mock_srv.io.write_file.assert_any_call(
            "marathon-SHARED.yaml",
            _yamlize(marathon_stanza),
        )

        for clustername in clusternames:
            mock_srv.io.symlink_file_relative.assert_any_call(
                "marathon-SHARED.yaml",
                "marathon-%s.yaml" % clustername,
            )
