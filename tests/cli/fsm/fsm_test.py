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
import mock
from pytest import raises
from pytest import yield_fixture

import paasta_tools.cli.cmds.fsm as fsm
from paasta_tools.cli.fsm.questions import _yamlize
from paasta_tools.cli.fsm.service import SrvReaderWriter
from paasta_tools.utils import SystemPaastaConfig


class ValidateOptionsTest:
    @yield_fixture
    def mock_exists(self):
        with mock.patch("paasta_tools.cli.cmds.fsm.exists") as mock_exists:
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
            mock.patch("paasta_tools.cli.cmds.fsm.get_srvname", autospec=True)
        ) as mock_get_srvname:
            yield mock_get_srvname

    @yield_fixture
    def mock_get_service_stanza(self):
        with (
            mock.patch("paasta_tools.cli.cmds.fsm.get_service_stanza", autospec=True)
        ) as mock_get_service_stanza:
            yield mock_get_service_stanza

    @yield_fixture
    def mock_get_smartstack_stanza(self):
        with (
            mock.patch("paasta_tools.cli.cmds.fsm.get_smartstack_stanza", autospec=True)
        ) as mock_get_smartstack_stanza:
            yield mock_get_smartstack_stanza

    @yield_fixture
    def mock_get_marathon_stanza(self):
        with (
            mock.patch("paasta_tools.cli.cmds.fsm.get_marathon_stanza", autospec=True)
        )as mock_get_marathon_stanza:
            yield mock_get_marathon_stanza

    @yield_fixture
    def mock_get_monitoring_stanza(self):
        with (
            mock.patch("paasta_tools.cli.cmds.fsm.get_monitoring_stanza", autospec=True)
        ) as mock_get_monitoring_stanza:
            yield mock_get_monitoring_stanza

    @yield_fixture
    def mock_get_paasta_config(self):
        with (
            mock.patch("paasta_tools.cli.cmds.fsm.load_system_paasta_config", autospec=True)
        ) as mock_get_paasta_config:
            yield mock_get_paasta_config

    def test_everything_specified(
        self,
        mock_get_srvname,
        mock_get_service_stanza,
        mock_get_smartstack_stanza,
        mock_get_marathon_stanza,
        mock_get_monitoring_stanza,
        mock_get_paasta_config,
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
        mock_get_paasta_config.assert_called_once_with()


class TestWritePaastaConfig:
    @yield_fixture
    def mock_srv(self):
        mock_srv = mock.Mock()
        mock_srv.io = mock.Mock(spec_set=SrvReaderWriter)
        yield mock_srv

    @yield_fixture
    def test_paasta_config(self):
        test_paasta_config = SystemPaastaConfig(
            config={
                'fsm_cluster_map': {
                    'pnw-stagea': 'STAGE',
                    'norcal-stageb': 'STAGE',
                    'norcal-devb': 'DEV',
                    'norcal-devc': 'DEV',
                    'norcal-prod': 'PROD',
                    'nova-prod': 'PROD'
                },
                'fsm_deploy_pipeline': {
                    'otto': 'dude',
                },
            },
            directory='/fake/dir',
        )
        yield test_paasta_config

    def test(self, mock_srv, test_paasta_config):
        service_stanza = {'foo': 'bar'}
        smartstack_stanza = {'stack': 'smrt'}
        monitoring_stanza = {'team': 'homer'}
        deploy_stanza = {'otto': 'dude'}
        marathon_stanza = (
            (('DEV', '2015-04-20')),
            (('STAGE', '2015-04-20')),
            (('PROD', '2015-04-20')),
        )

        fsm.write_paasta_config(
            mock_srv,
            service_stanza,
            smartstack_stanza,
            monitoring_stanza,
            test_paasta_config.get_fsm_deploy_pipeline(),
            marathon_stanza,
            test_paasta_config.get_fsm_cluster_map(),
        )

        mock_srv.io.write_file.assert_any_call(
            'smartstack.yaml',
            _yamlize(smartstack_stanza),
        )
        mock_srv.io.write_file.assert_any_call(
            'service.yaml',
            _yamlize(service_stanza),
        )
        mock_srv.io.write_file.assert_any_call(
            'monitoring.yaml',
            _yamlize(monitoring_stanza),
        )
        mock_srv.io.write_file.assert_any_call(
            'deploy.yaml',
            _yamlize(deploy_stanza),
        )
        for (filename, stanza) in marathon_stanza:
            mock_srv.io.write_file.assert_any_call(
                'marathon-%s.yaml' % filename,
                _yamlize(stanza),
            )

        for (clustername, filename) in test_paasta_config.get_fsm_cluster_map().items():
            mock_srv.io.symlink_file_relative.assert_any_call(
                'marathon-%s.yaml' % filename,
                'marathon-%s.yaml' % clustername,
            )
