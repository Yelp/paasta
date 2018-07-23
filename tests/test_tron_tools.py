import mock
import pytest

from paasta_tools import tron_tools
from paasta_tools.tron_tools import MASTER_NAMESPACE
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable


class TestTronConfig:

    @pytest.fixture
    def config_dict(self):
        return {
            'cluster_name': 'dev-batch',
            'default_paasta_cluster': 'dev-oregon',
            'url': 'http://mesos-master.com:2000',
        }

    def test_normal(self, config_dict):
        config = tron_tools.TronConfig(config_dict)
        assert config.get_cluster_name() == 'dev-batch'
        assert config.get_default_paasta_cluster() == 'dev-oregon'
        assert config.get_url() == 'http://mesos-master.com:2000'

    def test_no_cluster_name(self, config_dict):
        del config_dict['cluster_name']
        config = tron_tools.TronConfig(config_dict)
        with pytest.raises(tron_tools.TronNotConfigured):
            config.get_cluster_name()

    def test_no_default_paasta_cluster(self, config_dict):
        del config_dict['default_paasta_cluster']
        config = tron_tools.TronConfig(config_dict)
        with pytest.raises(tron_tools.TronNotConfigured):
            config.get_default_paasta_cluster()

    def test_no_url(self, config_dict):
        del config_dict['url']
        config = tron_tools.TronConfig(config_dict)
        with pytest.raises(tron_tools.TronNotConfigured):
            config.get_url()


class TestTronActionConfig:

    def test_get_job_name(self):
        action_dict = {
            'name': 'print',
            'command': 'echo something',
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('cool_job', 'print'),
            config_dict=action_dict,
            branch_dict={},
        )
        assert action_config.get_job_name() == 'cool_job'

    def test_get_action_name(self):
        action_dict = {
            'name': 'sleep',
            'command': 'sleep 10',
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'sleep'),
            config_dict=action_dict,
            branch_dict={},
        )
        assert action_config.get_action_name() == 'sleep'

    def test_get_cluster(self):
        action_dict = {
            'name': 'do_something',
            'command': 'echo something',
            'cluster': 'dev-oregon',
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'do_something'),
            config_dict=action_dict,
            branch_dict={},
        )
        assert action_config.get_cluster() == 'dev-oregon'

    def test_get_executor_default(self):
        action_dict = {
            'name': 'do_something',
            'command': 'echo something',
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'do_something'),
            config_dict=action_dict,
            branch_dict={},
        )
        assert action_config.get_executor() is None

    def test_get_executor_paasta(self):
        action_dict = {
            'name': 'do_something',
            'command': 'echo something',
            'executor': 'paasta',
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'do_something'),
            config_dict=action_dict,
            branch_dict={},
        )
        assert action_config.get_executor() == 'mesos'


class TestTronJobConfig:

    @pytest.mark.parametrize(
        'action_service,action_deploy,action_cluster', [
            (None, None, None),
            (None, 'special_deploy', None),
            ('other_service', None, None),
            (None, None, 'other-cluster'),
        ],
    )
    @mock.patch('paasta_tools.tron_tools.load_v2_deployments_json', autospec=True)
    def test_get_action_config(
        self,
        mock_load_deployments,
        action_service,
        action_deploy,
        action_cluster,
    ):
        """Check resulting action config with various overrides from the action."""
        action_dict = {
            'name': 'normal',
            'command': 'echo first',
        }
        if action_service:
            action_dict['service'] = action_service
        if action_deploy:
            action_dict['deploy_group'] = action_deploy
        if action_cluster:
            action_dict['cluster'] = action_cluster

        job_service = 'my_service'
        job_deploy = 'prod'
        default_cluster = 'paasta-dev'
        expected_service = action_service or job_service
        expected_deploy = action_deploy or job_deploy
        expected_cluster = action_cluster or default_cluster

        job_dict = {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'service': job_service,
            'deploy_group': job_deploy,
            'max_runtime': '2h',
            'actions': [action_dict],
        }
        soa_dir = '/other_dir'
        job_config = tron_tools.TronJobConfig(job_dict, soa_dir=soa_dir)

        action_config = job_config._get_action_config(action_dict, default_cluster)

        mock_load_deployments.assert_called_once_with(expected_service, soa_dir)
        mock_deployments_json = mock_load_deployments.return_value
        mock_deployments_json.get_docker_image_for_deploy_group.assert_called_once_with(expected_deploy)
        mock_deployments_json.get_git_sha_for_deploy_group.assert_called_once_with(expected_deploy)
        expected_branch_dict = {
            'docker_image': mock_deployments_json.get_docker_image_for_deploy_group.return_value,
            'git_sha': mock_deployments_json.get_git_sha_for_deploy_group.return_value,
            'desired_state': 'start',
            'force_bounce': None,
        }

        assert action_config == tron_tools.TronActionConfig(
            service=expected_service,
            instance=tron_tools.compose_instance('my_job', 'normal'),
            config_dict={
                'name': 'normal',
                'command': 'echo first',
                'cluster': expected_cluster,
                'service': expected_service,
                'deploy_group': expected_deploy,
            },
            branch_dict=expected_branch_dict,
            soa_dir=soa_dir,
        )

    @mock.patch('paasta_tools.tron_tools.load_v2_deployments_json', autospec=True)
    def test_get_action_config_no_deployment(
        self,
        mock_load_deployments,
    ):
        action_dict = {
            'name': 'normal',
            'command': 'echo first',
        }
        job_dict = {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'service': 'my_service',
            'deploy_group': 'prod',
            'max_runtime': '2h',
            'actions': [action_dict],
        }
        job_config = tron_tools.TronJobConfig(job_dict)
        mock_load_deployments.side_effect = NoDeploymentsAvailable

        with pytest.raises(tron_tools.InvalidTronConfig):
            job_config._get_action_config(action_dict, 'some-cluster')

    @mock.patch('paasta_tools.tron_tools.load_v2_deployments_json', autospec=True)
    def test_get_action_config_load_deployments_false(
        self,
        mock_load_deployments,
    ):
        action_dict = {
            'name': 'normal',
            'command': 'echo first',
        }
        job_dict = {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'service': 'my_service',
            'deploy_group': 'prod',
            'max_runtime': '2h',
            'actions': [action_dict],
        }
        soa_dir = '/other_dir'
        default_cluster = 'paasta-dev'
        job_config = tron_tools.TronJobConfig(
            job_dict,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        mock_load_deployments.side_effect = NoDeploymentsAvailable

        action_config = job_config._get_action_config(action_dict, default_cluster)

        assert mock_load_deployments.call_count == 0
        assert action_config == tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'normal'),
            config_dict={
                'name': 'normal',
                'command': 'echo first',
                'cluster': default_cluster,
                'service': 'my_service',
                'deploy_group': 'prod',
            },
            branch_dict=None,
            soa_dir=soa_dir,
        )

    @mock.patch('paasta_tools.tron_tools.TronJobConfig._get_action_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_tron_action_dict', autospec=True)
    def test_format_tron_job_dict(
        self,
        mock_format_action,
        mock_get_action_config,
    ):
        action_dict = {
            'name': 'normal',
            'command': 'echo first',
        }
        job_dict = {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'service': 'my_service',
            'deploy_group': 'prod',
            'max_runtime': '2h',
            'actions': [action_dict],
            'expected_runtime': '1h',
        }
        soa_dir = '/other_dir'
        job_config = tron_tools.TronJobConfig(job_dict, soa_dir=soa_dir)
        fqdn_format = 'paasta-{cluster:s}'
        default_cluster = 'paasta-dev'

        result = tron_tools.format_tron_job_dict(job_config, fqdn_format, default_cluster)

        mock_get_action_config.assert_called_once_with(job_config, action_dict, default_cluster)
        mock_format_action.assert_called_once_with(mock_get_action_config.return_value, fqdn_format)

        assert result == {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'max_runtime': '2h',
            'actions': [mock_format_action.return_value],
            'expected_runtime': '1h',
        }

    @mock.patch('paasta_tools.tron_tools.TronJobConfig._get_action_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_tron_action_dict', autospec=True)
    def test_format_tron_job_dict_with_cleanup_action(
        self,
        mock_format_action,
        mock_get_action_config,
    ):
        job_dict = {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'service': 'my_service',
            'deploy_group': 'prod',
            'max_runtime': '2h',
            'actions': [{
                'name': 'normal',
                'command': 'echo first',
            }],
            'cleanup_action': {
                'command': 'rm *',
            },
        }
        job_config = tron_tools.TronJobConfig(job_dict)

        result = tron_tools.format_tron_job_dict(job_config, 'paasta-{cluster:s}', 'paasta-dev')

        assert mock_get_action_config.call_count == 2
        assert mock_format_action.call_count == 2
        assert result == {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'max_runtime': '2h',
            'actions': [mock_format_action.return_value],
            'cleanup_action': mock_format_action.return_value,
        }

    def test_validate_all_actions(self):
        job_dict = {
            'name': 'my_job',
            'node': 'batch_server',
            'schedule': 'daily 12:10:00',
            'actions': [
                {
                    'name': 'first',
                    'command': 'echo first',
                    'cpus': 'bad string',
                },
                {
                    'name': 'second',
                    'command': 'echo second',
                    'mem': 'not a number',
                },
            ],
            'cleanup_action': {
                'command': 'rm *',
                'cpus': 'also bad',
            },
        }
        job_config = tron_tools.TronJobConfig(job_dict)
        errors = job_config.validate()
        assert len(errors) == 3


class TestTronTools:

    @mock.patch('paasta_tools.tron_tools.load_system_paasta_config', autospec=True)
    def test_load_tron_config(self, mock_system_paasta_config):
        result = tron_tools.load_tron_config()
        assert mock_system_paasta_config.return_value.get_tron_config.call_count == 1
        assert result == tron_tools.TronConfig(mock_system_paasta_config.return_value.get_tron_config.return_value)

    @mock.patch('paasta_tools.tron_tools.load_tron_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.TronClient', autospec=True)
    def test_get_tron_client(self, mock_client, mock_system_tron_config):
        result = tron_tools.get_tron_client()
        assert mock_system_tron_config.return_value.get_url.call_count == 1
        mock_client.assert_called_once_with(mock_system_tron_config.return_value.get_url.return_value)
        assert result == mock_client.return_value

    def test_compose_instance(self):
        result = tron_tools.compose_instance('great_job', 'fast_action')
        assert result == 'great_job.fast_action'

    def test_decompose_instance_valid(self):
        result = tron_tools.decompose_instance('job_a.start')
        assert result == ('job_a', 'start')

    def test_decompose_instance_invalid(self):
        with pytest.raises(InvalidInstanceConfig):
            tron_tools.decompose_instance('job_a')

    def test_format_master_config(self):
        master_config = {
            'some_key': 101,
            'another': 'hello',
            'mesos_options': {
                'default_volumes': [{
                    'container_path': '/nail/tmp',
                    'host_path': '/nail/tmp',
                    'mode': 'RW',
                }],
                'other_mesos': True,
            },
        }
        paasta_volumes = [{
            'containerPath': '/nail/other',
            'hostPath': '/other/home',
            'mode': 'RW',
        }]
        dockercfg = 'file://somewhere'
        result = tron_tools.format_master_config(master_config, paasta_volumes, dockercfg)
        assert result == {
            'some_key': 101,
            'another': 'hello',
            'mesos_options': {
                'default_volumes': [{
                    'container_path': '/nail/other',
                    'host_path': '/other/home',
                    'mode': 'RW',
                }],
                'dockercfg_location': dockercfg,
                'other_mesos': True,
            },
        }

    def test_format_tron_action_dict_default_executor(self):
        action_dict = {
            'name': 'do_something',
            'command': 'echo something',
            'requires': ['required_action'],
            'retries': 2,
            'expected_runtime': '30m',
        }
        branch_dict = {
            'docker_image': 'my_service:paasta-123abcde',
            'git_sha': 'aabbcc44',
            'desired_state': 'start',
            'force_bounce': None,
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'do_something'),
            config_dict=action_dict,
            branch_dict=branch_dict,
        )
        result = tron_tools.format_tron_action_dict(action_config, '{cluster:s}.com')
        assert result == {
            'name': 'do_something',
            'command': 'echo something',
            'requires': ['required_action'],
            'retries': 2,
            'expected_runtime': '30m',
        }

    def test_format_tron_action_dict_paasta(self):
        action_dict = {
            'name': 'do_something',
            'command': 'echo something',
            'requires': ['required_action'],
            'retries': 2,
            'retries_delay': '5m',
            'cluster': 'paasta-dev',
            'service': 'my_service',
            'deploy_group': 'prod',
            'executor': 'paasta',
            'cpus': 2,
            'mem': 1200,
            'pool': 'special_pool',
            'env': {'SHELL': '/bin/bash'},
            'extra_volumes': [
                {'containerPath': '/nail/tmp', 'hostPath': '/nail/tmp', 'mode': 'RW'},
            ],
        }
        branch_dict = {
            'docker_image': 'my_service:paasta-123abcde',
            'git_sha': 'aabbcc44',
            'desired_state': 'start',
            'force_bounce': None,
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'do_something'),
            config_dict=action_dict,
            branch_dict=branch_dict,
        )

        with mock.patch.object(
            action_config,
            'get_docker_registry',
            return_value='docker-registry.com:400',
        ):
            result = tron_tools.format_tron_action_dict(action_config, '{cluster:s}.com')

        assert result == {
            'name': 'do_something',
            'command': 'echo something',
            'requires': ['required_action'],
            'retries': 2,
            'retries_delay': '5m',
            'mesos_address': 'paasta-dev.com',
            'docker_image': mock.ANY,
            'executor': 'mesos',
            'cpus': 2,
            'mem': 1200,
            'env': mock.ANY,
            'extra_volumes': [{
                'container_path': '/nail/tmp',
                'host_path': '/nail/tmp',
                'mode': 'RW',
            }],
            'docker_parameters': mock.ANY,
            'constraints': [
                {
                    'attribute': 'pool',
                    'operator': 'LIKE',
                    'value': 'special_pool',
                },
            ],
        }
        expected_docker = '{}/{}'.format('docker-registry.com:400', branch_dict['docker_image'])
        assert result['docker_image'] == expected_docker
        assert result['env']['SHELL'] == '/bin/bash'
        assert isinstance(result['docker_parameters'], list)

    def test_format_tron_action_dict_paasta_no_branch_dict(self):
        action_dict = {
            'name': 'do_something',
            'command': 'echo something',
            'requires': ['required_action'],
            'retries': 2,
            'cluster': 'paasta-dev',
            'service': 'my_service',
            'deploy_group': 'prod',
            'executor': 'paasta',
            'cpus': 2,
            'mem': 1200,
            'pool': 'special_pool',
            'env': {'SHELL': '/bin/bash'},
            'extra_volumes': [
                {'containerPath': '/nail/tmp', 'hostPath': '/nail/tmp', 'mode': 'RW'},
            ],
        }
        action_config = tron_tools.TronActionConfig(
            service='my_service',
            instance=tron_tools.compose_instance('my_job', 'do_something'),
            config_dict=action_dict,
            branch_dict=None,
        )

        result = tron_tools.format_tron_action_dict(action_config, '{cluster:s}.com')

        assert result == {
            'name': 'do_something',
            'command': 'echo something',
            'requires': ['required_action'],
            'retries': 2,
            'mesos_address': 'paasta-dev.com',
            'docker_image': '',
            'executor': 'mesos',
            'cpus': 2,
            'mem': 1200,
            'env': mock.ANY,
            'extra_volumes': [{
                'container_path': '/nail/tmp',
                'host_path': '/nail/tmp',
                'mode': 'RW',
            }],
            'docker_parameters': mock.ANY,
            'constraints': [
                {
                    'attribute': 'pool',
                    'operator': 'LIKE',
                    'value': 'special_pool',
                },
            ],
        }
        assert result['env']['SHELL'] == '/bin/bash'
        assert isinstance(result['docker_parameters'], list)

    @mock.patch('paasta_tools.tron_tools.service_configuration_lib.read_extra_service_information', autospec=True)
    @mock.patch('paasta_tools.tron_tools.service_configuration_lib._read_yaml_file', autospec=True)
    @mock.patch('paasta_tools.tron_tools.TronJobConfig', autospec=True)
    def test_load_tron_from_service_dir(self, mock_job_config, mock_read_file, mock_read_service_info):
        job_1 = mock.Mock()
        job_2 = mock.Mock()
        config_dict = {
            'value_a': 20,
            'other_value': 'string',
            'jobs': [job_1, job_2],
        }
        mock_read_service_info.return_value = config_dict
        mock_read_file.return_value = {}
        soa_dir = '/other/services'

        job_configs, extra_config = tron_tools.load_tron_service_config(
            'foo',
            'dev',
            load_deployments=True,
            soa_dir=soa_dir,
        )
        assert extra_config == {
            'value_a': 20,
            'other_value': 'string',
        }
        assert job_configs == [mock_job_config.return_value for i in range(2)]
        assert mock_job_config.call_args_list == [
            mock.call(config_dict=job_1, load_deployments=True, soa_dir=soa_dir),
            mock.call(config_dict=job_2, load_deployments=True, soa_dir=soa_dir),
        ]
        assert mock_read_service_info.call_count == 1
        assert mock_read_file.call_count == 0
        mock_read_service_info.assert_has_calls([mock.call('foo', 'tron-dev', '/other/services')])

    @mock.patch('paasta_tools.tron_tools.service_configuration_lib.read_extra_service_information', autospec=True)
    @mock.patch('paasta_tools.tron_tools.service_configuration_lib._read_yaml_file', autospec=True)
    @mock.patch('paasta_tools.tron_tools.TronJobConfig', autospec=True)
    def test_load_tron_from_tron_dir(self, mock_job_config, mock_read_file, mock_read_service_info):
        job_1 = mock.Mock()
        job_2 = mock.Mock()
        config_dict = {
            'value_a': 20,
            'other_value': 'string',
            'jobs': [job_1, job_2],
        }
        mock_read_service_info.return_value = {}
        mock_read_file.return_value = config_dict
        soa_dir = '/other/services'

        job_configs, extra_config = tron_tools.load_tron_service_config(
            'foo',
            'dev',
            load_deployments=True,
            soa_dir=soa_dir,
        )
        assert extra_config == {
            'value_a': 20,
            'other_value': 'string',
        }
        assert job_configs == [mock_job_config.return_value for i in range(2)]
        assert mock_job_config.call_args_list == [
            mock.call(config_dict=job_1, load_deployments=True, soa_dir=soa_dir),
            mock.call(config_dict=job_2, load_deployments=True, soa_dir=soa_dir),
        ]
        assert mock_read_service_info.call_count == 1
        assert mock_read_file.call_count == 1
        mock_read_service_info.assert_has_calls([mock.call('foo', 'tron-dev', '/other/services')])
        mock_read_file.assert_has_calls([mock.call('/other/services/tron/dev/foo.yaml')])

    @mock.patch('paasta_tools.tron_tools.service_configuration_lib.read_extra_service_information', autospec=True)
    @mock.patch('paasta_tools.tron_tools.service_configuration_lib._read_yaml_file', autospec=True)
    def test_load_tron_service_config_empty(self, mock_read_file, mock_read_service_info):
        mock_read_file.return_value = {}
        mock_read_service_info.return_value = {}
        soa_dir = '/other/services'

        with pytest.raises(NoConfigurationForServiceError):
            tron_tools.load_tron_service_config('foo', 'dev', soa_dir=soa_dir)

        assert mock_read_file.call_count == 1
        assert mock_read_service_info.call_count == 1
        mock_read_file.assert_has_calls([mock.call('/other/services/tron/dev/foo.yaml')])
        mock_read_service_info.assert_has_calls([mock.call('foo', 'tron-dev', soa_dir)])

    @mock.patch('paasta_tools.tron_tools.service_configuration_lib.read_extra_service_information', autospec=True)
    @mock.patch('paasta_tools.tron_tools.service_configuration_lib._read_yaml_file', autospec=True)
    def test_load_tron_service_config_jobs_none(self, mock_read_file, mock_read_service_info):
        mock_read_file.return_value = {'jobs': None}
        mock_read_service_info.return_value = None
        soa_dir = '/other/services'

        jc, _ = tron_tools.load_tron_service_config('foo', 'dev', soa_dir=soa_dir)
        assert jc == []

    @mock.patch('paasta_tools.tron_tools.load_system_paasta_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.load_tron_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.load_tron_service_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_tron_job_dict', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_master_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.yaml.dump', autospec=True)
    @pytest.mark.parametrize('service', [MASTER_NAMESPACE, 'my_app'])
    def test_create_complete_config(
        self,
        mock_yaml_dump,
        mock_format_master_config,
        mock_format_job,
        mock_tron_service_config,
        mock_tron_system_config,
        mock_system_config,
        service,
    ):
        job_config = tron_tools.TronJobConfig({})
        other_config = {
            'my_config_value': [1, 2],
        }
        mock_format_master_config.return_value = other_config
        mock_tron_service_config.return_value = (
            [job_config],
            other_config,
        )
        soa_dir = '/testing/services'

        assert tron_tools.create_complete_config(service, soa_dir) == mock_yaml_dump.return_value
        mock_tron_service_config.assert_called_once_with(
            service,
            mock_tron_system_config.return_value.get_cluster_name.return_value,
            True,
            soa_dir,
        )
        if service == MASTER_NAMESPACE:
            mock_format_master_config.assert_called_once_with(
                other_config,
                mock_system_config.return_value.get_volumes.return_value,
                mock_system_config.return_value.get_dockercfg_location.return_value,
            )
        else:
            assert mock_format_master_config.call_count == 0
        mock_format_job.assert_called_once_with(
            job_config,
            mock_system_config.return_value.get_cluster_fqdn_format.return_value,
            mock_tron_system_config.return_value.get_default_paasta_cluster.return_value,
        )
        complete_config = other_config.copy()
        complete_config.update({
            'jobs': [mock_format_job.return_value],
        })
        mock_yaml_dump.assert_called_once_with(
            complete_config,
            Dumper=mock.ANY,
            default_flow_style=mock.ANY,
        )

    @mock.patch('paasta_tools.tron_tools.load_tron_service_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_tron_job_dict', autospec=True)
    @mock.patch('subprocess.run', autospec=True)
    def test_validate_complete_config_paasta_validate_fails(
        self,
        mock_run,
        mock_format_job,
        mock_load_config,
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.validate = mock.Mock(return_value=['some error'])
        mock_load_config.return_value = ([job_config], {})

        result = tron_tools.validate_complete_config(
            'a_service',
            'a-cluster',
        )

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 0
        assert mock_run.call_count == 0
        assert result == ['some error']

    @mock.patch('paasta_tools.tron_tools.load_tron_service_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_tron_job_dict', autospec=True)
    @mock.patch('subprocess.run', autospec=True)
    def test_validate_complete_config_tronfig_fails(
        self,
        mock_run,
        mock_format_job,
        mock_load_config,
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.validate = mock.Mock(return_value=[])
        mock_load_config.return_value = ([job_config], {})
        mock_format_job.return_value = {}
        mock_run.return_value = mock.Mock(
            returncode=1,
            stdout='tronfig error',
            stderr='',
        )

        result = tron_tools.validate_complete_config(
            'a_service',
            'a-cluster',
        )

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 1
        assert mock_run.call_count == 1
        assert result == ['tronfig error']

    @mock.patch('paasta_tools.tron_tools.load_tron_service_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_tron_job_dict', autospec=True)
    @mock.patch('subprocess.run', autospec=True)
    def test_validate_complete_config_passes(
        self,
        mock_run,
        mock_format_job,
        mock_load_config,
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.validate = mock.Mock(return_value=[])
        mock_load_config.return_value = ([job_config], {})
        mock_format_job.return_value = {}
        mock_run.return_value = mock.Mock(
            returncode=0,
            stdout='OK',
            stderr='',
        )

        result = tron_tools.validate_complete_config(
            'a_service',
            'a-cluster',
        )

        assert mock_load_config.call_count == 1
        assert mock_format_job.call_count == 1
        assert mock_run.call_count == 1
        assert not result

    @mock.patch('paasta_tools.tron_tools.load_tron_service_config', autospec=True)
    @mock.patch('paasta_tools.tron_tools.format_tron_job_dict', autospec=True)
    @mock.patch('subprocess.run', autospec=True)
    @pytest.mark.parametrize('namespace,valid', [('MASTER', True), ('bob', False)])
    def test_validate_complete_config_non_job_keys(
        self,
        mock_run,
        mock_format_job,
        mock_load_config,
        namespace,
        valid,
    ):
        job_config = mock.Mock(spec_set=tron_tools.TronJobConfig)
        job_config.validate = mock.Mock(return_value=[])
        mock_load_config.return_value = ([job_config], {'time_zone': 'US/Pacific'})
        mock_format_job.return_value = {}

        result = tron_tools.validate_complete_config(
            namespace,
            'a-cluster',
        )

        assert mock_load_config.call_count == 1
        if not valid:
            assert len(result) == 1
            assert 'time_zone' in result[0]

    @mock.patch('os.walk', autospec=True)
    @mock.patch('os.listdir', autospec=True)
    def test_get_tron_namespaces_for_cluster(self, mock_ls, mock_walk):
        cluster_name = 'stage'
        expected_namespaces = ['app', 'foo', 'cool']
        mock_walk.return_value = [
            ('/my_soa_dir/foo', [], ['tron-stage.yaml']),
            ('/my_soa_dir/app', [], ['tron-stage.yaml']),
            ('my_soa_dir/woo', [], ['something-else.yaml']),
        ]
        mock_ls.return_value = ['cool.yaml']
        soa_dir = '/my_soa_dir'

        namespaces = tron_tools.get_tron_namespaces_for_cluster(
            cluster=cluster_name,
            soa_dir=soa_dir,
        )
        for expected_namespace in expected_namespaces:
            assert expected_namespace in namespaces
        assert len(namespaces) == 3

    @mock.patch('os.walk', autospec=True)
    @mock.patch('os.listdir', autospec=True)
    @mock.patch('paasta_tools.tron_tools.load_tron_config', autospec=True)
    def test_get_tron_namespaces_for_cluster_default(self, mock_system_tron_config, mock_ls, mock_walk):
        mock_system_tron_config.return_value.get_cluster_name.return_value = 'this-cluster'
        mock_walk.return_value = [('/my_soa_dir/this-service', [], ['tron-this-cluster.yaml'])]
        soa_dir = '/my_soa_dir'
        expected_namespaces = ['this-service']

        namespaces = tron_tools.get_tron_namespaces_for_cluster(
            soa_dir=soa_dir,
        )
        assert namespaces == expected_namespaces

    @mock.patch('os.walk', autospec=True)
    @mock.patch('os.listdir', autospec=True)
    def test_get_tron_namespaces_for_cluster_conflict(self, mock_ls, mock_walk):
        cluster_name = 'stage'
        mock_walk.return_value = [
            ('/my_soa_dir/cool', [], ['tron-stage.yaml']),
        ]
        mock_ls.return_value = ['cool.yaml']
        soa_dir = '/my_soa_dir'

        with pytest.raises(tron_tools.ConflictingNamespacesError):
            tron_tools.get_tron_namespaces_for_cluster(
                cluster=cluster_name,
                soa_dir=soa_dir,
            )

    @mock.patch('glob.glob', autospec=True)
    def test_list_tron_clusters(self, mock_glob):
        mock_glob.return_value = [
            '/home/service/tron-dev-cluster2.yaml',
            '/home/service/tron-prod.yaml',
            '/home/service/marathon-other.yaml',
        ]
        result = tron_tools.list_tron_clusters('foo')
        assert sorted(result) == ['dev-cluster2', 'prod']
