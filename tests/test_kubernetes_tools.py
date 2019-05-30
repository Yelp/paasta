import unittest
from typing import Sequence

import mock
import pytest
from hypothesis import given
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from kubernetes.client import V1AWSElasticBlockStoreVolumeSource
from kubernetes.client import V1beta1PodDisruptionBudget
from kubernetes.client import V1Container
from kubernetes.client import V1ContainerPort
from kubernetes.client import V1Deployment
from kubernetes.client import V1DeploymentSpec
from kubernetes.client import V1DeploymentStrategy
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1ExecAction
from kubernetes.client import V1Handler
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1HTTPGetAction
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1Lifecycle
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PersistentVolumeClaim
from kubernetes.client import V1PersistentVolumeClaimSpec
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodTemplateSpec
from kubernetes.client import V1Probe
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1RollingUpdateDeployment
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1StatefulSet
from kubernetes.client import V1StatefulSetSpec
from kubernetes.client import V1TCPSocketAction
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import create_custom_resource
from paasta_tools.kubernetes_tools import create_deployment
from paasta_tools.kubernetes_tools import create_kubernetes_secret_signature
from paasta_tools.kubernetes_tools import create_pod_disruption_budget
from paasta_tools.kubernetes_tools import create_secret
from paasta_tools.kubernetes_tools import create_stateful_set
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import filter_nodes_by_blacklist
from paasta_tools.kubernetes_tools import filter_pods_by_service_instance
from paasta_tools.kubernetes_tools import get_active_shas_for_service
from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_kubernetes_app_by_name
from paasta_tools.kubernetes_tools import get_kubernetes_app_deploy_status
from paasta_tools.kubernetes_tools import get_kubernetes_secret_hashes
from paasta_tools.kubernetes_tools import get_kubernetes_secret_signature
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here_for_nerve
from paasta_tools.kubernetes_tools import get_nodes_grouped_by_attribute
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import is_node_ready
from paasta_tools.kubernetes_tools import is_pod_ready
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeCustomResource
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfigDict
from paasta_tools.kubernetes_tools import KubernetesDeployStatus
from paasta_tools.kubernetes_tools import KubeService
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import list_custom_resources
from paasta_tools.kubernetes_tools import load_kubernetes_service_config
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.kubernetes_tools import max_unavailable
from paasta_tools.kubernetes_tools import maybe_add_yelp_prefix
from paasta_tools.kubernetes_tools import pod_disruption_budget_for_service_instance
from paasta_tools.kubernetes_tools import pods_for_service_instance
from paasta_tools.kubernetes_tools import sanitise_service_name
from paasta_tools.kubernetes_tools import update_custom_resource
from paasta_tools.kubernetes_tools import update_deployment
from paasta_tools.kubernetes_tools import update_kubernetes_secret_signature
from paasta_tools.kubernetes_tools import update_secret
from paasta_tools.kubernetes_tools import update_stateful_set
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.utils import AwsEbsVolume
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import NoConfigurationForServiceError


def test_load_kubernetes_service_config_no_cache():
    with mock.patch(
        'service_configuration_lib.read_service_configuration', autospec=True,
    ) as mock_read_service_configuration, mock.patch(
        'service_configuration_lib.read_extra_service_information', autospec=True,
    ) as mock_read_extra_service_information, mock.patch(
        'paasta_tools.kubernetes_tools.load_v2_deployments_json', autospec=True,
    ) as mock_load_v2_deployments_json, mock.patch(
        'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig', autospec=True,
    ) as mock_kube_deploy_config:
        with pytest.raises(NoConfigurationForServiceError):
            load_kubernetes_service_config_no_cache(
                service='kurupt',
                instance='fm',
                cluster='brentford',
                load_deployments=False,
            )
        with pytest.raises(InvalidJobNameError):
            load_kubernetes_service_config_no_cache(
                service='kurupt',
                instance='_fm',
                cluster='brentford',
                load_deployments=False,
            )

        mock_config = {'freq': '108.9'}
        mock_read_extra_service_information.return_value = {'fm': mock_config}
        mock_read_service_configuration.return_value = {}
        ret = load_kubernetes_service_config_no_cache(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            load_deployments=False,
            soa_dir='/nail/blah',
        )
        mock_kube_deploy_config.assert_called_with(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            config_dict={'freq': '108.9'},
            branch_dict=None,
            soa_dir='/nail/blah',
        )
        assert not mock_load_v2_deployments_json.called
        assert ret == mock_kube_deploy_config.return_value

        mock_kube_deploy_config.reset_mock()
        ret = load_kubernetes_service_config_no_cache(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            load_deployments=True,
            soa_dir='/nail/blah',
        )
        mock_load_v2_deployments_json.assert_called_with(
            service='kurupt',
            soa_dir='/nail/blah',
        )
        mock_kube_deploy_config.assert_called_with(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            config_dict={'freq': '108.9'},
            branch_dict=mock_load_v2_deployments_json.return_value.get_branch_dict(),
            soa_dir='/nail/blah',
        )
        assert ret == mock_kube_deploy_config.return_value


def test_load_kubernetes_service_config():
    with mock.patch(
        'paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache', autospec=True,
    ) as mock_load_kubernetes_service_config_no_cache:
        ret = load_kubernetes_service_config(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            load_deployments=True,
            soa_dir='/nail/blah',
        )
        assert ret == mock_load_kubernetes_service_config_no_cache.return_value


class TestKubernetesDeploymentConfig(unittest.TestCase):
    def setUp(self):
        mock_config_dict = KubernetesDeploymentConfigDict(
            bounce_method='crossover',
            instances=3,
        )
        self.deployment = KubernetesDeploymentConfig(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            config_dict=mock_config_dict,
            branch_dict=None,
            soa_dir='/nail/blah',
        )

    def test_copy(self):
        assert self.deployment.copy() == self.deployment
        assert self.deployment.copy() is not self.deployment

    def test_get_cmd_returns_None(self):
        deployment = KubernetesDeploymentConfig(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            config_dict={'cmd': None},
            branch_dict=None,
            soa_dir='/nail/blah',
        )
        assert deployment.get_cmd() is None

    def test_get_cmd_converts_str_to_list(self):
        deployment = KubernetesDeploymentConfig(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            config_dict={'cmd': "/bin/echo hi"},
            branch_dict=None,
            soa_dir='/nail/blah',
        )
        assert deployment.get_cmd() == ['sh', '-c', '/bin/echo hi']

    def test_get_cmd_list(self):
        deployment = KubernetesDeploymentConfig(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            config_dict={'cmd': ['/bin/echo', 'hi']},
            branch_dict=None,
            soa_dir='/nail/blah',
        )
        assert deployment.get_cmd() == ['/bin/echo', 'hi']

    def test_get_bounce_method(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_aws_ebs_volumes', autospec=True,
        ) as mock_get_aws_ebs_volumes:
            mock_get_aws_ebs_volumes.return_value = []
            assert self.deployment.get_bounce_method() == 'RollingUpdate'
            self.deployment.config_dict['bounce_method'] = 'downthenup'
            assert self.deployment.get_bounce_method() == 'Recreate'
            self.deployment.config_dict['bounce_method'] = 'crossover'
            # if ebs we must downthenup for now as we need to free up the EBS for the new instance
            mock_get_aws_ebs_volumes.return_value = ['some-ebs']
            with pytest.raises(Exception):
                self.deployment.get_bounce_method()

    def test_get_deployment_strategy(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_bounce_method', autospec=True,
            return_value='RollingUpdate',
        ) as mock_get_bounce_method:
            assert self.deployment.get_deployment_strategy_config() == V1DeploymentStrategy(
                type='RollingUpdate',
                rolling_update=V1RollingUpdateDeployment(
                    max_surge='100%',
                    max_unavailable='0%',
                ),
            )
            mock_get_bounce_method.return_value = 'Recreate'
            assert self.deployment.get_deployment_strategy_config() == V1DeploymentStrategy(
                type='Recreate',
            )

    def test_get_sanitised_volume_name(self):
        self.deployment.get_sanitised_volume_name('/var/tmp') == 'slash-varslash-tmp'
        self.deployment.get_sanitised_volume_name('/var/tmp/') == 'slash-varslash-tmp'

    def test_get_sidecar_containers(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_registrations', autospec=True,
            return_value=['universal.credit'],
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_environment', autospec=True,
            return_value={},
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name', autospec=True,
            return_value='sane-name',
        ):
            mock_system_config = mock.Mock(
                get_enable_nerve_readiness_check=mock.Mock(return_value=False),
                get_nerve_readiness_check_script=mock.Mock(return_value='/nail/blah.sh'),
                get_hacheck_sidecar_image_url=mock.Mock(return_value='some-docker-image'),
            )
            mock_service_namespace = mock.Mock(is_in_smartstack=mock.Mock(return_value=False))
            assert self.deployment.get_sidecar_containers(mock_system_config, mock_service_namespace) == []

            mock_service_namespace = mock.Mock(is_in_smartstack=mock.Mock(return_value=True))
            ret = self.deployment.get_sidecar_containers(mock_system_config, mock_service_namespace)
            expected = [
                V1Container(
                    env={},
                    image='some-docker-image',
                    lifecycle=V1Lifecycle(
                        pre_stop=V1Handler(
                            _exec=V1ExecAction(
                                command=[
                                    '/bin/sh',
                                    '-c',
                                    '/usr/bin/hadown '
                                    'universal.credit; sleep '
                                    '31',
                                ],
                            ),
                        ),
                    ),
                    name='hacheck',
                    ports=[V1ContainerPort(container_port=6666)],
                ),
            ]
            assert ret == expected

            mock_system_config = mock.Mock(
                get_enable_nerve_readiness_check=mock.Mock(return_value=True),
                get_nerve_readiness_check_script=mock.Mock(return_value='/nail/blah.sh'),
                get_hacheck_sidecar_image_url=mock.Mock(return_value='some-docker-image'),
            )
            ret = self.deployment.get_sidecar_containers(mock_system_config, mock_service_namespace)
            expected = [
                V1Container(
                    env={},
                    image='some-docker-image',
                    lifecycle=V1Lifecycle(
                        pre_stop=V1Handler(
                            _exec=V1ExecAction(
                                command=[
                                    '/bin/sh',
                                    '-c',
                                    '/usr/bin/hadown '
                                    'universal.credit; sleep '
                                    '31',
                                ],
                            ),
                        ),
                    ),
                    name='hacheck',
                    ports=[V1ContainerPort(container_port=6666)],
                    readiness_probe=V1Probe(
                        _exec=V1ExecAction(
                            command=['/nail/blah.sh', '8888', 'universal.credit'],
                        ),
                        initial_delay_seconds=10,
                        period_seconds=10,
                    ),
                ),
            ]
            assert ret == expected

    def test_get_container_env(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_env', autospec=True,
            return_value={
                'mc': 'grindah',
                'dj': 'beats',
                'A': 'SECRET(123)',
                'B': 'SHAREDSECRET(456)',
            },
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_environment', autospec=True,
            return_value=[
                V1EnvVar(
                    name='manager',
                    value='chabuddy',
                ),
            ],
        ), mock.patch(
            'paasta_tools.kubernetes_tools.is_secret_ref', autospec=True,
        ) as mock_is_secret_ref, mock.patch(
            'paasta_tools.kubernetes_tools.is_shared_secret', autospec=True,
        ) as mock_is_shared_secret, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_secret_env_vars', autospec=True,
            return_value=[],
        ) as mock_get_kubernetes_secret_env_vars:
            mock_is_secret_ref.side_effect = lambda x: True if 'SECRET' in x else False
            mock_is_shared_secret.side_effect = lambda x: False if not x.startswith("SHARED") else True
            expected = [
                V1EnvVar(name='mc', value='grindah'),
                V1EnvVar(name='dj', value='beats'),
                V1EnvVar(name='manager', value='chabuddy'),
            ]
            assert expected == self.deployment.get_container_env()
            mock_get_kubernetes_secret_env_vars.assert_called_with(
                self.deployment,
                secret_env_vars={'A': 'SECRET(123)'},
                shared_secret_env_vars={'B': 'SHAREDSECRET(456)'},
            )

    def test_get_kubernetes_environment(self):
        ret = self.deployment.get_kubernetes_environment()
        assert 'PAASTA_POD_IP' in [env.name for env in ret]
        assert 'POD_NAME' in [env.name for env in ret]

    def test_get_resource_requirements(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cpus', autospec=True,
            return_value=0.3,
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cpu_burst_add', autospec=True,
            return_value=1,
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_mem', autospec=True,
            return_value=2048,
        ):
            assert self.deployment.get_resource_requirements() == V1ResourceRequirements(
                limits={
                    'cpu': 1.3,
                    'memory': '2048Mi',
                },
                requests={
                    'cpu': 0.3,
                    'memory': '2048Mi',
                },
            )

    def test_get_kubernetes_containers(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_docker_url', autospec=True,
        ) as mock_get_docker_url, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cmd', autospec=True,
        ) as mock_get_cmd, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_args', autospec=True,
        ) as mock_get_args, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_resource_requirements', autospec=True,
        ) as mock_get_resource_requirements, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_container_env', autospec=True,
        ) as mock_get_container_env, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_service_name', autospec=True,
            return_value='kurupt',
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_instance_name', autospec=True,
            return_value='fm',
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volume_mounts', autospec=True,
        ) as mock_get_volume_mounts, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sidecar_containers', autospec=True,
            return_value=['mock_sidecar'],
        ):
            mock_system_config = mock.Mock()
            mock_docker_volumes: Sequence[DockerVolume] = []
            mock_aws_ebs_volumes: Sequence[AwsEbsVolume] = []
            expected = [
                V1Container(
                    args=mock_get_args.return_value,
                    command=mock_get_cmd.return_value,
                    env=mock_get_container_env.return_value,
                    resources=mock_get_resource_requirements.return_value,
                    image=mock_get_docker_url.return_value,
                    lifecycle=V1Lifecycle(
                        pre_stop=V1Handler(
                            _exec=V1ExecAction(
                                command=[
                                    '/bin/sh',
                                    '-c',
                                    'sleep 30',
                                ],
                            ),
                        ),
                    ),
                    liveness_probe=V1Probe(
                        failure_threshold=30,
                        http_get=V1HTTPGetAction(
                            path='/status',
                            port=8888,
                            scheme='HTTP',
                        ),
                        initial_delay_seconds=60,
                        period_seconds=10,
                        timeout_seconds=10,
                    ),
                    name='kurupt-fm',
                    ports=[V1ContainerPort(container_port=8888)],
                    volume_mounts=mock_get_volume_mounts.return_value,
                ), 'mock_sidecar',
            ]
            service_namespace_config = mock.Mock()
            service_namespace_config.get_healthcheck_mode.return_value = 'http'
            service_namespace_config.get_healthcheck_uri.return_value = '/status'
            assert self.deployment.get_kubernetes_containers(
                docker_volumes=mock_docker_volumes,
                system_paasta_config=mock_system_config,
                aws_ebs_volumes=mock_aws_ebs_volumes,
                service_namespace_config=service_namespace_config,
            ) == expected

    def test_get_liveness_probe(self):
        liveness_probe = V1Probe(
            failure_threshold=30,
            http_get=V1HTTPGetAction(
                path='/status',
                port=8888,
                scheme='HTTP',
            ),
            initial_delay_seconds=60,
            period_seconds=10,
            timeout_seconds=10,
        )

        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = 'http'
        service_namespace_config.get_healthcheck_uri.return_value = '/status'

        assert self.deployment.get_liveness_probe(service_namespace_config) == liveness_probe

    def test_get_liveness_probe_non_smartstack(self):
        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = None
        assert self.deployment.get_liveness_probe(service_namespace_config) is None

    def test_get_liveness_probe_numbers(self):
        liveness_probe = V1Probe(
            failure_threshold=1,
            http_get=V1HTTPGetAction(
                path='/status',
                port=8888,
                scheme='HTTP',
            ),
            initial_delay_seconds=2,
            period_seconds=3,
            timeout_seconds=4,
        )

        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = 'http'
        service_namespace_config.get_healthcheck_uri.return_value = '/status'

        self.deployment.config_dict['healthcheck_max_consecutive_failures'] = 1
        self.deployment.config_dict['healthcheck_grace_period_seconds'] = 2
        self.deployment.config_dict['healthcheck_interval_seconds'] = 3
        self.deployment.config_dict['healthcheck_timeout_seconds'] = 4

        assert self.deployment.get_liveness_probe(service_namespace_config) == liveness_probe

    def test_get_liveness_probe_tcp_socket(self):
        liveness_probe = V1Probe(
            failure_threshold=30,
            tcp_socket=V1TCPSocketAction(
                port=8888,
            ),
            initial_delay_seconds=60,
            period_seconds=10,
            timeout_seconds=10,
        )
        mock_service_namespace_config = mock.Mock()
        mock_service_namespace_config.get_healthcheck_mode.return_value = 'tcp'
        assert self.deployment.get_liveness_probe(mock_service_namespace_config) == liveness_probe

    def test_get_liveness_probe_cmd(self):
        liveness_probe = V1Probe(
            failure_threshold=30,
            _exec=V1ExecAction(
                command=[
                    '/bin/sh',
                    '-c',
                    '/bin/true',
                ],
            ),
            initial_delay_seconds=60,
            period_seconds=10,
            timeout_seconds=10,
        )
        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = 'cmd'
        self.deployment.config_dict['healthcheck_cmd'] = '/bin/true'
        assert self.deployment.get_liveness_probe(service_namespace_config) == liveness_probe

    def test_get_pod_volumes(self):
        mock_docker_volumes = [
            {'hostPath': '/nail/blah', 'containerPath': '/nail/foo'},
            {'hostPath': '/nail/thing', 'containerPath': '/nail/bar'},
        ]
        mock_aws_ebs_volumes = [
            {'volume_id': 'vol-ZZZZZZZZZZZZZZZZZ', 'fs_type': 'ext4', 'container_path': '/nail/qux'},
        ]
        expected_volumes = [
            V1Volume(
                host_path=V1HostPathVolumeSource(
                    path='/nail/blah',
                ),
                name='host--slash-nailslash-blah',
            ),
            V1Volume(
                host_path=V1HostPathVolumeSource(
                    path='/nail/thing',
                ),
                name='host--slash-nailslash-thing',
            ),
            V1Volume(
                aws_elastic_block_store=V1AWSElasticBlockStoreVolumeSource(
                    volume_id='vol-ZZZZZZZZZZZZZZZZZ',
                    fs_type='ext4',
                    read_only=False,
                ),
                name='aws-ebs--vol-ZZZZZZZZZZZZZZZZZ',
            ),
        ]
        assert self.deployment.get_pod_volumes(
            docker_volumes=mock_docker_volumes,
            aws_ebs_volumes=mock_aws_ebs_volumes,
        ) == expected_volumes

    def test_get_volume_mounts(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name', autospec=True,
            return_value='some-volume',
        ):
            mock_docker_volumes = [
                {'hostPath': '/nail/blah', 'containerPath': '/nail/foo'},
                {'hostPath': '/nail/thing', 'containerPath': '/nail/bar', 'mode': 'RW'},
            ]
            mock_aws_ebs_volumes = [
                {'volume_id': 'vol-ZZZZZZZZZZZZZZZZZ', 'fs_type': 'ext4', 'container_path': '/nail/qux'},
            ]
            mock_persistent_volumes = [
                {'container_path': '/blah', 'mode': 'RW'},
            ]
            expected_volumes = [
                V1VolumeMount(
                    mount_path='/nail/foo',
                    name='some-volume',
                    read_only=True,
                ),
                V1VolumeMount(
                    mount_path='/nail/bar',
                    name='some-volume',
                    read_only=False,
                ),
                V1VolumeMount(
                    mount_path='/nail/qux',
                    name='some-volume',
                    read_only=True,
                ),
                V1VolumeMount(
                    mount_path='/blah',
                    name='some-volume',
                    read_only=False,
                ),
            ]
            assert self.deployment.get_volume_mounts(
                docker_volumes=mock_docker_volumes,
                aws_ebs_volumes=mock_aws_ebs_volumes,
                persistent_volumes=mock_persistent_volumes,
            ) == expected_volumes

    def test_get_sanitised_service_name(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service', autospec=True,
            return_value='my_service',
        ):
            assert self.deployment.get_sanitised_service_name() == 'my--service'

    def test_get_sanitised_instance_name(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance', autospec=True,
            return_value='my_instance',
        ):
            assert self.deployment.get_sanitised_instance_name() == 'my--instance'

    def test_get_desired_instances(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_aws_ebs_volumes', autospec=True,
        ) as mock_get_aws_ebs_volumes:
            mock_get_aws_ebs_volumes.return_value = []
            assert self.deployment.get_desired_instances() == 3

            mock_get_aws_ebs_volumes.return_value = ['some-ebs-vol']
            with pytest.raises(Exception):
                self.deployment.get_desired_instances()

    def test_format_kubernetes_app_dict(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.load_system_paasta_config', autospec=True,
        ) as mock_load_system_config, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_docker_url', autospec=True,
        ) as mock_get_docker_url, mock.patch(
            'paasta_tools.kubernetes_tools.get_code_sha_from_dockerurl', autospec=True,
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_service_name', autospec=True,
            return_value='kurupt',
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_instance_name', autospec=True,
            return_value='fm',
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service', autospec=True,
        ) as mock_get_service, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance', autospec=True,
        ) as mock_get_instance, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_desired_instances', autospec=True,
        ) as mock_get_instances, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_deployment_strategy_config', autospec=True,
        ) as mock_get_deployment_strategy_config, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name', autospec=True,
        ), mock.patch(
            'paasta_tools.kubernetes_tools.get_config_hash', autospec=True,
        ) as mock_get_config_hash, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_force_bounce', autospec=True,
        ) as mock_get_force_bounce, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.sanitize_for_config_hash', autospec=True,
        ) as mock_sanitize_for_config_hash, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_persistent_volumes', autospec=True,
        ) as mock_get_persistent_volumes, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volume_claim_templates', autospec=True,
        ) as mock_get_volumes_claim_templates, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_template_spec', autospec=True,
        ) as mock_get_pod_template_spec, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_metadata', autospec=True,
        ) as mock_get_kubernetes_metadata:
            mock_get_persistent_volumes.return_value = []
            ret = self.deployment.format_kubernetes_app()
            assert mock_load_system_config.called
            assert mock_get_docker_url.called
            mock_get_config_hash.assert_called_with(
                mock_sanitize_for_config_hash.return_value,
                force_bounce=mock_get_force_bounce.return_value,
            )
            expected = V1Deployment(
                api_version='apps/v1',
                kind='Deployment',
                metadata=mock_get_kubernetes_metadata.return_value,
                spec=V1DeploymentSpec(
                    replicas=mock_get_instances.return_value,
                    selector=V1LabelSelector(
                        match_labels={
                            'yelp.com/paasta_instance': mock_get_instance.return_value,
                            'yelp.com/paasta_service': mock_get_service.return_value,
                        },
                    ),
                    strategy=mock_get_deployment_strategy_config.return_value,
                    template=mock_get_pod_template_spec.return_value,
                ),
            )
            assert ret == expected
            ret.metadata.labels.__setitem__.assert_called_with(
                'yelp.com/paasta_config_sha', mock_get_config_hash.return_value,
            )
            ret.spec.template.metadata.labels.__setitem__.assert_called_with(
                'yelp.com/paasta_config_sha',
                mock_get_config_hash.return_value,
            )

            mock_get_deployment_strategy_config.side_effect = Exception("Bad bounce method")
            with pytest.raises(InvalidKubernetesConfig):
                self.deployment.format_kubernetes_app()

            mock_get_persistent_volumes.return_value = [mock.Mock()]
            ret = self.deployment.format_kubernetes_app()
            expected = V1StatefulSet(
                api_version='apps/v1',
                kind='StatefulSet',
                metadata=mock_get_kubernetes_metadata.return_value,
                spec=V1StatefulSetSpec(
                    service_name='kurupt-fm',
                    replicas=mock_get_instances.return_value,
                    selector=V1LabelSelector(
                        match_labels={
                            'yelp.com/paasta_instance': mock_get_instance.return_value,
                            'yelp.com/paasta_service': mock_get_service.return_value,
                        },
                    ),
                    template=mock_get_pod_template_spec.return_value,
                    volume_claim_templates=mock_get_volumes_claim_templates.return_value,
                ),
            )
            assert ret == expected
            ret.metadata.labels.__setitem__.assert_called_with(
                'yelp.com/paasta_config_sha', mock_get_config_hash.return_value,
            )
            ret.spec.template.metadata.labels.__setitem__.assert_called_with(
                'yelp.com/paasta_config_sha',
                mock_get_config_hash.return_value,
            )

    def test_get_pod_template_spec(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volumes', autospec=True,
        ) as mock_get_volumes, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service', autospec=True,
        ) as mock_get_service, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance', autospec=True,
        ) as mock_get_instance, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_containers', autospec=True,
        ) as mock_get_kubernetes_containers, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_volumes', autospec=True,
            return_value=[],
        ) as mock_get_pod_volumes:
            ret = self.deployment.get_pod_template_spec(code_sha='aaaa123', system_paasta_config=mock.Mock())
            assert mock_get_pod_volumes.called
            assert mock_get_volumes.called
            assert ret == V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    labels={
                        'yelp.com/paasta_git_sha': 'aaaa123',
                        'yelp.com/paasta_instance': mock_get_instance.return_value,
                        'yelp.com/paasta_service': mock_get_service.return_value,
                    },
                    annotations={
                        'smartstack_registrations': '["kurupt.fm"]',
                    },
                ),
                spec=V1PodSpec(
                    service_account_name=None,
                    containers=mock_get_kubernetes_containers.return_value,
                    restart_policy='Always',
                    volumes=[],
                    dns_policy='Default',
                ),
            )

    def test_get_kubernetes_metadata(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service', autospec=True,
            return_value='kurupt',
        ) as mock_get_service, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance', autospec=True, return_value='fm',
        ) as mock_get_instance:

            ret = self.deployment.get_kubernetes_metadata('aaa123')
            assert ret == V1ObjectMeta(
                labels={
                    'yelp.com/paasta_git_sha': 'aaa123',
                    'yelp.com/paasta_instance': mock_get_instance.return_value,
                    'yelp.com/paasta_service': mock_get_service.return_value,
                },
                name='kurupt-fm',
            )

    def test_sanitize_for_config_hash(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.get_kubernetes_secret_hashes', autospec=True,
        ) as mock_get_kubernetes_secret_hashes:
            mock_config = V1Deployment(
                metadata=V1ObjectMeta(
                    name='qwe',
                    labels={
                        'mc': 'grindah',
                    },
                ),
                spec=V1DeploymentSpec(
                    replicas=2,
                    selector=V1LabelSelector(
                        match_labels={
                            'freq': '108.9',
                        },
                    ),
                    template=V1PodTemplateSpec(),
                ),
            )
            ret = self.deployment.sanitize_for_config_hash(mock_config)
            assert 'replicas' not in ret['spec'].keys()
            assert ret['paasta_secrets'] == mock_get_kubernetes_secret_hashes.return_value

    def test_get_kubernetes_secret_env_vars(self):
        assert self.deployment.get_kubernetes_secret_env_vars(
            secret_env_vars={'SOME': 'SECRET(ref)'},
            shared_secret_env_vars={'A': 'SHAREDSECRET(ref1)'},
        ) == [
            V1EnvVar(
                name='SOME',
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name='paasta-secret-kurupt-ref',
                        key='ref',
                        optional=False,
                    ),
                ),
            ),
            V1EnvVar(
                name='A',
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name='paasta-secret---shared-ref1',
                        key='ref1',
                        optional=False,
                    ),
                ),
            ),
        ]

    def test_get_bounce_margin_factor(self):
        assert isinstance(self.deployment.get_bounce_margin_factor(), float)

    def test_get_volume_claim_templates(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_persistent_volumes', autospec=True,
        ) as mock_get_persistent_volumes, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_persistent_volume_name', autospec=True,
        ) as mock_get_persistent_volume_name, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_storage_class_name', autospec=True,
        ) as mock_get_storage_class_name:
            mock_get_persistent_volumes.return_value = [
                {'size': 20},
                {'size': 10},
            ]
            expected = [
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name=mock_get_persistent_volume_name.return_value,
                    ),
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        storage_class_name=mock_get_storage_class_name.return_value,
                        resources=V1ResourceRequirements(
                            requests={
                                'storage': '10Gi',
                            },
                        ),
                    ),
                ),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name=mock_get_persistent_volume_name.return_value,
                    ),
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        storage_class_name=mock_get_storage_class_name.return_value,
                        resources=V1ResourceRequirements(
                            requests={
                                'storage': '20Gi',
                            },
                        ),
                    ),
                ),
            ]
            ret = self.deployment.get_volume_claim_templates()
            assert expected[0] in ret
            assert expected[1] in ret
            assert len(ret) == 2

    def test_get_storage_class_name(self):
        assert isinstance(self.deployment.get_storage_class_name(), str)

    def test_get_persistent_volume_name(self):
        assert self.deployment.get_persistent_volume_name(
            {'container_path': '/blah/what'},
        ) == 'pv--slash-blahslash-what'


def test_get_kubernetes_services_running_here():
    with mock.patch(
        'paasta_tools.kubernetes_tools.requests.get', autospec=True,
    ) as mock_requests_get:
        mock_requests_get.return_value.json.return_value = {'items': []}
        assert get_kubernetes_services_running_here() == []

        spec = {
            'containers': [
                {
                    'name': 'something-something',
                    'ports': [
                        {
                            'containerPort': 8888,
                        },
                    ],
                },
            ],
        }

        mock_requests_get.return_value.json.return_value = {'items': [
            {
                'status': {
                    'phase': 'Running',
                    'podIP': '10.1.1.1',
                },
                'metadata': {
                    'namespace': 'paasta',
                    'labels': {
                        'yelp.com/paasta_service': 'kurupt',
                        'yelp.com/paasta_instance': 'fm',
                    },
                    'annotations': {
                        'smartstack_registrations': "[]",
                    },
                },
                'spec': spec,
            }, {
                'status': {
                    'phase': 'Something',
                    'podIP': '10.1.1.1',
                },
                'metadata': {
                    'namespace': 'paasta',
                    'labels': {
                        'yelp.com/paasta_service': 'kurupt',
                        'yelp.com/paasta_instance': 'garage',
                    },
                    'annotations': {
                        'smartstack_registrations': "[]",
                    },
                },
                'spec': spec,
            }, {
                'status': {
                    'phase': 'Running',
                },
                'metadata': {
                    'namespace': 'paasta',
                    'labels': {
                        'yelp.com/paasta_service': 'kurupt',
                        'yelp.com/paasta_instance': 'grindah',
                    },
                    'annotations': {
                        'smartstack_registrations': "[]",
                    },
                },
                'spec': spec,
            }, {
                'status': {
                    'phase': 'Running',
                    'podIP': '10.1.1.1',
                },
                'metadata': {
                    'namespace': 'paasta',
                    'labels': {
                        'yelp.com/paasta_service': 'kurupt',
                        'yelp.com/paasta_instance': 'beats',
                    },
                    'annotations': {},
                },
                'spec': spec,
            },
        ]}
        assert get_kubernetes_services_running_here() == [
            KubeService(
                name='kurupt',
                instance='fm',
                port=8888,
                pod_ip='10.1.1.1',
                registrations=[],
            ),
        ]


class MockNerveDict(dict):
    def is_in_smartstack(self):
        return False if self['name'] == 'garage' else True


def test_get_kubernetes_services_running_here_for_nerve():
    with mock.patch(
        'paasta_tools.kubernetes_tools.load_system_paasta_config', autospec=True,
    ) as mock_load_system_config, mock.patch(
        'paasta_tools.kubernetes_tools.get_kubernetes_services_running_here', autospec=True,
    ) as mock_get_kubernetes_services_running_here, mock.patch(
        'paasta_tools.kubernetes_tools.load_service_namespace_config', autospec=True,
    ) as mock_load_service_namespace:

        mock_load_service_namespace.side_effect = lambda service, namespace, soa_dir: MockNerveDict(name=namespace)
        mock_get_kubernetes_services_running_here.return_value = [
            KubeService(
                name='kurupt',
                instance='fm',
                port=8888,
                pod_ip='10.1.1.1',
                registrations=['kurupt.fm'],
            ),
            KubeService(
                name='unkurupt',
                instance='garage',
                port=8888,
                pod_ip='10.1.1.1',
                registrations=['unkurupt.garage'],
            ),
            KubeService(
                name='kurupt',
                instance='garage',
                port=8888,
                pod_ip='10.1.1.1',
                registrations=[],
            ),
        ]

        mock_load_system_config.side_effect = None
        mock_load_system_config.return_value = mock.Mock(
            get_cluster=mock.Mock(return_value='brentford'),
            get_register_k8s_pods=mock.Mock(return_value=False),
        )
        ret = get_kubernetes_services_running_here_for_nerve('brentford', '/nail/blah')
        assert ret == []

        mock_load_system_config.return_value = mock.Mock(
            get_cluster=mock.Mock(return_value='brentford'),
            get_register_k8s_pods=mock.Mock(return_value=True),
        )
        ret = get_kubernetes_services_running_here_for_nerve('brentford', '/nail/blah')
        assert ret == [(
            'kurupt.fm', {
                'name': 'fm',
                'hacheck_ip': '10.1.1.1',
                'service_ip': '10.1.1.1',
                'port': 8888,
            },
        )]

        mock_load_system_config.return_value = mock.Mock(
            get_cluster=mock.Mock(return_value='brentford'),
            get_register_k8s_pods=mock.Mock(return_value=True),
            get_kubernetes_use_hacheck_sidecar=mock.Mock(return_value=False),
        )
        ret = get_kubernetes_services_running_here_for_nerve('brentford', '/nail/blah')
        assert ret == [(
            'kurupt.fm', {
                'name': 'fm',
                'service_ip': '10.1.1.1',
                'port': 8888,
            },
        )]

        def mock_load_namespace_side(service, namespace, soa_dir):
            if namespace != 'kurupt':
                raise Exception
            return MockNerveDict(name=namespace)
        mock_load_service_namespace.side_effect = mock_load_namespace_side
        ret = get_kubernetes_services_running_here_for_nerve('brentford', '/nail/blah')
        assert ret == []


def test_KubeClient():
    with mock.patch(
        'paasta_tools.kubernetes_tools.kube_config.load_kube_config', autospec=True,
    ), mock.patch(
        'paasta_tools.kubernetes_tools.kube_client', autospec=True,
    ) as mock_kube_client:
        client = KubeClient()
        assert client.deployments == mock_kube_client.AppsV1Api()
        assert client.core == mock_kube_client.CoreV1Api()


def test_ensure_namespace():
    mock_metadata = mock.Mock()
    type(mock_metadata).name = 'paasta'
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)))
    ensure_namespace(mock_client, namespace='paasta')
    assert not mock_client.core.create_namespace.called

    mock_metadata = mock.Mock()
    type(mock_metadata).name = 'kube-system'
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)))
    ensure_namespace(mock_client, namespace='paasta')
    assert mock_client.core.create_namespace.called

    mock_client.core.create_namespace.reset_mock()
    mock_namespaces = mock.Mock(items=[])
    mock_client = mock.Mock(core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)))
    ensure_namespace(mock_client, namespace='paasta')
    assert mock_client.core.create_namespace.called


def test_list_all_deployments():
    mock_deployments = mock.Mock(items=[])
    mock_stateful_sets = mock.Mock(items=[])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_namespaced_deployment=mock.Mock(return_value=mock_deployments),
            list_namespaced_stateful_set=mock.Mock(return_value=mock_stateful_sets),
        ),
    )
    assert list_all_deployments(mock_client) == []

    mock_items = [
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    'yelp.com/paasta_service': 'kurupt',
                    'yelp.com/paasta_instance': 'fm',
                    'yelp.com/paasta_git_sha': 'a12345',
                    'yelp.com/paasta_config_sha': 'b12345',
                },
            ),
        ),
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    'yelp.com/paasta_service': 'kurupt',
                    'yelp.com/paasta_instance': 'am',
                    'yelp.com/paasta_git_sha': 'a12345',
                    'yelp.com/paasta_config_sha': 'b12345',
                },
            ),
        ),
    ]
    type(mock_items[0]).spec = mock.Mock(replicas=3)
    type(mock_items[1]).spec = mock.Mock(replicas=3)
    mock_deployments = mock.Mock(items=[mock_items[0]])
    mock_stateful_sets = mock.Mock(items=[mock_items[1]])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_namespaced_deployment=mock.Mock(return_value=mock_deployments),
            list_namespaced_stateful_set=mock.Mock(return_value=mock_stateful_sets),
        ),
    )
    assert list_all_deployments(mock_client) == [
        KubeDeployment(
            service='kurupt',
            instance='fm',
            git_sha='a12345',
            config_sha='b12345',
            replicas=3,
        ), KubeDeployment(
            service='kurupt',
            instance='am',
            git_sha='a12345',
            config_sha='b12345',
            replicas=3,
        ),
    ]


@given(integers(min_value=0), floats(min_value=0, max_value=1.0))
def test_max_unavailable(instances, bmf):
    res = max_unavailable(instances, bmf)
    if instances == 0:
        assert res == 0
    if instances > 0:
        assert res >= 1 and res <= instances
    assert type(res) is int


def test_pod_disruption_budget_for_service_instance():
    x = pod_disruption_budget_for_service_instance(
        service='foo',
        instance='bar',
        min_instances=10,
    )

    assert x.metadata.name == "foo-bar"
    assert x.metadata.namespace == "paasta"
    assert x.spec.min_available == 10
    assert x.spec.selector.match_labels == {
        'yelp.com/paasta_service': 'foo',
        'yelp.com/paasta_instance': 'bar',
    }


def test_create_pod_disruption_budget():
    mock_client = mock.Mock()
    mock_pdr = V1beta1PodDisruptionBudget()
    create_pod_disruption_budget(mock_client, mock_pdr)
    mock_client.policy.create_namespaced_pod_disruption_budget.assert_called_with(
        namespace='paasta',
        body=mock_pdr,
    )


def test_create_deployment():
    mock_client = mock.Mock()
    create_deployment(mock_client, V1Deployment(api_version='some'))
    mock_client.deployments.create_namespaced_deployment.assert_called_with(
        namespace='paasta',
        body=V1Deployment(api_version='some'),
    )


def test_update_deployment():
    mock_client = mock.Mock()
    update_deployment(mock_client, V1Deployment(metadata=V1ObjectMeta(name='kurupt')))
    mock_client.deployments.replace_namespaced_deployment.assert_called_with(
        namespace='paasta',
        name='kurupt',
        body=V1Deployment(metadata=V1ObjectMeta(name='kurupt')),
    )

    mock_client = mock.Mock()
    create_deployment(mock_client, V1Deployment(api_version='some'))
    mock_client.deployments.create_namespaced_deployment.assert_called_with(
        namespace='paasta',
        body=V1Deployment(api_version='some'),
    )


def test_create_custom_resource():
    mock_client = mock.Mock()
    formatted_resource = mock.Mock()
    create_custom_resource(
        kube_client=mock_client,
        formatted_resource=formatted_resource,
        version='v1',
        kind=mock.Mock(plural='someclusters'),
        group='yelp.com',
    )
    mock_client.custom.create_namespaced_custom_object.assert_called_with(
        namespace='paasta-someclusters',
        body=formatted_resource,
        version='v1',
        plural='someclusters',
        group='yelp.com',
    )


def test_update_custom_resource():
    mock_get_object = mock.Mock(return_value={'metadata': {'resourceVersion': 2}})
    mock_client = mock.Mock(custom=mock.Mock(get_namespaced_custom_object=mock_get_object))
    mock_formatted_resource = {'metadata': {}}
    update_custom_resource(
        kube_client=mock_client,
        formatted_resource=mock_formatted_resource,
        version='v1',
        kind=mock.Mock(plural='someclusters'),
        name='grindah',
        group='yelp.com',
    )
    mock_client.custom.replace_namespaced_custom_object.assert_called_with(
        namespace='paasta-someclusters',
        group='yelp.com',
        name='grindah',
        version='v1',
        plural='someclusters',
        body={'metadata': {'resourceVersion': 2}},
    )


def test_list_custom_resources():
    mock_list_object = mock.Mock(return_value={
        'items': [
            {'some': 'nonpaasta'},
            {
                'kind': 'somecluster',
                'metadata': {
                    'labels': {
                        'yelp.com/paasta_service': 'kurupt',
                        'yelp.com/paasta_instance': 'fm',
                        'yelp.com/paasta_config_sha': 'con123',
                    },
                },
            },
        ],
    })

    mock_client = mock.Mock(custom=mock.Mock(list_namespaced_custom_object=mock_list_object))
    expected = [KubeCustomResource(
        service='kurupt',
        instance='fm',
        config_sha='con123',
        kind='somecluster',
    )]
    assert list_custom_resources(
        kind=mock.Mock(plural='someclusters'),
        version='v1',
        kube_client=mock_client,
        group='yelp.com',
    ) == expected


def test_create_stateful_set():
    mock_client = mock.Mock()
    create_stateful_set(mock_client, V1StatefulSet(api_version='some'))
    mock_client.deployments.create_namespaced_stateful_set.assert_called_with(
        namespace='paasta',
        body=V1StatefulSet(api_version='some'),
    )


def test_update_stateful_set():
    mock_client = mock.Mock()
    update_stateful_set(mock_client, V1StatefulSet(metadata=V1ObjectMeta(name='kurupt')))
    mock_client.deployments.replace_namespaced_stateful_set.assert_called_with(
        namespace='paasta',
        name='kurupt',
        body=V1StatefulSet(metadata=V1ObjectMeta(name='kurupt')),
    )

    mock_client = mock.Mock()
    create_stateful_set(mock_client, V1StatefulSet(api_version='some'))
    mock_client.deployments.create_namespaced_stateful_set.assert_called_with(
        namespace='paasta',
        body=V1StatefulSet(api_version='some'),
    )


def test_get_kubernetes_app_deploy_status():
    mock_status = mock.Mock(
        replicas=1,
        ready_replicas=1,
        updated_replicas=1,
    )
    mock_app = mock.Mock(status=mock_status)
    mock_client = mock.Mock()
    assert get_kubernetes_app_deploy_status(
        mock_client,
        mock_app,
        desired_instances=1,
    ) == KubernetesDeployStatus.Running

    assert get_kubernetes_app_deploy_status(
        mock_client,
        mock_app,
        desired_instances=2,
    ) == KubernetesDeployStatus.Waiting

    mock_status = mock.Mock(
        replicas=1,
        ready_replicas=2,
        updated_replicas=1,
    )
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(
        mock_client,
        mock_app,
        desired_instances=2,
    ) == KubernetesDeployStatus.Deploying

    mock_status = mock.Mock(
        replicas=0,
        ready_replicas=0,
        updated_replicas=0,
    )
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(
        mock_client,
        mock_app,
        desired_instances=0,
    ) == KubernetesDeployStatus.Stopped

    mock_status = mock.Mock(
        replicas=1,
        ready_replicas=None,
        updated_replicas=None,
    )
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(
        mock_client,
        mock_app,
        desired_instances=1,
    ) == KubernetesDeployStatus.Waiting


def test_get_kubernetes_app_by_name():
    mock_client = mock.Mock()
    mock_deployment = mock.Mock()
    mock_client.deployments.read_namespaced_deployment_status.return_value = mock_deployment
    assert get_kubernetes_app_by_name('someservice', mock_client) == mock_deployment
    assert mock_client.deployments.read_namespaced_deployment_status.called
    assert not mock_client.deployments.read_namespaced_stateful_set_status.called

    mock_stateful_set = mock.Mock()
    mock_client.deployments.read_namespaced_deployment_status.reset_mock()
    mock_client.deployments.read_namespaced_deployment_status.side_effect = ApiException(404)
    mock_client.deployments.read_namespaced_stateful_set_status.return_value = mock_stateful_set
    assert get_kubernetes_app_by_name('someservice', mock_client) == mock_stateful_set
    assert mock_client.deployments.read_namespaced_deployment_status.called
    assert mock_client.deployments.read_namespaced_stateful_set_status.called


def test_pods_for_service_instance():
    mock_client = mock.Mock()
    assert pods_for_service_instance(
        'kurupt',
        'fm',
        mock_client,
    ) == mock_client.core.list_namespaced_pod.return_value.items


def test_get_active_shas_for_service():
    mock_pod_list = [
        mock.Mock(metadata=mock.Mock(labels={
            'yelp.com/paasta_config_sha': 'a123',
            'yelp.com/paasta_git_sha': 'b456',
        })),
        mock.Mock(metadata=mock.Mock(labels={
            'yelp.com/paasta_config_sha': 'a123!!!',
            'yelp.com/paasta_git_sha': 'b456!!!',
        })),
        mock.Mock(metadata=mock.Mock(labels={
            'yelp.com/paasta_config_sha': 'a123!!!',
            'yelp.com/paasta_git_sha': 'b456!!!',
        })),
    ]
    assert get_active_shas_for_service(mock_pod_list) == {
        'git_sha': {'b456', 'b456!!!'},
        'config_sha': {'a123', 'a123!!!'},
    }


def test_get_all_pods():
    mock_client = mock.Mock()
    assert get_all_pods(mock_client) == mock_client.core.list_namespaced_pod.return_value.items


def test_get_all_nodes():
    mock_client = mock.Mock()
    assert get_all_nodes(mock_client) == mock_client.core.list_node.return_value.items


def test_filter_pods_for_service_instance():
    mock_pod_1 = mock.MagicMock(
        metadata=mock.MagicMock(
            labels={'yelp.com/paasta_service': 'kurupt', 'yelp.com/paasta_instance': 'fm'},
        ),
    )
    mock_pod_2 = mock.MagicMock(
        metadata=mock.MagicMock(
            labels={'yelp.com/paasta_service': 'kurupt', 'yelp.com/paasta_instance': 'garage'},
        ),
    )
    mock_pods = [mock_pod_1, mock_pod_2]
    assert filter_pods_by_service_instance(mock_pods, 'kurupt', 'fm') == [mock_pod_1]


def test_is_pod_ready():
    mock_pod = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(
                    type='Ready',
                    status='True',
                ),
                mock.MagicMock(
                    type='Another',
                    status='False',
                ),
            ],
        ),
    )
    assert is_pod_ready(mock_pod)

    mock_pod = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(
                    type='Ready',
                    status='False',
                ),
                mock.MagicMock(
                    type='Another',
                    status='False',
                ),
            ],
        ),
    )
    assert not is_pod_ready(mock_pod)

    mock_pod = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(
                    type='Another',
                    status='False',
                ),
            ],
        ),
    )
    assert not is_pod_ready(mock_pod)


def test_is_node_ready():
    mock_node = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(
                    type='Ready',
                    status='True',
                ),
                mock.MagicMock(
                    type='Another',
                    status='False',
                ),
            ],
        ),
    )
    assert is_node_ready(mock_node)

    mock_node = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(
                    type='Ready',
                    status='False',
                ),
                mock.MagicMock(
                    type='Another',
                    status='False',
                ),
            ],
        ),
    )
    assert not is_node_ready(mock_node)

    mock_node = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(
                    type='Another',
                    status='False',
                ),
            ],
        ),
    )
    assert not is_node_ready(mock_node)


def test_filter_nodes_by_blacklist():
    with mock.patch(
        'paasta_tools.kubernetes_tools.host_passes_whitelist', autospec=True,
    ) as mock_host_passes_whitelist, mock.patch(
        'paasta_tools.kubernetes_tools.host_passes_blacklist', autospec=True,
    ) as mock_host_passes_blacklist, mock.patch(
        'paasta_tools.kubernetes_tools.maybe_add_yelp_prefix', autospec=True,
        side_effect=lambda x: x,
    ):
        mock_nodes = [mock.Mock(), mock.Mock()]
        mock_host_passes_blacklist.return_value = True
        mock_host_passes_whitelist.return_value = True
        ret = filter_nodes_by_blacklist(
            mock_nodes, blacklist=[('location', 'westeros')], whitelist=('nodes', ['1', '2']),
        )
        assert ret == mock_nodes

        mock_nodes = [mock.Mock(), mock.Mock()]
        mock_host_passes_blacklist.return_value = True
        mock_host_passes_whitelist.return_value = True
        ret = filter_nodes_by_blacklist(mock_nodes, blacklist=[('location', 'westeros')], whitelist=None)
        assert ret == mock_nodes

        mock_host_passes_blacklist.return_value = True
        mock_host_passes_whitelist.return_value = False
        ret = filter_nodes_by_blacklist(
            mock_nodes, blacklist=[('location', 'westeros')], whitelist=('nodes', ['1', '2']),
        )
        assert ret == []

        mock_host_passes_blacklist.return_value = False
        mock_host_passes_whitelist.return_value = True
        ret = filter_nodes_by_blacklist(
            mock_nodes, blacklist=[('location', 'westeros')], whitelist=('nodes', ['1', '2']),
        )
        assert ret == []

        mock_host_passes_blacklist.return_value = False
        mock_host_passes_whitelist.return_value = False
        ret = filter_nodes_by_blacklist(
            mock_nodes, blacklist=[('location', 'westeros')], whitelist=('nodes', ['1', '2']),
        )
        assert ret == []


def test_get_nodes_grouped_by_attribute():
    with mock.patch(
        'paasta_tools.kubernetes_tools.maybe_add_yelp_prefix', autospec=True,
        side_effect=lambda x: x,
    ):
        mock_node_1 = mock.MagicMock(
            metadata=mock.MagicMock(
                labels={'region': 'westeros'},
            ),
        )
        mock_node_2 = mock.MagicMock(
            metadata=mock.MagicMock(
                labels={'region': 'middle-earth'},
            ),
        )
        assert get_nodes_grouped_by_attribute([mock_node_1, mock_node_2], 'region') == {
            'westeros': [mock_node_1],
            'middle-earth': [mock_node_2],
        }
        assert get_nodes_grouped_by_attribute([mock_node_1, mock_node_2], 'superregion') == {}


def test_maybe_add_yelp_prefix():
    assert maybe_add_yelp_prefix('kubernetes.io/thing') == 'kubernetes.io/thing'
    assert maybe_add_yelp_prefix('region') == 'yelp.com/region'


def test_sanitise_service_name():
    assert sanitise_service_name('my_service') == 'my--service'
    assert sanitise_service_name('myservice') == 'myservice'


def test_create_kubernetes_secret_signature():
    mock_client = mock.Mock()
    create_kubernetes_secret_signature(
        kube_client=mock_client,
        secret='mortys-fate',
        service='universe',
        secret_signature='ab1234',
    )
    assert mock_client.core.create_namespaced_config_map.called


def test_update_kubernetes_secret_signature():
    mock_client = mock.Mock()
    update_kubernetes_secret_signature(
        kube_client=mock_client,
        secret='mortys-fate',
        service='universe',
        secret_signature='ab1234',
    )
    assert mock_client.core.replace_namespaced_config_map.called


def test_get_kubernetes_secret_signature():
    mock_client = mock.Mock()
    mock_client.core.read_namespaced_config_map.return_value = mock.Mock(
        data={'signature': 'hancock'},
    )
    assert get_kubernetes_secret_signature(
        kube_client=mock_client,
        secret='mortys-morty',
        service='universe',
    ) == 'hancock'
    mock_client.core.read_namespaced_config_map.side_effect = ApiException(404)
    assert get_kubernetes_secret_signature(
        kube_client=mock_client,
        secret='mortys-morty',
        service='universe',
    ) is None
    mock_client.core.read_namespaced_config_map.side_effect = ApiException(401)
    with pytest.raises(ApiException):
        get_kubernetes_secret_signature(
            kube_client=mock_client,
            secret='mortys-morty',
            service='universe',
        )


def test_create_secret():
    mock_client = mock.Mock()
    mock_secret_provider = mock.Mock()
    mock_secret_provider.decrypt_secret_raw.return_value = bytes("plaintext", 'utf-8')
    create_secret(
        kube_client=mock_client,
        service='universe',
        secret='mortys-fate',
        secret_provider=mock_secret_provider,
    )
    assert mock_client.core.create_namespaced_secret.called
    mock_secret_provider.decrypt_secret_raw.assert_called_with('mortys-fate')


def test_update_secret():
    mock_client = mock.Mock()
    mock_secret_provider = mock.Mock()
    mock_secret_provider.decrypt_secret_raw.return_value = bytes("plaintext", 'utf-8')
    update_secret(
        kube_client=mock_client,
        service='universe',
        secret='mortys-fate',
        secret_provider=mock_secret_provider,
    )
    assert mock_client.core.replace_namespaced_secret.called
    mock_secret_provider.decrypt_secret_raw.assert_called_with('mortys-fate')


def test_get_kubernetes_secret_hashes():
    with mock.patch(
        'paasta_tools.kubernetes_tools.KubeClient', autospec=True,
    ) as mock_client, mock.patch(
        'paasta_tools.kubernetes_tools.is_secret_ref', autospec=True,
    ) as mock_is_secret_ref, mock.patch(
        'paasta_tools.kubernetes_tools.get_kubernetes_secret_signature', autospec=True,
        return_value='somesig',
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        'paasta_tools.kubernetes_tools.is_shared_secret', autospec=True,
    ) as mock_is_shared_secret:
        mock_is_secret_ref.side_effect = lambda x: False if x == 'ASECRET' else True
        mock_is_shared_secret.side_effect = lambda x: False if not x.startswith("SHARED") else True
        hashes = get_kubernetes_secret_hashes(
            environment_variables={'A': 'SECRET(ref)', 'NOT': 'ASECRET', 'SOME': 'SHAREDSECRET(ref1)'},
            service='universe',
        )
        mock_get_kubernetes_secret_signature.assert_has_calls([
            mock.call(
                kube_client=mock_client.return_value,
                secret='ref',
                service='universe',
            ),
            mock.call(
                kube_client=mock_client.return_value,
                secret='ref1',
                service=SHARED_SECRET_SERVICE,
            ),
        ])
        assert hashes == {'SECRET(ref)': 'somesig', 'SHAREDSECRET(ref1)': 'somesig'}
