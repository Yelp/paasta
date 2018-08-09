import unittest
from typing import Sequence

import mock
import pytest
from kubernetes.client import V1AWSElasticBlockStoreVolumeSource
from kubernetes.client import V1Container
from kubernetes.client import V1ContainerPort
from kubernetes.client import V1Deployment
from kubernetes.client import V1DeploymentSpec
from kubernetes.client import V1DeploymentStrategy
from kubernetes.client import V1EnvVar
from kubernetes.client import V1ExecAction
from kubernetes.client import V1Handler
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1HTTPGetAction
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1Lifecycle
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodTemplateSpec
from kubernetes.client import V1Probe
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1RollingUpdateDeployment
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount

from paasta_tools.kubernetes_tools import create_deployment
from paasta_tools.kubernetes_tools import ensure_paasta_namespace
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here_for_nerve
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfigDict
from paasta_tools.kubernetes_tools import KubeService
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import load_kubernetes_service_config
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.kubernetes_tools import read_all_registrations_for_service_instance
from paasta_tools.kubernetes_tools import update_deployment
from paasta_tools.utils import AwsEbsVolume
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaNotConfiguredError


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

    def test_get_bounce_method(self):
        assert self.deployment.get_bounce_method() == 'RollingUpdate'
        self.deployment.config_dict['bounce_method'] = 'downthenup'
        assert self.deployment.get_bounce_method() == 'Recreate'

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
            ret = self.deployment.get_sidecar_containers(mock_system_config)
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
            ret = self.deployment.get_sidecar_containers(mock_system_config)
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
                            command=['/nail/blah.sh', 'universal.credit'],
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
            },
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_environment', autospec=True,
            return_value=[
                V1EnvVar(
                    name='manager',
                    value='chabuddy',
                ),
            ],
        ):
            expected = [
                V1EnvVar(name='mc', value='grindah'),
                V1EnvVar(name='dj', value='beats'),
                V1EnvVar(name='manager', value='chabuddy'),
            ]
            assert expected == self.deployment.get_container_env()

    def test_get_kubernetes_environment(self):
        ret = self.deployment.get_kubernetes_environment()
        assert 'PAASTA_POD_IP' in [env.name for env in ret]

    def test_get_resource_requirements(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cpus', autospec=True,
            return_value=0.3,
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cpu_burst_pct', autospec=True,
            return_value=200,
        ), mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_mem', autospec=True,
            return_value=2048,
        ):
            assert self.deployment.get_resource_requirements() == V1ResourceRequirements(
                limits={
                    'cpu': 0.6,
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
            service_namespace_config.get_mode.return_value = 'http'
            service_namespace_config.get_healthcheck_uri.return_value = '/status'
            assert self.deployment.get_kubernetes_containers(
                docker_volumes=mock_docker_volumes,
                system_paasta_config=mock_system_config,
                aws_ebs_volumes=mock_aws_ebs_volumes,
                service_namespace_config=service_namespace_config,
            ) == expected

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
            ]
            assert self.deployment.get_volume_mounts(
                docker_volumes=mock_docker_volumes,
                aws_ebs_volumes=mock_aws_ebs_volumes,
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

    def test_format_kubernetes_app_dict(self):
        with mock.patch(
            'paasta_tools.kubernetes_tools.load_system_paasta_config', autospec=True,
        ) as mock_load_system_config, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_docker_url', autospec=True,
        ) as mock_get_docker_url, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volumes', autospec=True,
        ) as mock_get_volumes, mock.patch(
            'paasta_tools.kubernetes_tools.get_code_sha_from_dockerurl', autospec=True,
        ) as mock_get_code_sha_from_dockerurl, mock.patch(
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
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instances', autospec=True,
        ) as mock_get_instances, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_deployment_strategy_config', autospec=True,
        ) as mock_get_deployment_strategy_config, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_containers', autospec=True,
        ) as mock_get_kubernetes_containers, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_volumes', autospec=True,
            return_value=[],
        ) as mock_get_pod_volumes, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name', autospec=True,
        ), mock.patch(
            'paasta_tools.kubernetes_tools.get_config_hash', autospec=True,
        ) as mock_get_config_hash, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_force_bounce', autospec=True,
        ) as mock_get_force_bounce, mock.patch(
            'paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.sanitize_for_config_hash', autospec=True,
        ) as mock_sanitize_for_config_hash:
            ret = self.deployment.format_kubernetes_app()
            assert mock_load_system_config.called
            assert mock_get_docker_url.called
            assert mock_get_volumes.called
            assert mock_get_pod_volumes.called
            mock_get_config_hash.assert_called_with(
                mock_sanitize_for_config_hash.return_value,
                force_bounce=mock_get_force_bounce.return_value,
            )
            expected = V1Deployment(
                api_version='apps/v1',
                kind='Deployment',
                metadata=V1ObjectMeta(
                    labels={
                        'config_sha': mock_get_config_hash.return_value,
                        'git_sha': mock_get_code_sha_from_dockerurl.return_value,
                        'instance': mock_get_instance.return_value,
                        'service': mock_get_service.return_value,
                    },
                    name='kurupt-fm',
                ),
                spec=V1DeploymentSpec(
                    replicas=mock_get_instances.return_value,
                    selector=V1LabelSelector(
                        match_labels={
                            'instance': mock_get_instance.return_value,
                            'service': mock_get_service.return_value,
                        },
                    ),
                    strategy=mock_get_deployment_strategy_config.return_value,

                    template=V1PodTemplateSpec(
                        metadata=V1ObjectMeta(
                            labels={
                                'config_sha': mock_get_config_hash.return_value,
                                'git_sha': mock_get_code_sha_from_dockerurl.return_value,
                                'instance': mock_get_instance.return_value,
                                'service': mock_get_service.return_value,
                            },
                        ),
                        spec=V1PodSpec(
                            containers=mock_get_kubernetes_containers.return_value,
                            restart_policy='Always',
                            volumes=[],
                        ),
                    ),
                ),
            )
            assert ret == expected

    def test_sanitize_config_hash(self):
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

    def test_get_bounce_margin_factor(self):
        assert isinstance(self.deployment.get_bounce_margin_factor(), float)


def test_read_all_registrations_for_service_instance():
    with mock.patch(
        'paasta_tools.kubernetes_tools.load_system_paasta_config', autospec=True,
    ), mock.patch(
        'paasta_tools.kubernetes_tools.load_kubernetes_service_config', autospec=True,
    ) as mock_load_kubernetes_service_config:
        assert read_all_registrations_for_service_instance(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            soa_dir='/nail/blah',
        ) == mock_load_kubernetes_service_config.return_value.get_registrations()
        mock_load_kubernetes_service_config.assert_called_with(
            service='kurupt',
            instance='fm',
            cluster='brentford',
            load_deployments=False,
            soa_dir='/nail/blah',
        )


def test_get_kubernetes_services_running_here():
    with mock.patch(
        'paasta_tools.kubernetes_tools.requests.get', autospec=True,
    ) as mock_requests_get:
        mock_requests_get.return_value.json.return_value = {'items': []}
        assert get_kubernetes_services_running_here() == []

        mock_requests_get.return_value.json.return_value = {'items': [
            {
                'status': {
                    'phase': 'Running',
                    'podIP': '10.1.1.1',
                },
                'metadata': {
                    'namespace': 'paasta',
                    'labels': {
                        'service': 'kurupt',
                        'instance': 'fm',
                    },
                },
            }, {
                'status': {
                    'phase': 'Something',
                    'podIP': '10.1.1.1',
                },
                'metadata': {
                    'namespace': 'paasta',
                    'labels': {
                        'service': 'kurupt',
                        'instance': 'garage',
                    },
                },
            }, {
                'status': {
                    'phase': 'Running',
                },
                'metadata': {
                    'namespace': 'paasta',
                    'labels': {
                        'service': 'kurupt',
                        'instance': 'grindah',
                    },
                },
            }, {
                'status': {
                    'phase': 'Running',
                    'podIP': '10.1.1.1',
                },
                'metadata': {
                    'namespace': 'kube-system',
                    'labels': {
                        'service': 'kurupt',
                        'instance': 'beats',
                    },
                },
            },
        ]}
        assert get_kubernetes_services_running_here() == [
            KubeService(
                name='kurupt',
                instance='fm',
                port=8888,
                pod_ip='10.1.1.1',
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
        'paasta_tools.kubernetes_tools.read_all_registrations_for_service_instance', autospec=True,
    ) as mock_read_all_registrations_for_service_instance, mock.patch(
        'paasta_tools.kubernetes_tools.load_service_namespace_config', autospec=True,
    ) as mock_load_service_namespace:

        mock_get_kubernetes_services_running_here.return_value = [
            KubeService(
                name='kurupt',
                instance='fm',
                port=8888,
                pod_ip='10.1.1.1',
            ),
            KubeService(
                name='kurupt',
                instance='garage',
                port=8888,
                pod_ip='10.1.1.1',
            ),
        ]

        mock_read_all_registrations_for_service_instance.side_effect = lambda a, b, c, d: [f"{a}.{b}"]
        mock_load_service_namespace.side_effect = lambda service, namespace, soa_dir: MockNerveDict(name=namespace)
        mock_load_system_config.side_effect = PaastaNotConfiguredError
        ret = get_kubernetes_services_running_here_for_nerve('brentford', '/nail/blah')
        assert ret == []

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
        mock_read_all_registrations_for_service_instance.assert_has_calls([
            mock.call(
                'kurupt', 'fm', 'brentford', '/nail/blah',
            ),
            mock.call(
                'kurupt', 'garage', 'brentford', '/nail/blah',
            ),
        ])

        mock_read_all_registrations_for_service_instance.side_effect = NoConfigurationForServiceError
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


def test_ensure_paasta_namespace():
    mock_metadata = mock.Mock()
    type(mock_metadata).name = 'paasta'
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)))
    ensure_paasta_namespace(mock_client)
    assert not mock_client.core.create_namespace.called

    mock_metadata = mock.Mock()
    type(mock_metadata).name = 'kube-system'
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)))
    ensure_paasta_namespace(mock_client)
    assert mock_client.core.create_namespace.called

    mock_client.core.create_namespace.reset_mock()
    mock_namespaces = mock.Mock(items=[])
    mock_client = mock.Mock(core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)))
    ensure_paasta_namespace(mock_client)
    assert mock_client.core.create_namespace.called


def test_list_all_deployments():
    mock_deployments = mock.Mock(items=[])
    mock_client = mock.Mock(deployments=mock.Mock(list_namespaced_deployment=mock.Mock(return_value=mock_deployments)))
    assert list_all_deployments(mock_client) == []

    mock_item = mock.Mock(
        metadata=mock.Mock(
            labels={
                'service': 'kurupt',
                'instance': 'fm',
                'git_sha': 'a12345',
                'config_sha': 'b12345',
            },
        ),
    )
    type(mock_item).spec = mock.Mock(replicas=3)
    mock_deployments = mock.Mock(items=[mock_item])
    mock_client = mock.Mock(deployments=mock.Mock(list_namespaced_deployment=mock.Mock(return_value=mock_deployments)))
    assert list_all_deployments(mock_client) == [KubeDeployment(
        service='kurupt',
        instance='fm',
        git_sha='a12345',
        config_sha='b12345',
        replicas=3,
    )]


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
