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
import logging

import pykube
import service_configuration_lib

from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SPACER


log = logging.getLogger(__name__)


def load_kubernetes_pod_config(service, instance, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR):
    general_config = service_configuration_lib.read_service_configuration(
        service,
        soa_dir=soa_dir
    )
    kubernetes_conf_file = "kubernetes-%s" % cluster
    log.info("Reading kubernetes configuration file: %s.yaml", kubernetes_conf_file)
    instance_configs = service_configuration_lib.read_extra_service_information(
        service_name=service,
        extra_info=kubernetes_conf_file,
        soa_dir=soa_dir
    )

    if instance not in instance_configs:
        raise NoConfigurationForServiceError(
            "%s not found in config file %s/%s/%s.yaml." % (instance, soa_dir, service, kubernetes_conf_file)
        )

    general_config = deep_merge_dictionaries(overrides=instance_configs[instance], defaults=general_config)

    branch_dict = {}
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        branch = general_config.get('branch', get_paasta_branch(cluster, instance))
        deploy_group = general_config.get('deploy_group', branch)
        branch_dict = deployments_json.get_branch_dict_v2(service, branch, deploy_group)

    return KubernetesPodConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
    )


class KubernetesPodConfig(LongRunningServiceConfig):

    def __init__(self, service, instance, cluster, config_dict, branch_dict):
        super(KubernetesPodConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )

    def format_kubernetes_deployment_dict(self):
        service = self.get_service()
        instance = self.get_instance()
        git_sha = self.branch_dict['git_sha']

        docker_registry = load_system_paasta_config().get_docker_registry()
        resource_requirements = {
            'cpu': self.get_cpus(),
            'memory': '%dMi' % self.get_mem(),
        }

        container_spec = {
            'name': 'container1',
            'image': '%s/%s' % (
                docker_registry,
                self.branch_dict['docker_image'],
            ),
            'env': {},
            'imagePullPolicy': 'Always',
            'ports': [
                {
                    'containerPort': 8888,
                    'name': 'port0',
                }
            ],
            'resources': {
                'requests': resource_requirements,
                'limits': resource_requirements,
            },
        }

        cmd = self.get_cmd()
        if cmd:
            container_spec['command'] = [cmd]

        args = self.get_args()
        if args:
            container_spec['args'] = args

        complete_dict = {
            'metadata': {
                'namespace': 'default',
                'labels': {
                    'service': service,
                    'instance': instance,
                    'git_sha': git_sha,
                },
            },
            'spec': {
                'replicas': 1,
                'template': {
                    'metadata': {
                        'namespace': 'default',
                        'labels': {
                            'service': service,
                            'instance': instance,
                            'git_sha': git_sha,
                        },
                    },
                    'spec': {
                        'containers': [
                            container_spec,
                        ],
                        'restartPolicy': 'Always',
                        'volumes': [],
                    },
                },
            },
        }

        config_hash = get_config_hash(complete_dict)
        complete_dict['metadata']['labels']['config_hash'] = config_hash
        complete_dict['spec']['template']['metadata']['labels']['config_hash'] = config_hash
        complete_dict['metadata']['name'] = replace_underscores(
            compose_job_id(
                service,
                instance,
                git_sha,
                config_hash,
            )
        )

        return complete_dict

    def format_kubernetes_service_dict(self, deployment_config):
        service_config = {
            'metadata': {
                'name': 'my-service'
            },
            'spec': {
                'selector': deployment_config['metadata']['labels'],
                'ports': [
                    {
                        'protocol': 'TCP',
                        'targetPort': port_def['containerPort'],
                        'port': 80 + increment,
                    }
                    for increment, port_def in enumerate(
                        deployment_config['spec']['template']['spec']['containers'][0]['ports'],
                    )
                ],
            },
        }

        return service_config

    def get_args(self):
        """
        Gets args for the service. Overloads the default method since
        kubernetes can have args with no cmd specified.

        :param service_config: The service instance's configuration dictionary
        :returns: An array of args specified in the config, ``[]`` if not specified
        """
        return self.config_dict.get('args', [])


def replace_underscores(string):
    return string.replace('_', '--')


class KubeClient(object):
    def __init__(self):
        self.config = load_system_paasta_config().get_kubernetes_config()
        try:
            self.pykube_config = pykube.KubeConfig.from_url(
                # url=self.config['url'],
                url='https://paasta-mesosstage.yelp:6443',
            )
        except KeyError:
            raise PaastaNotConfiguredError('KubeClient config not present in kubernetes_config')
        # Disable SSL cert validation
        self.pykube_config.cluster['insecure-skip-tls-verify'] = True
        self.api = pykube.HTTPClient(self.pykube_config)

    def get_deployments(self):
        return pykube.Deployment.objects(self.api).filter(namespace='default')

    def get_matching_deployments(self, service, instance):
        return self.get_deployments().filter(
            selector={
                'service__eq': service,
                'instance__eq': instance,
            },
        )

    def create_deployment(self, deployment_config, service_config):
        pykube.Deployment(self.api, deployment_config).create()
        pykube.Service(self.api, service_config).create()


def format_pod_name(service, instance):
    return SPACER.join((service, instance))
