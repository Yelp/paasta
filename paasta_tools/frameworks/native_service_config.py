#!/usr/bin/env python
# Without this, the import of mesos.interface breaks because paasta_tools.mesos exists
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

import service_configuration_lib
from addict import Dict

from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_docker_url
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import paasta_print

MESOS_TASK_SPACER = '.'


class NativeServiceConfig(LongRunningServiceConfig):
    def __init__(self, service, instance, cluster, config_dict, branch_dict, soa_dir,
                 service_namespace_config=None):
        super(NativeServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )
        # service_namespace_config may be omitted/set to None at first, then set
        # after initializing. e.g. we do this in load_paasta_native_job_config
        # so we can call get_nerve_namespace() to figure out what SNC to read.
        # It may also be set to None if this service is not in nerve.
        if service_namespace_config is not None:
            self.service_namespace_config = service_namespace_config
        else:
            self.service_namespace_config = ServiceNamespaceConfig()

    def task_name(self, base_task):
        code_sha = get_code_sha_from_dockerurl(
            base_task.container.docker.image
        )

        filled_in_task = copy.deepcopy(base_task)
        filled_in_task.update(
            Dict(
                name='',
                task_id=Dict(value=''),
                slave_id=Dict(value=''),
            )
        )

        config_hash = get_config_hash(
            filled_in_task,
            force_bounce=self.get_force_bounce(),
        )

        return compose_job_id(
            self.service,
            self.instance,
            git_hash=code_sha,
            config_hash=config_hash,
            spacer=MESOS_TASK_SPACER,
        )

    def base_task(self, system_paasta_config, portMappings=True):
        """Return a TaskInfo Dict with all the fields corresponding to the
        configuration filled in.

        Does not include task.slave_id or a task.id; those need to be
        computed separately.
        """
        docker_volumes = self.get_volumes(
            system_volumes=system_paasta_config.get_volumes()
        )
        task = Dict({
            'container': {
                'type': 'DOCKER',
                'docker': {
                    'image': get_docker_url(
                        system_paasta_config.get_docker_registry(),
                        self.get_docker_image()
                    ),
                    'parameters': [
                        Dict(key=param['key'], value=param['value'])
                        for param in self.format_docker_parameters()
                    ],
                    'network': self.get_mesos_network_mode()
                },
                'volumes': [
                    {
                        'container_path': volume['containerPath'],
                        'host_path': volume['hostPath'],
                        'mode': volume['mode'].upper(),
                    }
                    for volume in docker_volumes
                ],
            },
            'command': {
                'value': self.get_cmd(),
                'uris': [
                    {
                        'value': system_paasta_config.get_dockercfg_location(),
                        'extract': False
                    }
                ]
            },
            'resources': [
                {
                    'name': 'cpus',
                    'type': 'SCALAR',
                    'scalar': {'value': self.get_cpus()},
                },
                {
                    'name': 'mem',
                    'type': 'SCALAR',
                    'scalar': {'value': self.get_mem()}
                }
            ],
        })

        if portMappings:
            task.container.docker.port_mappings = [
                Dict(
                    container_port=self.get_container_port(),
                    # filled by tasks_and_state_for_offer()
                    host_port=0,
                    protocol='tcp'
                )
            ]

            task.resources.append(
                Dict(
                    name='ports',
                    type='RANGES',
                    ranges=Dict(
                        # filled by tasks_and_state_for_offer
                        range=[Dict(begin=0, end=0)]
                    )
                )
            )

        task.name = self.task_name(task)

        return Dict(task)

    def get_mesos_network_mode(self):
        return self.get_net().upper()

    def get_constraints(self):
        return self.config_dict.get('constraints', None)


def load_paasta_native_job_config(
    service,
    instance,
    cluster,
    load_deployments=True,
    soa_dir=DEFAULT_SOA_DIR,
    instance_type='paasta_native',
    config_overrides=None
):
    service_paasta_native_jobs = read_service_config(
        service=service,
        instance=instance,
        instance_type=instance_type,
        cluster=cluster,
        soa_dir=soa_dir
    )
    branch_dict = {}
    if load_deployments:
        deployments_json = load_deployments_json(service, soa_dir=soa_dir)
        branch = get_paasta_branch(cluster=cluster, instance=instance)
        branch_dict = deployments_json.get_branch_dict(service, branch)

    instance_config_dict = service_paasta_native_jobs[instance].copy()
    instance_config_dict.update(config_overrides or {})
    service_config = NativeServiceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=instance_config_dict,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )

    service_namespace_config = load_service_namespace_config(
        service=service,
        namespace=service_config.get_nerve_namespace(),
        soa_dir=soa_dir
    )
    service_config.service_namespace_config = service_namespace_config

    return service_config


def read_service_config(service, instance, instance_type, cluster, soa_dir=DEFAULT_SOA_DIR):
    conf_file = '%s-%s' % (instance_type, cluster)
    full_path = '%s/%s/%s.yaml' % (soa_dir, service, conf_file)
    paasta_print("Reading paasta-remote configuration file: %s" % full_path)

    config = service_configuration_lib.read_extra_service_information(
        service,
        conf_file,
        soa_dir=soa_dir
    )

    if instance not in config:
        raise UnknownNativeServiceError(
            'No job named "%s" in config file %s: \n%s' % (
                instance, full_path, open(full_path).read())
        )

    return config


class UnknownNativeServiceError(Exception):
    pass
