#!/usr/bin/env python
# Without this, the import of mesos.interface breaks because paasta_tools.mesos exists
from __future__ import absolute_import
from __future__ import unicode_literals

import binascii

import service_configuration_lib
from mesos.interface import mesos_pb2

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
    def __init__(self, service, instance, cluster, config_dict, branch_dict,
                 service_namespace_config=None):
        super(NativeServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
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
        code_sha = get_code_sha_from_dockerurl(base_task.container.docker.image)

        filled_in_task = mesos_pb2.TaskInfo()
        filled_in_task.MergeFrom(base_task)
        filled_in_task.name = ""
        filled_in_task.task_id.value = ""
        filled_in_task.slave_id.value = ""

        config_hash = get_config_hash(
            binascii.b2a_base64(filled_in_task.SerializeToString()).decode(),
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
        """Return a TaskInfo protobuf with all the fields corresponding to the configuration filled in. Does not
        include task.slave_id or a task.id; those need to be computed separately."""
        task = mesos_pb2.TaskInfo()
        task.container.type = mesos_pb2.ContainerInfo.DOCKER
        task.container.docker.image = get_docker_url(system_paasta_config.get_docker_registry(),
                                                     self.get_docker_image())

        for param in self.format_docker_parameters():
            p = task.container.docker.parameters.add()
            p.key = param['key']
            p.value = param['value']

        task.container.docker.network = self.get_mesos_network_mode()

        docker_volumes = self.get_volumes(system_volumes=system_paasta_config.get_volumes())
        for volume in docker_volumes:
            v = task.container.volumes.add()
            v.mode = getattr(mesos_pb2.Volume, volume['mode'].upper())
            v.container_path = volume['containerPath']
            v.host_path = volume['hostPath']

        task.command.value = self.get_cmd()
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.get_cpus()
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.get_mem()

        if portMappings:
            pm = task.container.docker.port_mappings.add()
            pm.container_port = self.get_container_port()
            pm.host_port = 0  # will be filled in by tasks_and_state_for_offer()
            pm.protocol = "tcp"

            port = task.resources.add()
            port.name = "ports"
            port.type = mesos_pb2.Value.RANGES
            port.ranges.range.add()
            port.ranges.range[0].begin = 0  # will be filled in by tasks_and_state_for_offer().
            port.ranges.range[0].end = 0  # will be filled in by tasks_and_state_for_offer().

        task.name = self.task_name(task)

        docker_cfg_uri = task.command.uris.add()
        docker_cfg_uri.value = system_paasta_config.get_dockercfg_location()
        docker_cfg_uri.extract = False

        return task

    def get_mesos_network_mode(self):
        return getattr(mesos_pb2.ContainerInfo.DockerInfo, self.get_net().upper())

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
