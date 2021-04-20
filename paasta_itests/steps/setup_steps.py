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
import json
import os
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import yaml
from behave import given
from behave import when
from itest_utils import get_service_connection_string

from paasta_tools import utils
from paasta_tools.api.client import get_paasta_oapi_client_by_url
from paasta_tools.frameworks import native_scheduler
from paasta_tools.utils import decompose_job_id


def _get_zookeeper_connection_string(chroot):
    return "zk://{}/{}".format(get_service_connection_string("zookeeper"), chroot)


def setup_system_paasta_config():
    zk_connection_string = _get_zookeeper_connection_string("mesos-testcluster")
    system_paasta_config = utils.SystemPaastaConfig(
        {
            "cluster": "testcluster",
            "deployd_log_level": "DEBUG",
            "docker_volumes": [],
            "docker_registry": "docker-dev.yelpcorp.com",
            "zookeeper": zk_connection_string,
            "synapse_port": 3212,
            "dashboard_links": {"testcluster": {}},
        },
        "/some_fake_path_to_config_dir/",
    )
    return system_paasta_config


def get_paasta_api_url():
    return "http://{}/{}".format(get_service_connection_string("api"), "swagger.json")


def setup_paasta_api_client():
    return get_paasta_oapi_client_by_url(urlparse(get_paasta_api_url()))


def _generate_mesos_cli_config(zk_host_and_port):
    config = {
        "profile": "default",
        "default": {
            "master": zk_host_and_port,
            "log_level": "warning",
            "log_file": "None",
            "response_timeout": 5,
        },
    }
    return config


def write_mesos_cli_config(config):
    with NamedTemporaryFile(mode="w", delete=False) as mesos_cli_config_file:
        mesos_cli_config_file.write(json.dumps(config))
        return mesos_cli_config_file.name


def write_etc_paasta(context, config, filename):
    context.etc_paasta = "/etc/paasta"
    if not os.path.exists(context.etc_paasta):
        os.makedirs(context.etc_paasta)
    with open(os.path.join(context.etc_paasta, filename), "w") as f:
        f.write(json.dumps(config))


@given("we add a new docker volume to the public config")
def add_volume_public_config(context):
    write_etc_paasta(
        context,
        {
            "volumes": [
                {
                    "hostPath": "/nail/etc/beep",
                    "containerPath": "/nail/etc/beep",
                    "mode": "RO",
                },
                {
                    "hostPath": "/nail/etc/bop",
                    "containerPath": "/nail/etc/bop",
                    "mode": "RO",
                },
                {
                    "hostPath": "/nail/etc/boop",
                    "containerPath": "/nail/etc/boop",
                    "mode": "RO",
                },
                {
                    "hostPath": "/nail/tmp/noob",
                    "containerPath": "/nail/tmp/noob",
                    "mode": "RO",
                },
            ]
        },
        "volumes.json",
    )


@given("a working paasta cluster")
def working_paasta_cluster(context):
    return working_paasta_cluster_with_registry(context, "docker.io")


@given("a working paasta cluster, with docker registry {docker_registry}")
def working_paasta_cluster_with_registry(context, docker_registry):
    if not hasattr(context, "paasta_api_client"):
        context.paasta_api_client = setup_paasta_api_client()

    mesos_cli_config = _generate_mesos_cli_config(
        _get_zookeeper_connection_string("mesos-testcluster")
    )
    mesos_cli_config_filename = write_mesos_cli_config(mesos_cli_config)
    context.tag_version = 0

    write_etc_paasta(
        context,
        {
            "cluster": "testcluster",
            "zookeeper": "zk://zookeeper/mesos-testcluster",
            "vault_environment": "devc",
            "docker_registry": docker_registry,
        },
        "cluster.json",
    )
    write_etc_paasta(context, {"log_writer": {"driver": "null"}}, "logs.json")
    write_etc_paasta(context, {"sensu_host": None}, "sensu.json")
    write_etc_paasta(
        context,
        {
            "volumes": [
                {
                    "hostPath": "/nail/etc/beep",
                    "containerPath": "/nail/etc/beep",
                    "mode": "RO",
                },
                {
                    "hostPath": "/nail/etc/bop",
                    "containerPath": "/nail/etc/bop",
                    "mode": "RO",
                },
                {
                    "hostPath": "/nail/etc/boop",
                    "containerPath": "/nail/etc/boop",
                    "mode": "RO",
                },
            ]
        },
        "volumes.json",
    )
    write_etc_paasta(
        context,
        {"paasta_native": {"principal": "paasta_native", "secret": "secret4"}},
        "paasta_native.json",
    )
    write_etc_paasta(
        context, {"mesos_config": {"path": mesos_cli_config_filename}}, "mesos.json"
    )
    write_etc_paasta(
        context,
        {"api_endpoints": {"testcluster": get_paasta_api_url()}},
        "api_endpoints.json",
    )
    write_etc_paasta(
        context, {"dashboard_links": {"testcluster": {}}}, "dashboard_links.json",
    )
    write_etc_paasta(context, {"deployd_use_zk_queue": True}, "deployd.json")


@given('we have yelpsoa-configs for native service "{job_id}"')
def write_soa_dir_native_service(context, job_id):
    (service, instance, _, __) = decompose_job_id(job_id)
    try:
        soa_dir = context.soa_dir
    except AttributeError:
        soa_dir = "/nail/etc/services/"
    if not os.path.exists(os.path.join(soa_dir, service)):
        os.makedirs(os.path.join(soa_dir, service))
    with open(
        os.path.join(soa_dir, service, "paasta_native-%s.yaml" % context.cluster), "w"
    ) as f:
        f.write(
            yaml.safe_dump(
                {"%s" % instance: {"cpus": 0.1, "mem": 100, "cmd": "/bin/sleep 300"}}
            )
        )
    context.soa_dir = soa_dir
    context.service = service
    context.instance = instance


@given("we load_paasta_native_job_config")
def call_load_paasta_native_job_config(context):
    context.new_config = native_scheduler.load_paasta_native_job_config(
        service=context.service,
        instance=context.instance,
        cluster=context.cluster,
        soa_dir=context.soa_dir,
    )


@given(
    'we have a deployments.json for the service "{service}" with {disabled} instance '
    '"{csv_instances}" image "{image}"'
)
def write_soa_dir_deployments(context, service, disabled, csv_instances, image):
    if disabled == "disabled":
        desired_state = "stop"
    else:
        desired_state = "start"

    if not os.path.exists(os.path.join(context.soa_dir, service)):
        os.makedirs(os.path.join(context.soa_dir, service))
    with open(os.path.join(context.soa_dir, service, "deployments.json"), "w") as dp:
        dp.write(
            json.dumps(
                {
                    "v1": {
                        "{}:paasta-{}".format(
                            service, utils.get_paasta_branch(context.cluster, instance)
                        ): {"docker_image": image, "desired_state": desired_state}
                        for instance in csv_instances.split(",")
                    },
                    "v2": {
                        "deployments": {
                            f"{context.cluster}.{instance}": {
                                "docker_image": image,
                                "git_sha": "deadbeef",
                            }
                            for instance in csv_instances.split(",")
                        },
                        "controls": {
                            f"{service}:{context.cluster}.{instance}": {
                                "desired_state": desired_state,
                                "force_bounce": None,
                            }
                            for instance in csv_instances.split(",")
                        },
                    },
                }
            )
        )


@given(
    'we have a deployments.json for the service "{service}" with {disabled} instance "{csv_instance}"'
)
def write_soa_dir_deployments_default_image(context, service, disabled, csv_instance):
    write_soa_dir_deployments(
        context,
        service,
        disabled,
        csv_instance,
        "test-image-foobar%d" % context.tag_version,
    )


@when(
    (
        'we set the "{field}" field of the {framework} config for service "{service}"'
        ' and instance "{instance}" to "{value}"'
    )
)
def modify_configs(context, field, framework, service, instance, value):
    soa_dir = context.soa_dir
    with open(
        os.path.join(soa_dir, service, f"{framework}-{context.cluster}.yaml"), "r+"
    ) as f:
        data = yaml.safe_load(f.read())
        data[instance][field] = value
        f.seek(0)
        f.write(yaml.safe_dump(data))
        f.truncate()


@when(
    (
        'we set the "{field}" field of the {framework} config for service "{service}"'
        ' and instance "{instance}" to the integer {value:d}'
    )
)
def modify_configs_for_int(context, field, framework, service, instance, value):
    modify_configs(context, field, framework, service, instance, value)
