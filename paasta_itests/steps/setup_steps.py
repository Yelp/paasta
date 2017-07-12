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
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import os
from tempfile import NamedTemporaryFile

import chronos
import yaml
from behave import given
from behave import when
from bravado.client import SwaggerClient
from itest_utils import get_service_connection_string

from paasta_tools import chronos_tools
from paasta_tools import marathon_tools
from paasta_tools import utils
from paasta_tools.frameworks import native_scheduler
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import paasta_print


def _get_marathon_connection_string():
    return 'http://%s' % get_service_connection_string('marathon')


def _get_chronos_connection_string():
    return 'http://%s' % get_service_connection_string('chronos')


def _get_zookeeper_connection_string(chroot):
    return 'zk://%s/%s' % (get_service_connection_string('zookeeper'), chroot)


def setup_system_paasta_config():
    marathon_connection_string = _get_marathon_connection_string()
    zk_connection_string = _get_zookeeper_connection_string('mesos-testcluster')
    chronos_connection_string = _get_chronos_connection_string()
    system_paasta_config = utils.SystemPaastaConfig({
        'cluster': 'testcluster',
        'deployd_log_level': 'DEBUG',
        'docker_volumes': [],
        'docker_registry': 'docker-dev.yelpcorp.com',
        'zookeeper': zk_connection_string,
        'synapse_port': 3212,
        'marathon_config': {
            'url': marathon_connection_string,
            'user': None,
            'password': None,
        },
        'chronos_config': {
            'user': None,
            'password': None,
            'url': [chronos_connection_string],
        },
    }, '/some_fake_path_to_config_dir/')
    return system_paasta_config


def setup_marathon_client():
    system_paasta_config = setup_system_paasta_config()
    marathon_config = marathon_tools.MarathonConfig(system_paasta_config.get_marathon_config())
    client = marathon_tools.get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                                marathon_config.get_password())
    return (client, marathon_config, system_paasta_config)


def setup_chronos_client():
    connection_string = get_service_connection_string('chronos')
    return chronos.connect(connection_string)


def setup_chronos_config():
    system_paasta_config = setup_system_paasta_config()
    chronos_config = chronos_tools.ChronosConfig(system_paasta_config.get_chronos_config())
    return chronos_config


def setup_paasta_api_client():
    return SwaggerClient.from_url("http://%s/%s" % (get_service_connection_string('api'), 'swagger.json'))


def _generate_mesos_cli_config(zk_host_and_port):
    config = {
        'profile': 'default',
        'default': {
            'master': zk_host_and_port,
            'log_level': 'warning',
            'log_file': 'None',
            'response_timeout': 5,
        }
    }
    return config


def write_mesos_cli_config(config):
    mesos_cli_config_file = NamedTemporaryFile(delete=False)
    mesos_cli_config_file.write(json.dumps(config))
    mesos_cli_config_file.close()
    return mesos_cli_config_file.name


def write_etc_paasta(context, config, filename):
    context.etc_paasta = '/etc/paasta'
    if not os.path.exists(context.etc_paasta):
        os.makedirs(context.etc_paasta)
    with open(os.path.join(context.etc_paasta, filename), 'w') as f:
        f.write(json.dumps(config))


@given('we add a new docker volume to the public config')
def add_volume_public_config(context):
    write_etc_paasta(context, {
        'volumes': [
            {'hostPath': '/nail/etc/beep', 'containerPath': '/nail/etc/beep', 'mode': 'RO'},
            {'hostPath': '/nail/etc/bop', 'containerPath': '/nail/etc/bop', 'mode': 'RO'},
            {'hostPath': '/nail/etc/boop', 'containerPath': '/nail/etc/boop', 'mode': 'RO'},
            {'hostPath': '/nail/tmp/noob', 'containerPath': '/nail/tmp/noob', 'mode': 'RO'},
        ]
    }, 'volumes.json')


@given('a working paasta cluster')
def working_paasta_cluster(context):
    return working_paasta_cluster_with_registry(context, 'docker.io')


@given('a working paasta cluster, with docker registry {docker_registry}')
def working_paasta_cluster_with_registry(context, docker_registry):
    """Adds a working marathon client and chronos client for the purposes of
    interacting with them in the test."""
    if not hasattr(context, 'marathon_client'):
        context.marathon_client, context.marathon_config, context.system_paasta_config = setup_marathon_client()
    else:
        paasta_print('Marathon connection already established')

    if not hasattr(context, 'chronos_client'):
        context.chronos_config = setup_chronos_config()
        context.chronos_client = setup_chronos_client()
        context.jobs = {}
    else:
        paasta_print('Chronos connection already established')

    if not hasattr(context, 'paasta_api_client'):
        context.paasta_api_client = setup_paasta_api_client()

    mesos_cli_config = _generate_mesos_cli_config(_get_zookeeper_connection_string('mesos-testcluster'))
    mesos_cli_config_filename = write_mesos_cli_config(mesos_cli_config)
    context.tag_version = 0

    write_etc_paasta(context, {'marathon_config': context.marathon_config}, 'marathon.json')
    write_etc_paasta(context, {'chronos_config': context.chronos_config}, 'chronos.json')
    write_etc_paasta(context, {
        "cluster": "testcluster",
        "zookeeper": "zk://zookeeper/mesos-testcluster",
        "docker_registry": docker_registry
    }, 'cluster.json')
    write_etc_paasta(context, {'log_writer': {'driver': "null"}}, 'logs.json')
    write_etc_paasta(context, {"sensu_host": None}, 'sensu.json')
    write_etc_paasta(context, {
        'volumes': [
            {'hostPath': '/nail/etc/beep', 'containerPath': '/nail/etc/beep', 'mode': 'RO'},
            {'hostPath': '/nail/etc/bop', 'containerPath': '/nail/etc/bop', 'mode': 'RO'},
            {'hostPath': '/nail/etc/boop', 'containerPath': '/nail/etc/boop', 'mode': 'RO'},
        ]
    }, 'volumes.json')
    write_etc_paasta(context, {
        'paasta_native': {
            'principal': 'paasta_native',
            'secret': 'secret4',
        }
    }, 'paasta_native.json')
    write_etc_paasta(context, {
        'mesos_config': {
            "path": mesos_cli_config_filename
        }
    }, 'mesos.json')


@given('we have yelpsoa-configs for the service "{service}" with {disabled} scheduled chronos instance "{instance}"')
def write_soa_dir_chronos_instance(context, service, disabled, instance):
    soa_dir = '/nail/etc/services/'
    desired_disabled = (disabled == 'disabled')
    if not os.path.exists(os.path.join(soa_dir, service)):
        os.makedirs(os.path.join(soa_dir, service))
    with open(os.path.join(soa_dir, service, 'chronos-%s.yaml' % context.cluster), 'w') as f:
        f.write(yaml.safe_dump({
            instance: {
                'schedule': 'R0/2000-01-01T16:20:00Z/PT60S',  # R0 prevents the job from being scheduled automatically
                'cmd': 'echo "Taking a nap..." && sleep 60m && echo "Nap time over, back to work"',
                'monitoring': {'team': 'fake_team'},
                'disabled': desired_disabled,
                'mem': 50,
                'disk': 10,
            }
        }))
    context.soa_dir = soa_dir


@given(('we have yelpsoa-configs for the service "{service}" with {disabled} dependent chronos instance'
        ' "{instance}" and parent "{parent}"'))
def write_soa_dir_dependent_chronos_instance(context, service, disabled, instance, parent):
    soa_dir = '/nail/etc/services/'
    desired_disabled = (disabled == 'disabled')
    if not os.path.exists(os.path.join(soa_dir, service)):
        os.makedirs(os.path.join(soa_dir, service))
    with open(os.path.join(soa_dir, service, 'chronos-%s.yaml' % context.cluster), 'w') as f:
        (_, parent_instance, _, __) = decompose_job_id(parent)
        f.write(yaml.safe_dump({
            parent_instance: {
                'schedule': 'R0/2000-01-01T16:20:00Z/PT60S',  # R0 prevents the job from being scheduled automatically
                'cmd': 'echo "Taking a nap..." && sleep 60m && echo "Nap time over, back to work"',
                'monitoring': {'team': 'fake_team'},
                'disabled': desired_disabled,
            },
            instance: {
                'parents': [parent],
                'cmd': 'echo "Taking a nap..." && sleep 1m && echo "Nap time over, back to work"',
                'monitoring': {'team': 'fake_team'},
                'disabled': desired_disabled,
            }
        }))
    context.soa_dir = soa_dir


@given('I have yelpsoa-configs for the marathon job "{job_id}"')
def write_soa_dir_marathon_job(context, job_id):
    (service, instance, _, __) = decompose_job_id(job_id)
    try:
        soa_dir = context.soa_dir
    except AttributeError:
        soa_dir = '/nail/etc/services/'
    if not os.path.exists(os.path.join(soa_dir, service)):
        os.makedirs(os.path.join(soa_dir, service))

    soa = {"%s" % instance: {
           'cpus': 0.1,
           'mem': 100}}
    if hasattr(context, "cmd"):
        soa[instance]['cmd'] = context.cmd
    with open(os.path.join(soa_dir, service, 'marathon-%s.yaml' % context.cluster), 'w') as f:
        f.write(yaml.safe_dump(soa))
    context.soa_dir = soa_dir


@given('we have yelpsoa-configs for native service "{job_id}"')
def write_soa_dir_native_service(context, job_id):
    (service, instance, _, __) = decompose_job_id(job_id)
    try:
        soa_dir = context.soa_dir
    except AttributeError:
        soa_dir = '/nail/etc/services/'
    if not os.path.exists(os.path.join(soa_dir, service)):
        os.makedirs(os.path.join(soa_dir, service))
    with open(os.path.join(soa_dir, service, 'paasta_native-%s.yaml' % context.cluster), 'w') as f:
        f.write(yaml.safe_dump({
            "%s" % instance: {
                'cpus': 0.1,
                'mem': 100,
                'cmd': '/bin/sleep 300',
            }
        }))
    context.soa_dir = soa_dir
    context.service = service
    context.instance = instance


@given('we load_paasta_native_job_config')
def call_load_paasta_native_job_config(context):
    context.new_config = native_scheduler.load_paasta_native_job_config(
        service=context.service,
        instance=context.instance,
        cluster=context.cluster,
        soa_dir=context.soa_dir,
    )


@given('we have a deployments.json for the service "{service}" with {disabled} instance '
       '"{csv_instances}" image "{image}"')
def write_soa_dir_deployments(context, service, disabled, csv_instances, image):
    if disabled == 'disabled':
        desired_state = 'stop'
    else:
        desired_state = 'start'

    if not os.path.exists(os.path.join(context.soa_dir, service)):
        os.makedirs(os.path.join(context.soa_dir, service))
    with open(os.path.join(context.soa_dir, service, 'deployments.json'), 'w') as dp:
        dp.write(json.dumps({
            'v1': {
                '%s:paasta-%s' % (service, utils.get_paasta_branch(context.cluster, instance)): {
                    'docker_image': image,
                    'desired_state': desired_state,
                }
                for instance in csv_instances.split(',')
            }
        }))


@given('we have a deployments.json for the service "{service}" with {disabled} instance "{csv_instance}"')
def write_soa_dir_deployments_default_image(context, service, disabled, csv_instance):
    write_soa_dir_deployments(context, service, disabled, csv_instance, 'test-image-foobar%d' % context.tag_version)


@when(('we set the "{field}" field of the {framework} config for service "{service}"'
       ' and instance "{instance}" to "{value}"'))
def modify_configs(context, field, framework, service, instance, value):
    soa_dir = context.soa_dir
    with open(os.path.join(soa_dir, service, "%s-%s.yaml" % (framework, context.cluster)), 'r+') as f:
        data = yaml.safe_load(f.read())
        data[instance][field] = value
        f.seek(0)
        f.write(yaml.safe_dump(data))
        f.truncate()


@when(('we set the "{field}" field of the {framework} config for service "{service}"'
       ' and instance "{instance}" to the integer {value:d}'))
def modify_configs_for_int(context, field, framework, service, instance, value):
    modify_configs(context, field, framework, service, instance, value)
