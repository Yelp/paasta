import os
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp

from behave import given
import chronos
import json
import yaml

from itest_utils import get_service_connection_string
from paasta_tools import marathon_tools
from paasta_tools import utils
from paasta_tools import chronos_tools


def _get_marathon_connection_string():
    return 'http://%s' % get_service_connection_string('marathon')


def _get_chronos_connection_string():
    return 'http://%s' % get_service_connection_string('chronos')


def _get_zookeeper_connection_string(chroot):
    return 'zk://%s/%s' % (get_service_connection_string('zookeeper'), chroot)


def setup_marathon_client():
    marathon_connection_string = _get_marathon_connection_string()
    zk_connection_string = _get_zookeeper_connection_string('mesos-testcluster')
    marathon_config = marathon_tools.MarathonConfig({
        'url': marathon_connection_string,
        'user': None,
        'password': None,
    }, '/some_fake_path_to_marathon.json')
    client = marathon_tools.get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                                marathon_config.get_password())
    system_paasta_config = utils.SystemPaastaConfig({
        'cluster': 'testcluster',
        'docker_volumes': [],
        'docker_registry': u'docker-dev.yelpcorp.com',
        'zookeeper': zk_connection_string
    }, '/some_fake_path_to_config_dir/')
    return (client, marathon_config, system_paasta_config)


def setup_chronos_client():
    connection_string = get_service_connection_string('chronos')
    return chronos.connect(connection_string)


def setup_chronos_config():
    chronos_connection_string = _get_chronos_connection_string()
    chronos_config = chronos_tools.ChronosConfig({
        'user': None,
        'password': None,
        'url': [chronos_connection_string],
    }, '/some_fake_path_to_chronos.json')
    return chronos_config


def _generate_mesos_cli_config(zk_host_and_port):
    config = {
        'profile': 'default',
        'default': {
            'master': zk_host_and_port,
            'log_level': 'warning',
            'log_file': 'None',
        }
    }
    return config


def write_mesos_cli_config(config):
    mesos_cli_config_file = NamedTemporaryFile(delete=False)
    mesos_cli_config_file.write(json.dumps(config))
    mesos_cli_config_file.close()
    return mesos_cli_config_file.name


def write_etc_paasta(config, filename):
    paasta_dir = '/etc/paasta'
    if not os.path.exists(paasta_dir):
        os.makedirs(paasta_dir)
    with open(os.path.join(paasta_dir, filename), 'w') as f:
        f.write(json.dumps(config))


@given(u'a working paasta cluster')
def working_paasta_cluster(context):
    """Adds a working marathon client and chronos client for the purposes of
    interacting with them in the test."""
    if not hasattr(context, 'marathon_client'):
        context.marathon_client, context.marathon_config, context.system_paasta_config = setup_marathon_client()
    else:
        print 'Marathon connection already established'

    if not hasattr(context, 'chronos_client'):
        context.chronos_config = setup_chronos_config()
        context.chronos_client = setup_chronos_client()
    else:
        print 'Chronos connection already established'

    mesos_cli_config = _generate_mesos_cli_config(_get_zookeeper_connection_string('mesos-testcluster'))
    context.mesos_cli_config_filename = write_mesos_cli_config(mesos_cli_config)
    context.tag_version = 0
    write_etc_paasta(context.marathon_config, 'marathon.json')
    write_etc_paasta(context.chronos_config, 'chronos.json')
    write_etc_paasta({
        "cluster": "testcluster",
        "zookeeper": "zk://fake",
        "docker_registry": "fake.com"
    }, 'cluster.json')
    write_etc_paasta({
        'volumes': [
            {'hostPath': u'/nail/etc/beep', 'containerPath': '/nail/etc/beep', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/bop', 'containerPath': '/nail/etc/bop', 'mode': 'RO'},
            {'hostPath': u'/nail/etc/boop', 'containerPath': '/nail/etc/boop', 'mode': 'RO'},
        ]
    }, 'volumes.json')


@given(u'I have yelpsoa-configs for the service "{service_name}" with {disabled} {app} instance "{instance_name}"')
def write_soa_dir_chronos_instance(context, service_name, disabled, app, instance_name):
    soa_dir = mkdtemp()
    desired_disabled = (disabled == 'disabled')
    if not os.path.exists(os.path.join(soa_dir, service_name)):
        os.makedirs(os.path.join(soa_dir, service_name))
    with open(os.path.join(soa_dir, service_name, '%s-%s.yaml' % (app, context.cluster)), 'w') as f:
        f.write(yaml.dump({
            "%s" % instance_name: {
                'schedule': 'R/2000-01-01T16:20:00Z/PT60S',
                'command': 'echo "Taking a nap..." && sleep 1m && echo "Nap time over, back to work"',
                'monitoring': {'team': 'fake_team'},
                'disabled': desired_disabled,
            }
        }))
    context.soa_dir = soa_dir


@given(u'I have a deployments.json for the service "{service_name}" with {disabled} instance "{instance_name}"')
def write_soa_dir_chronos_deployments(context, service_name, disabled, instance_name):
    if disabled == 'disabled':
        desired_state = 'stop'
    else:
        desired_state = 'start'

    with open(os.path.join(context.soa_dir, service_name, 'deployments.json'), 'w') as dp:
        dp.write(json.dumps({
            'v1': {
                '%s:%s' % (service_name, utils.get_default_branch(context.cluster, instance_name)): {
                    'docker_image': 'test-image-foobar%d' % context.tag_version,
                    'desired_state': desired_state,
                }
            }
        }))
