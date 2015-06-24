from tempfile import NamedTemporaryFile

from behave import given
import json

from itest_utils import get_service_connection_string
from paasta_tools import marathon_tools
from paasta_tools import utils


def _get_marathon_connection_string():
    return 'http://%s' % get_service_connection_string('marathon')


def _get_zookeeper_connection_string(chroot):
    return 'zk://%s/%s' % (get_service_connection_string('zookeeper'), chroot)


def setup_marathon_client():
    marathon_connection_string = _get_marathon_connection_string()
    zk_connection_string  = _get_zookeeper_connection_string('mesos-testcluster')
    marathon_config = marathon_tools.MarathonConfig({
        'url': marathon_connection_string,
        'user': None,
        'password': None,
    }, '/some_fake_path_to_marathon.json')
    client = marathon_tools.get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                                        marathon_config.get_password())
    marathon_config = marathon_config
    system_paasta_config = utils.SystemPaastaConfig({
        'cluster': 'testcluster',
        'docker_volumes': [],
        'docker_registry': u'docker-dev.yelpcorp.com',
        'zookeeper': zk_connection_string
    }, '/some_fake_path_to_config_dir/')
    system_paasta_config = system_paasta_config
    return (client, marathon_config, system_paasta_config)


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


@given('a working paasta cluster')
def working_paasta_cluster(context):
    """Adds a working marathon client as context.client for the purposes of
    interacting with it in the test."""
    if not hasattr(context, 'client'):
        context.client, context.marathon_config, context.system_paasta_config = setup_marathon_client()
    else:
        print 'Marathon connection already established'
    mesos_cli_config = _generate_mesos_cli_config(_get_zookeeper_connection_string('mesos-testcluster'))
    context.mesos_cli_config_filename = write_mesos_cli_config(mesos_cli_config)
