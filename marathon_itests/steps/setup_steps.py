from behave import given

from itest_utils import get_service_connection_string
from paasta_tools import marathon_tools
from paasta_tools import utils


def setup_marathon_client():
    marathon_connection_string = "http://%s" % \
        get_service_connection_string('marathon')
    zk_connection_string = "zk://%s/mesos-testcluster" % \
        get_service_connection_string('zookeeper')
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


@given('a working paasta cluster')
def working_paasta_cluster(context):
    """Adds a working marathon client as context.client for the purposes of
    interacting with it in the test."""
    if not hasattr(context, 'client'):
        context.client, context.marathon_config, context.system_paasta_config = setup_marathon_client()
    else:
        print "Marathon connection already established"
