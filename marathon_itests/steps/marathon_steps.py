import sys

from behave import given, when, then
import mock

from itest_utils import get_service_connection_string
sys.path.append('../')
import paasta_tools
from paasta_tools import marathon_tools
from paasta_tools import utils


@given('a working marathon instance')
def working_marathon(context):
    """Adds a working marathon client as context.client for the purposes of
    interacting with it in the test."""
    if not hasattr(context, 'client'):
        marathon_connection_string = "http://%s" % \
            get_service_connection_string('marathon')
        zk_connection_string = "zk://%s/mesos-testcluster" % \
            get_service_connection_string('zookeeper')
        marathon_config = marathon_tools.MarathonConfig({
            'url': marathon_connection_string,
            'user': None,
            'password': None,
        }, '/some_fake_path_to_marathon.json')
        context.client = marathon_tools.get_marathon_client(marathon_config['url'], marathon_config['user'],
                                                            marathon_config['password'])
        context.marathon_config = marathon_config
        system_paasta_config = utils.SystemPaastaConfig({
            'cluster': 'testcluster',
            'docker_volumes': [],
            'docker_registry': u'docker-dev.yelpcorp.com',
            'zookeeper': zk_connection_string
        }, '/some_fake_path_to_config_dir/')
        context.system_paasta_config = system_paasta_config
    else:
        print "Marathon connection already established"


@when(u'we create a trivial new app')
def create_trivial_new_app(context):
    trivial_app_config = {
        'id': 'behavetest',
        'cmd': '/bin/true',
    }
    with mock.patch('paasta_tools.bounce_lib.create_app_lock'):
        paasta_tools.bounce_lib.create_marathon_app('behavetest', trivial_app_config, context.client)


@then(u'we should see it running via the marathon api')
def see_it_running(context):
    assert 'behavetest' in paasta_tools.marathon_tools.list_all_marathon_app_ids(context.client)
    assert context.client.get_app('/behavetest')
