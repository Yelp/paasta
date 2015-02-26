import sys
import mock

from fig.cli import command

sys.path.append('../')
import paasta_tools
from paasta_tools import setup_marathon_job
from paasta_tools import marathon_tools


def get_service_connection_string(service_name, port):
    """Given a desired internal port and container name this function returns
    the host and ephemeral port that you need to use to connect to. For example
    if you are spinning up a 'web' container that inside listens on 80, this
    function would return 0.0.0.0:23493 or whatever ephemeral forwarded port
    it has from fig"""
    cmd = command.Command()
    project = cmd.get_project(cmd.get_config_path())
    service = project.get_service(service_name)
    return service.get_container().get_local_port(port)


@given('a working marathon instance')
def working_marathon(context):
    """Adds a working marathon client as context.client for the purposes of
    interacting with it in the test."""
    if not hasattr(context, 'client'):
        marathon_connection_string = "http://%s" % get_service_connection_string('marathon', 8080)
        zk_connection_string = "zk://%s/meoss-testcluster" % get_service_connection_string('zookeeper', 2181)
        marathon_config = marathon_tools.MarathonConfig({
          'docker_volumes': [],
          'url': marathon_connection_string,
          'zk_hosts': zk_connection_string,
          'cluster': 'testcluster',
          'user': None,
          'pass': None,
          'docker_registry': u'docker-dev.yelpcorp.com'
        })
        context.client = marathon_tools.get_marathon_client(marathon_config['url'], marathon_config['user'],
                                     marathon_config['pass'])
        context.marathon_config = marathon_config
    else:
        print "Marathon connection already established"

@when(u'we create a trivial new app')
def step_impl(context):
    trivial_app_config = {
        'id': 'behavetest',
        'cmd': '/bin/true',
    }
    with mock.patch('paasta_tools.bounce_lib.create_app_lock'):
        paasta_tools.bounce_lib.create_marathon_app('behavetest', trivial_app_config, context.client)


@then(u'we should see it running via the marathon api')
def step_impl(context):
    assert 'behavetest' in paasta_tools.marathon_tools.list_all_marathon_app_ids(context.client)
    assert context.client.get_app('/behavetest')
