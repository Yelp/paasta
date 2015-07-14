import sys

from behave import when, then
import mock

sys.path.append('../')
import paasta_tools


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
