import contextlib
import mock

from paasta_tools import bounce_lib
from paasta_tools import marathon_tools


@given(u'a new app to be deployed')
def step_impl(context):
    context.service_name = 'bouncetest1'
    context.instance_name = 'newapp'
    context.new_config = {
        'id': 'bouncetest1-newapp',
        'cmd': '/bin/true',
    }


@given(u'an old app to be destroyed')
def step_impl(context):
    old_app_name = "bouncetest1-oldapp"
    context.old_ids = [old_app_name]
    old_app_config = {
        'id': old_app_name,
        'cmd': '/bin/true',
    }
    with contextlib.nested(
        mock.patch('paasta_tools.bounce_lib.create_app_lock'),
    ) as (
        mock_creat_app_lock,
    ):
        bounce_lib.create_marathon_app(old_app_name, old_app_config, context.client)


@when(u'an upthendown_bounce is intitiated')
def step_impl(context):
     with contextlib.nested(
        mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper'),
        mock.patch('paasta_tools.bounce_lib.create_app_lock'),
        mock.patch('paasta_tools.bounce_lib.time.sleep'),
    ) as (
        mock_bounce_lock_zookeeper,
        mock_create_app_lock,
        mock_sleep,
    ):
       bounce_lib.upthendown_bounce(context.service_name, context.instance_name,
                                    context.old_ids, context.new_config, context.client)


@then(u'the new app should be running')
def step_impl(context):
    assert marathon_tools.is_app_id_running(context.new_config['id'],
                                            context.client) is True


@then(u'the old app should be gone')
def step_impl(context):
    for old_app in context.old_ids:
        assert marathon_tools.is_app_id_running(old_app, context.client) is False
