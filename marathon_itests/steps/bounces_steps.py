import contextlib
import mock
import time

from paasta_tools import bounce_lib
from paasta_tools import marathon_tools
from paasta_tools import setup_marathon_job


def which_id(context, which):
    config = {
        'new': context.new_config,
        'old': context.old_app_config,
    }[which]
    return config['id']


@given(u'a new app to be deployed')
def step_impl(context):
    context.service_name = 'bounce'
    context.instance_name = 'test1'
    context.new_config = {
        'id': 'bounce.test1.newapp',
        'cmd': '/bin/sleep 300',
        'instances': 2,
        'backoff_seconds': 0.1,
        'backoff_factor': 1,
    }


@given(u'an old app to be destroyed')
def step_impl(context):
    old_app_name = "bounce.test1.oldapp"
    context.old_ids = [old_app_name]
    context.old_app_config = {
        'id': old_app_name,
        'cmd': '/bin/sleep 300',
        'instances': 2,
        'backoff_seconds': 0.1,
        'backoff_factor': 1,
    }
    with contextlib.nested(
        mock.patch('paasta_tools.bounce_lib.create_app_lock'),
    ) as (
        mock_creat_app_lock,
    ):
        bounce_lib.create_marathon_app(old_app_name, context.old_app_config, context.client)


@when(u'there are {num} {which} tasks')
def step_impl(context, num, which):
    context.max_happy_tasks = int(num)
    app_id = which_id(context, which)

    while True:
        tasks = context.client.list_tasks(app_id)
        if len([t for t in tasks if t.started_at]) >= context.max_happy_tasks:
            return
        time.sleep(0.5)



@when(u'deploy_service with bounce strategy "{bounce_method}" is initiated')
def step_impl(context, bounce_method):
    with contextlib.nested(
        mock.patch(
            'paasta_tools.bounce_lib.get_happy_tasks',
            autospec=True,
            side_effect=lambda t, _, __, **kwargs: t[:context.max_happy_tasks],
        ),
        mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', autospec=True),
        mock.patch('paasta_tools.bounce_lib.create_app_lock', autospec=True),
        mock.patch('paasta_tools.bounce_lib.time.sleep', autospec=True),
    ):
        setup_marathon_job.deploy_service(
            context.service_name,
            context.instance_name,
            context.new_config['id'],
            context.new_config,
            context.client,
            bounce_method,
            context.instance_name,
            {},
        )


@when(u'the {which} app is down to {num} instances')
def step_impl(context, which, num):
    app_id = which_id(context, which)
    while True:
        tasks = context.client.list_tasks(app_id)
        if len([t for t in tasks if t.started_at]) <= int(num):
            return
        time.sleep(0.5)


@then(u'the {which} app should be running')
def step_impl(context, which):
    assert marathon_tools.is_app_id_running(which_id(context, which), context.client) is True


@then(u'the {which} app should be configured to have {num} instances')
def check_instances(context, which, num, retries=10):
    app_id = which_id(context, which)

    for _ in xrange(retries):
        app = context.client.get_app(app_id)
        if app.instances == int(num):
            return
        time.sleep(0.5)

    raise ValueError("Expected there to be %d instances, but there were %d", int(num), app.instances)


@then(u'the {which} app should be gone')
def step_impl(context, which):
    assert marathon_tools.is_app_id_running(which_id(context, which), context.client) is False
