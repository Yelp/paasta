# Copyright 2015 Yelp Inc.
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
import contextlib
import time

import mock
from behave import given
from behave import then
from behave import when
from marathon import MarathonHttpError

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import marathon_tools
from paasta_tools import setup_marathon_job
from paasta_tools.bounce_lib import get_happy_tasks


def which_id(context, which):
    config = {
        'new': context.new_config,
        'old': context.old_app_config,
    }[which]
    return config['id']


@given(u'a new {state} app to be deployed')
def given_a_new_app_to_be_deployed(context, state):
    if state == "healthy":
        cmd = "/bin/true"
    elif state == "unhealthy":
        cmd = "/bin/false"
    else:
        return ValueError("can't start test app with unknown state %s", state)

    context.service = 'bounce'
    context.instance = 'test1'
    context.new_config = {
        'id': 'bounce.test1.newapp.confighash',
        'cmd': '/bin/sleep 300',
        'instances': 2,
        'backoff_seconds': 1,
        'backoff_factor': 1,
        'health_checks': [
            {
                "protocol": "COMMAND",
                "command": {"value": cmd}
            }
        ]
    }


@given(u'an old app to be destroyed')
def given_an_old_app_to_be_destroyed(context):
    old_app_name = "bounce.test1.oldapp.confighash"
    context.old_ids = [old_app_name]
    context.old_app_config = {
        'id': old_app_name,
        'cmd': '/bin/sleep 300',
        'instances': 2,
        'backoff_seconds': 1,
        'backoff_factor': 1,
    }
    with contextlib.nested(
        mock.patch('paasta_tools.bounce_lib.create_app_lock'),
    ) as (
        mock_creat_app_lock,
    ):
        bounce_lib.create_marathon_app(old_app_name, context.old_app_config, context.marathon_client)


@when(u'there are {num} {which} {state} tasks')
def when_there_are_num_which_tasks(context, num, which, state):
    context.max_tasks = int(num)
    app_id = which_id(context, which)

    # 120 * 0.5 = 60 seconds
    for _ in xrange(120):
        app = context.marathon_client.get_app(app_id, embed_tasks=True)
        happy_count = len(get_happy_tasks(app, context.service, "fake_nerve_ns", context.system_paasta_config))
        if state == "healthy":
            if happy_count >= context.max_tasks:
                return
        elif state == "unhealthy":
            if len(app.tasks) - happy_count >= context.max_tasks:
                return
        time.sleep(0.5)
    raise Exception("timed out waiting for %d %s tasks on %s; there are %d" %
                    (context.max_tasks, state, app_id, app.tasks))


@when(u'deploy_service with bounce strategy "{bounce_method}" and drain method "{drain_method}" is initiated')
def when_deploy_service_initiated(context, bounce_method, drain_method):
    with contextlib.nested(
        mock.patch(
            'paasta_tools.bounce_lib.get_happy_tasks',
            autospec=True,
            # Wrap function call so we can select a subset of tasks or test
            # intermediate steps, like when an app is not completely up
            side_effect=lambda app, _, __, ___, **kwargs: get_happy_tasks(
                app, context.service, "fake_nerve_ns", context.system_paasta_config)[:context.max_tasks],
        ),
        mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', autospec=True),
        mock.patch('paasta_tools.bounce_lib.create_app_lock', autospec=True),
        mock.patch('paasta_tools.bounce_lib.time.sleep', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
    ) as (
        _,
        _,
        _,
        _,
        mock_load_system_paasta_config,
        _,
    ):
        mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value=context.cluster)
        # 120 * 0.5 = 60 seconds
        for _ in xrange(120):
            try:
                setup_marathon_job.deploy_service(
                    service=context.service,
                    instance=context.instance,
                    marathon_jobid=context.new_config['id'],
                    config=context.new_config,
                    client=context.marathon_client,
                    bounce_method=bounce_method,
                    drain_method_name=drain_method,
                    drain_method_params={},
                    nerve_ns=context.instance,
                    bounce_health_params={},
                    soa_dir=None,
                )
                return
            except MarathonHttpError:
                time.sleep(0.5)
        raise Exception("Unable to qcuiqre app lock for setup_marathon_job.deploy_service")


@when(u'the {which} app is down to {num} instances')
def when_the_which_app_is_down_to_num_instances(context, which, num):
    app_id = which_id(context, which)
    while True:
        tasks = context.marathon_client.list_tasks(app_id)
        if len([t for t in tasks if t.started_at]) <= int(num):
            return
        time.sleep(0.5)


@then(u'the {which} app should be running')
def then_the_which_app_should_be_running(context, which):
    assert marathon_tools.is_app_id_running(which_id(context, which), context.marathon_client) is True


@then(u'the {which} app should be configured to have {num} instances')
def then_the_which_app_should_be_configured_to_have_num_instances(context, which, num, retries=10):
    app_id = which_id(context, which)

    for _ in xrange(retries):
        app = context.marathon_client.get_app(app_id)
        if app.instances == int(num):
            return
        time.sleep(0.5)

    raise ValueError("Expected there to be %d instances, but there were %d" % (int(num), app.instances))


@then(u'the {which} app should be gone')
def then_the_which_app_should_be_gone(context, which):
    assert marathon_tools.is_app_id_running(which_id(context, which), context.marathon_client) is False


@when(u'we wait a bit for the {which} app to disappear')
def and_we_wait_a_bit_for_the_app_to_disappear(context, which):
    """ Marathon will not make the app disappear until after all the tasks have died
    https://github.com/mesosphere/marathon/issues/1431 """
    for _ in xrange(10):
        if marathon_tools.is_app_id_running(which_id(context, which), context.marathon_client) is True:
            time.sleep(0.5)
        else:
            return True
    # It better not be running by now!
    assert marathon_tools.is_app_id_running(which_id(context, which), context.marathon_client) is False


@when(u'a task has drained')
def when_a_task_has_drained(context):
    """Tell the TestDrainMethod to mark a task as safe to kill.

    Normal drain methods, like hacheck, require waiting for something to happen in the background. The bounce code can
    cause a task to go from up -> draining, but the draining->drained transition normally happens outside of Paasta.
    With TestDrainMethod, we can control the draining->drained transition to emulate that external code, and that's what
    this step does.
    """
    drain_lib.TestDrainMethod.mark_arbitrary_task_as_safe_to_kill()
