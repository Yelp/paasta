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
import socket
import time
from unittest import mock

from behave import given
from behave import then
from behave import when
from marathon import MarathonHttpError

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import marathon_tools
from paasta_tools import mesos_maintenance
from paasta_tools import setup_marathon_job
from paasta_tools.bounce_lib import get_happy_tasks


def which_id(context, which):
    if which == "new":
        return context.new_id
    elif which == "old":
        return context.old_app_config["id"]


@given(
    'a new {state} app to be deployed, with bounce strategy "{bounce_method}" and drain method "{drain_method}"'
)
def given_a_new_app_to_be_deployed(context, state, bounce_method, drain_method):
    given_a_new_app_to_be_deployed_constraints(
        context, state, bounce_method, drain_method, constraints=str([])
    )


@given(
    "a new {state} app to be deployed, "
    + 'with bounce strategy "{bounce_method}" '
    + 'and drain method "{drain_method}" '
    + "and host_port {host_port:d} "
    + "and {net} networking "
    + "and {instances:d} instances"
)
def given_a_new_app_to_be_deployed_host_port_net(
    context, state, bounce_method, drain_method, host_port, net, instances
):
    given_a_new_app_to_be_deployed_constraints(
        context=context,
        state=state,
        bounce_method=bounce_method,
        drain_method=drain_method,
        constraints=str([]),
        host_port=host_port,
        net=net,
        instances=instances,
    )


@given(
    "a new {state} app to be deployed, "
    + 'with bounce strategy "{bounce_method}" '
    + 'and drain method "{drain_method}" '
    + "and constraints {constraints}"
)
def given_a_new_app_to_be_deployed_constraints(
    context,
    state,
    bounce_method,
    drain_method,
    constraints,
    host_port=0,
    net="bridge",
    instances=2,
):
    constraints = eval(constraints)
    if state == "healthy":
        cmd = "/bin/true"
    elif state == "unhealthy":
        cmd = "/bin/false"
    else:
        return ValueError("can't start test app with unknown state %s", state)

    context.service = "bounce"
    context.instance = "test1"
    context.new_id = "bounce.test1.newapp.confighash"
    context.new_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service=context.service,
        cluster=context.cluster,
        instance=context.instance,
        config_dict={
            "cmd": "/bin/sleep 300",
            "instances": instances,
            "healthcheck_mode": "cmd",
            "healthcheck_cmd": cmd,
            "bounce_method": str(bounce_method),
            "drain_method": str(drain_method),
            "cpus": 0.1,
            "mem": 100,
            "disk": 10,
            "constraints": constraints,
            "host_port": host_port,
            "net": net,
        },
        branch_dict={
            "docker_image": "busybox",
            "desired_state": "start",
            "force_bounce": None,
        },
    )
    context.current_client = context.marathon_clients.get_current_client_for_service(
        context.new_marathon_service_config
    )


@given("an old app to be destroyed")
def given_an_old_app_to_be_destroyed(context):
    given_an_old_app_to_be_destroyed_constraints(context, str([]))


@given("an old app to be destroyed with constraints {constraints}")
def given_an_old_app_to_be_destroyed_constraints(context, constraints):
    constraints = eval(constraints)
    old_app_name = "bounce.test1.oldapp.confighash"
    context.old_ids = [old_app_name]
    context.old_app_config = {
        "id": old_app_name,
        "cmd": "/bin/sleep 300",
        "instances": 2,
        "container": {
            "type": "DOCKER",
            "docker": {"network": "BRIDGE", "image": "busybox"},
        },
        "backoff_seconds": 1,
        "backoff_factor": 1,
        "constraints": constraints,
    }

    bounce_lib.create_marathon_app(
        old_app_name, context.old_app_config, context.current_client
    )


@when("there are exactly {num:d} {which} {state} tasks")
def when_there_are_exactly_num_which_tasks(context, num, which, state):
    there_are_num_which_tasks(context, num, which, state, True)


@when("there are {num:d} {which} {state} tasks")
def when_there_are_num_which_tasks(context, num, which, state):
    there_are_num_which_tasks(context, num, which, state, False)


def there_are_num_which_tasks(context, num, which, state, exact):
    context.max_tasks = num
    app_id = which_id(context, which)

    # 180 * 0.5 = 90 seconds
    for _ in range(180):
        app = context.current_client.get_app(app_id, embed_tasks=True)
        happy_tasks = get_happy_tasks(
            app, context.service, "fake_nerve_ns", context.system_paasta_config
        )
        happy_count = len(happy_tasks)
        if state == "healthy":
            if exact:
                if happy_count == context.max_tasks:
                    return
            else:
                if happy_count >= context.max_tasks:
                    return
        elif state == "unhealthy":
            if exact:
                if len(app.tasks) - happy_count == context.max_tasks:
                    return
            else:
                if len(app.tasks) - happy_count >= context.max_tasks:
                    return
        time.sleep(0.5)
    raise Exception(
        "timed out waiting for %d %s tasks on %s; there are %d"
        % (context.max_tasks, state, app_id, len(app.tasks))
    )


@when("setup_service is initiated")
def when_setup_service_initiated(context):
    with mock.patch(
        "paasta_tools.bounce_lib.get_happy_tasks",
        autospec=True,
        # Wrap function call so we can select a subset of tasks or test
        # intermediate steps, like when an app is not completely up
        side_effect=lambda app, _, __, ___, **kwargs: get_happy_tasks(
            app, context.service, "fake_nerve_ns", context.system_paasta_config
        )[: context.max_tasks],
    ), mock.patch(
        "paasta_tools.bounce_lib.bounce_lock_zookeeper", autospec=True
    ), mock.patch(
        "paasta_tools.bounce_lib.time.sleep", autospec=True
    ), mock.patch(
        "paasta_tools.setup_marathon_job.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config, mock.patch(
        "paasta_tools.setup_marathon_job._log", autospec=True
    ), mock.patch(
        "paasta_tools.marathon_tools.get_config_hash",
        autospec=True,
        return_value="confighash",
    ), mock.patch(
        "paasta_tools.marathon_tools.get_code_sha_from_dockerurl",
        autospec=True,
        return_value="newapp",
    ), mock.patch(
        "paasta_tools.utils.InstanceConfig.get_docker_url",
        autospec=True,
        return_value="busybox",
    ), mock.patch(
        "paasta_tools.mesos_maintenance.get_principal", autospec=True
    ) as mock_get_principal, mock.patch(
        "paasta_tools.mesos_maintenance.get_secret", autospec=True
    ) as mock_get_secret, mock.patch(
        "paasta_tools.mesos_maintenance.get_mesos_leader",
        autospec=True,
        return_value="mesosmaster",
    ):
        credentials = mesos_maintenance.load_credentials(
            mesos_secrets="/etc/mesos-slave-secret"
        )
        mock_get_principal.return_value = credentials.principal
        mock_get_secret.return_value = credentials.secret
        mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(
            return_value=context.cluster
        )
        # 120 * 0.5 = 60 seconds
        for _ in range(120):
            try:
                marathon_apps_with_clients = marathon_tools.get_marathon_apps_with_clients(
                    clients=context.marathon_clients.get_all_clients(), embed_tasks=True
                )
                (code, message, bounce_again) = setup_marathon_job.setup_service(
                    service=context.service,
                    instance=context.instance,
                    clients=context.marathon_clients,
                    marathon_apps_with_clients=marathon_apps_with_clients,
                    job_config=context.new_marathon_service_config,
                    soa_dir="/nail/etc/services",
                )
                assert code == 0, message
                return
            except MarathonHttpError:
                time.sleep(0.5)
        raise Exception(
            "Unable to acquire app lock for setup_marathon_job.setup_service"
        )


@when("the {which} app is down to {num} instances")
def when_the_which_app_is_down_to_num_instances(context, which, num):
    app_id = which_id(context, which)
    while True:
        tasks = context.current_client.list_tasks(app_id)
        if len([t for t in tasks if t.started_at]) <= int(num):
            return
        time.sleep(0.5)


@then("the {which} app should be running")
def then_the_which_app_should_be_running(context, which):
    assert (
        marathon_tools.is_app_id_running(
            which_id(context, which), context.current_client
        )
        is True
    )


@then("the {which} app should be configured to have {num} instances")
def then_the_which_app_should_be_configured_to_have_num_instances(
    context, which, num, retries=10
):
    app_id = which_id(context, which)

    for _ in range(retries):
        app = context.current_client.get_app(app_id)
        if app.instances == int(num):
            return
        time.sleep(0.5)

    raise ValueError(
        "Expected there to be %d instances, but there were %d"
        % (int(num), app.instances)
    )


@then("the {which} app should be gone")
def then_the_which_app_should_be_gone(context, which):
    assert (
        marathon_tools.is_app_id_running(
            which_id(context, which), context.current_client
        )
        is False
    )


@when("we wait a bit for the {which} app to disappear")
def and_we_wait_a_bit_for_the_app_to_disappear(context, which):
    """ Marathon will not make the app disappear until after all the tasks have died
    https://github.com/mesosphere/marathon/issues/1431 """
    for _ in range(10):
        if (
            marathon_tools.is_app_id_running(
                which_id(context, which), context.current_client
            )
            is True
        ):
            time.sleep(0.5)
        else:
            return True
    # It better not be running by now!
    assert (
        marathon_tools.is_app_id_running(
            which_id(context, which), context.current_client
        )
        is False
    )


@when("a task has drained")
def when_a_task_has_drained(context):
    """Tell the TestDrainMethod to mark a task as safe to kill.

    Normal drain methods, like hacheck, require waiting for something to happen in the background. The bounce code can
    cause a task to go from up -> draining, but the draining->drained transition normally happens outside of Paasta.
    With TestDrainMethod, we can control the draining->drained transition to emulate that external code, and that's what
    this step does.
    """
    drain_lib.TestDrainMethod.mark_arbitrary_task_as_safe_to_kill()


@then("it should be discoverable on port {host_port:d}")
def should_be_discoverable_on_port(context, host_port):
    all_discovered = {}
    for slave_ip in socket.gethostbyname_ex("mesosslave")[2]:
        with mock.patch(
            "paasta_tools.mesos_tools.socket.getfqdn",
            return_value=slave_ip,
            autospec=True,
        ):
            discovered = marathon_tools.marathon_services_running_here()
            all_discovered[slave_ip] = discovered
            if discovered == [("bounce", "test1", host_port)]:
                return

    raise Exception(
        "Did not find bounce.test1 in marathon_services_running_here for any of our slaves: %r",
        all_discovered,
    )


@then("it should be discoverable on any port")
def should_be_discoverable_on_any_port(context):
    return should_be_discoverable_on_port(context, mock.ANY)
