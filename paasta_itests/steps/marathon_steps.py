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
import time
from tempfile import NamedTemporaryFile
from unittest import mock

import itest_utils
from behave import given
from behave import then
from behave import when

import paasta_tools
from paasta_tools import marathon_tools
from paasta_tools.utils import _run
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import load_system_paasta_config


APP_ID = "test--marathon--app.instance.git01234567.configabcdef01"


@when("we create a trivial marathon app")
def create_trivial_marathon_app(context):
    app_config = {
        "id": APP_ID,
        "cmd": "/bin/sleep 30",
        "container": {
            "type": "DOCKER",
            "docker": {"network": "BRIDGE", "image": "busybox"},
        },
        "instances": 3,
        "constraints": [["hostname", "UNIQUE"]],
    }
    paasta_tools.bounce_lib.create_marathon_app(
        app_config["id"], app_config, context.marathon_clients.current[0]
    )


@then("we should see it running in marathon")
def list_marathon_apps_has_trivial_app(context):
    actual = paasta_tools.marathon_tools.list_all_marathon_app_ids(
        context.marathon_clients.current[0]
    )
    assert APP_ID in actual
    assert context.marathon_clients.current[0].get_app("/%s" % APP_ID)


@then("it should show up in marathon_services_running_here")
def marathon_services_running_here_works(context):
    with mock.patch(
        "paasta_tools.mesos_tools.socket.getfqdn",
        return_value="mesosslave",
        autospec=True,
    ):
        discovered = paasta_tools.marathon_tools.marathon_services_running_here()
        assert discovered == [("test_marathon_app", "instance", mock.ANY)]


@when("the task has started")
def when_the_task_has_started(context):
    # 120 * 0.5 = 60 seconds
    for _ in range(120):
        app = context.marathon_clients.current[0].get_app(APP_ID)
        happy_count = app.tasks_running
        if happy_count >= 3:
            return
        time.sleep(0.5)

    raise Exception("timed out waiting for task to start")


@when('we run the marathon app "{job_id}" with "{instances:d}" instances')
def run_marathon_app(context, job_id, instances):
    (service, instance, _, __) = decompose_job_id(job_id)
    job_config = marathon_tools.load_marathon_service_config(
        service=service,
        instance=instance,
        cluster=load_system_paasta_config().get_cluster(),
        soa_dir=context.soa_dir,
    )
    app_id = job_config.format_marathon_app_dict()["id"]
    app_config = {
        "id": app_id,
        "cmd": "/bin/sleep 1m",
        "container": {
            "type": "DOCKER",
            "docker": {"network": "BRIDGE", "image": "busybox"},
        },
        "instances": instances,
        "constraints": [["hostname", "UNIQUE"]],
    }
    paasta_tools.bounce_lib.create_marathon_app(
        app_id=app_id,
        config=app_config,
        client=context.marathon_clients.get_current_client_for_service(job_config),
    )


@given('a capacity check overrides file with contents "{contents}"')
def write_overrides_file(context, contents):
    with NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(contents)
        context.overridefile = f.name


@then(
    'capacity_check "{check_type}" --crit "{crit:d}" --warn "{warn:d}" should return "{status}" with code "{code:d}"'
)
def capacity_check_status_crit_warn(context, check_type, crit, warn, status, code):
    print(check_type, crit, warn)
    cmd = f"../paasta_tools/monitoring/check_capacity.py {check_type} --crit {crit} --warn {warn}"
    print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    print(output)
    assert exit_code == code
    assert status in output


@then('capacity_check "{check_type}" should return "{status}" with code "{code:d}"')
def capacity_check_type_status(context, check_type, status, code):
    cmd = "../paasta_tools/monitoring/check_capacity.py %s" % check_type
    print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    print(output)
    assert exit_code == code
    assert status in output


@then(
    'capacity_check with override file "{check_type}" and attributes "{attrs}" '
    'should return "{status}" with code "{code:d}"'
)
def capacity_check_type_status_overrides(context, check_type, attrs, status, code):
    cmd = "../paasta_tools/monitoring/check_capacity.py {} --overrides {} --attributes {}".format(
        check_type, context.overridefile, attrs
    )
    print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    print(output)
    assert exit_code == code
    assert status in output


@when('we wait for "{job_id}" to launch exactly {task_count:d} tasks')
def wait_launch_tasks(context, job_id, task_count):
    (service, instance, _, __) = decompose_job_id(job_id)
    job_config = marathon_tools.load_marathon_service_config(
        service=service,
        instance=instance,
        cluster=load_system_paasta_config().get_cluster(),
        soa_dir=context.soa_dir,
    )
    app_id = job_config.format_marathon_app_dict()["id"]
    client = context.marathon_clients.get_current_client_for_service(job_config)
    itest_utils.wait_for_app_to_launch_tasks(
        client, app_id, task_count, exact_matches_only=True
    )
