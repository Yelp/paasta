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
import re
import time
from tempfile import NamedTemporaryFile

import itest_utils
import requests_cache
from behave import given
from behave import then
from behave import when

import paasta_tools
from paasta_tools import marathon_serviceinit
from paasta_tools import marathon_tools
from paasta_tools.utils import _run
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


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


@then(
    'marathon_serviceinit status_marathon_job should return "{status}" for "{job_id}"'
)
def status_marathon_job(context, status, job_id):
    normal_instance_count = 1
    (service, instance, _, __) = decompose_job_id(job_id)
    job_config = marathon_tools.load_marathon_service_config(
        service=service,
        instance=instance,
        cluster=load_system_paasta_config().get_cluster(),
        soa_dir=context.soa_dir,
    )
    app_id = job_config.format_marathon_app_dict()["id"]

    with requests_cache.disabled():
        tasks, output = marathon_serviceinit.status_marathon_job(
            service=service,
            instance=instance,
            cluster=load_system_paasta_config().get_cluster(),
            soa_dir=context.soa_dir,
            dashboards=None,
            normal_instance_count=normal_instance_count,
            clients=context.marathon_clients,
            job_config=job_config,
            desired_app_id=app_id,
            verbose=0,
        )
    assert status in output, f"{status!r} not found in {output!r}"


@then(
    (
        'paasta_serviceinit status for the service_instance "{service_instance}"'
        " exits with return code 0 and the correct output"
    )
)
def chronos_status_returns_healthy(context, service_instance):
    cmd = f"python -m paasta_tools.paasta_serviceinit --soa-dir {context.soa_dir} {service_instance} status"
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0
    assert "Disabled" in output
    assert "New" in output


@then(
    (
        'paasta_serviceinit status --verbose for the service_instance "{service_instance}"'
        " exits with return code 0 and the correct output"
    )
)
def chronos_status_verbose_returns_healthy(context, service_instance):
    cmd = f"python -m paasta_tools.paasta_serviceinit --soa-dir {context.soa_dir} {service_instance} status --verbose"
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0
    assert "Running Tasks:" in output


@then(
    (
        'paasta_serviceinit status -vv for the service_instance "{service_instance}"'
        " exits with return code 0 and the correct output"
    )
)
def paasta_serviceinit_tail_stdstreams(context, service_instance):
    cmd = f"python -m paasta_tools.paasta_serviceinit --soa-dir {context.soa_dir} {service_instance} status -vv"
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0
    assert "stdout EOF" in output


@then(
    (
        'paasta_serviceinit status -s "{service}" -i "{instances}"'
        " exits with return code 0 and the correct output"
    )
)
def paasta_serviceinit_status_single_instance(context, service, instances):
    cmd = (
        "python -m paasta_tools.paasta_serviceinit --soa-dir %s -s %s -i %s status"
        % (context.soa_dir, service, instances)
    )
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert "Configured" in output
    assert exit_code == 0


@then(
    (
        'paasta_serviceinit status -s "{service}" -i "{instances}"'
        " has the correct output for instance main and exits with non-zero return code for instance test"
    )
)
def paasta_serviceinit_status_multi_instances(context, service, instances):
    cmd = (
        "python -m paasta_tools.paasta_serviceinit --soa-dir %s -s %s -i %s status"
        % (context.soa_dir, service, instances)
    )
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    # one service is deployed and the other is not
    assert "Configured" in output
    assert exit_code != 0


@then(
    'paasta_serviceinit status for the native service "{service_instance}"'
    " exits with return code {expected_exit_code:d}"
)
def paasta_native_status_returns_healthy(context, service_instance, expected_exit_code):
    cmd = f"python -m paasta_tools.paasta_serviceinit --soa-dir {context.soa_dir} {service_instance} status"
    paasta_print("Running cmd %s" % cmd)
    exit_code, context.output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{context.output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == expected_exit_code


@then('the output matches regex "{pattern}"')
def output_matches_pattern(context, pattern):
    assert re.search(pattern, context.output, re.MULTILINE)


@when('we run paasta serviceinit "{command}" on "{job_id}"')
def paasta_serviceinit_command(context, command, job_id):
    cmd = f"python -m paasta_tools.paasta_serviceinit --soa-dir {context.soa_dir} {job_id} {command}"
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0


@when('we run paasta serviceinit --appid "{command}" on "{job_id}"')
def paasta_serviceinit_command_appid(context, command, job_id):
    (service, instance, _, __) = decompose_job_id(job_id)
    app_id = marathon_tools.create_complete_config(
        service, instance, soa_dir=context.soa_dir
    )["id"]
    cmd = "python -m paasta_tools.paasta_serviceinit --soa-dir {} --appid {} {} {}".format(
        context.soa_dir, app_id, job_id, command
    )
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0


@when('we run paasta serviceinit scale --delta "{delta}" on "{job_id}"')
def paasta_serviceinit_command_scale(context, delta, job_id):
    cmd = (
        "python -m paasta_tools.paasta_serviceinit --soa-dir %s %s scale --delta %s"
        % (context.soa_dir, job_id, delta)
    )
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(f"Got exitcode {exit_code} with output:\n{output}")
    paasta_print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0


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


@when("we wait for our native scheduler to launch exactly {task_count:d} tasks")
def wait_launch_tasks_native(context, task_count):
    while not context.scheduler.get_happy_tasks(context.scheduler.task_store.keys()):
        paasta_print(
            "waiting for scheduler to have %d tasks; %r"
            % (task_count, context.scheduler.task_store)
        )
        time.sleep(0.5)


@given('a capacity check overrides file with contents "{contents}"')
def write_overrides_file(context, contents):
    with NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(contents)
        context.overridefile = f.name


@then('"{job_id}" has exactly {task_count:d} requested tasks in marathon')
def marathon_app_task_count(context, job_id, task_count):
    (service, instance, _, __) = decompose_job_id(job_id)
    app_id = marathon_tools.create_complete_config(
        service, instance, soa_dir=context.soa_dir
    )["id"]

    tasks = context.marathon_client.get_app(app_id).tasks
    assert len(tasks) == task_count


@then(
    'capacity_check "{check_type}" --crit "{crit:d}" --warn "{warn:d}" should return "{status}" with code "{code:d}"'
)
def capacity_check_status_crit_warn(context, check_type, crit, warn, status, code):
    paasta_print(check_type, crit, warn)
    cmd = f"../paasta_tools/monitoring/check_capacity.py {check_type} --crit {crit} --warn {warn}"
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(output)
    assert exit_code == code
    assert status in output


@then('capacity_check "{check_type}" should return "{status}" with code "{code:d}"')
def capacity_check_type_status(context, check_type, status, code):
    cmd = "../paasta_tools/monitoring/check_capacity.py %s" % check_type
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(output)
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
    paasta_print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    paasta_print(output)
    assert exit_code == code
    assert status in output


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
