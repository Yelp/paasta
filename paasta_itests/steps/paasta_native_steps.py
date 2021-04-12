import json
import os
import socket
import time
from typing import List
from typing import Tuple
from unittest import mock

import a_sync
import yaml
from behave import given
from behave import then
from behave import when
from itest_utils import clear_mesos_tools_cache
from requests import HTTPError

from paasta_tools import drain_lib
from paasta_tools import mesos_tools
from paasta_tools.adhoc_tools import AdhocJobConfig
from paasta_tools.frameworks.adhoc_scheduler import AdhocScheduler
from paasta_tools.frameworks.native_scheduler import create_driver
from paasta_tools.frameworks.native_scheduler import LIVE_TASK_STATES
from paasta_tools.frameworks.native_scheduler import NativeScheduler
from paasta_tools.frameworks.native_scheduler import TASK_RUNNING
from paasta_tools.frameworks.native_service_config import NativeServiceConfig
from paasta_tools.native_mesos_scheduler import main
from paasta_tools.native_mesos_scheduler import paasta_native_services_running_here
from paasta_tools.utils import load_system_paasta_config


@given("a new adhoc config to be deployed")
def new_adhoc_config(context):
    context.cluster = "fake_cluster"
    context.instance = "fake_instance"
    context.service = "fake_service"
    context.new_config = AdhocJobConfig(
        cluster=context.cluster,
        instance=context.instance,
        service=context.service,
        config_dict={"cpus": 0.1, "mem": 50},
        branch_dict={
            "docker_image": "busybox",
            "desired_state": "start",
            "force_bounce": None,
        },
    )


@given("a new paasta_native config to be deployed, with {num} instances")
def new_paasta_native_config(context, num):
    context.cluster = "fake_cluster"
    context.instance = "fake_instance"
    context.service = "fake_service"

    context.new_config = NativeServiceConfig(
        cluster=context.cluster,
        instance=context.instance,
        service=context.service,
        config_dict={
            "cpus": 0.1,
            "mem": 50,
            "instances": int(num),
            "cmd": "sleep 50",
            "drain_method": "test",
        },
        branch_dict={
            "docker_image": "busybox",
            "desired_state": "start",
            "force_bounce": None,
        },
        soa_dir="/fake/etc/services",
        service_namespace_config=None,
    )


@when(
    "we start a {scheduler} scheduler with reconcile_backoff {reconcile_backoff} and name {framework_name}"
)
def start_paasta_native_framework(
    context, scheduler, reconcile_backoff, framework_name
):
    clear_mesos_tools_cache()
    system_paasta_config = load_system_paasta_config()
    system_paasta_config.config_dict[
        "docker_registry"
    ] = "docker.io"  # so busybox runs.

    if scheduler == "paasta_native":
        scheduler_class = NativeScheduler
    elif scheduler == "adhoc":
        scheduler_class = AdhocScheduler
    else:
        raise Exception("unknown scheduler: %s" % scheduler)

    context.framework_name = framework_name
    context.scheduler = scheduler_class(
        service_name=context.service,
        instance_name=context.instance,
        cluster=context.cluster,
        staging_timeout=30,
        system_paasta_config=system_paasta_config,
        service_config=context.new_config,
        reconcile_backoff=int(reconcile_backoff),
    )

    context.driver = create_driver(
        framework_name=framework_name,
        scheduler=context.scheduler,
        system_paasta_config=system_paasta_config,
    )

    context.driver.start()

    if not hasattr(context, "framework_ids"):
        context.framework_ids = []

    for _ in range(10):
        if context.scheduler.framework_id:
            context.framework_ids.append(context.scheduler.framework_id)
            break
        time.sleep(1)
    else:
        raise Exception("Expected scheduler to successfully register before timeout")


@then("it should eventually start {num} tasks")
def should_eventually_start_num_tasks(context, num):
    num = int(num)

    for _ in range(20):
        actual_num = len(
            [
                p
                for p in context.scheduler.task_store.get_all_tasks().values()
                if p.mesos_task_state == TASK_RUNNING
            ]
        )
        if actual_num >= num:
            return
        time.sleep(1)

    raise Exception("Expected %d tasks before timeout, saw %d" % (num, actual_num))


@given("a fresh soa_dir")
def fresh_soa_dir(context):
    soa_dir = "/nail/etc/services/"
    context.soa_dir = soa_dir


@given(
    "paasta_native-cluster.yaml and deployments.json files for service {service} with instance {instance}"
)
def write_paasta_native_cluster_yaml_files(context, service, instance):
    if not os.path.exists(os.path.join(context.soa_dir, service)):
        os.makedirs(os.path.join(context.soa_dir, service))
    with open(
        os.path.join(
            context.soa_dir, service, "paasta_native-%s.yaml" % context.cluster
        ),
        "w",
    ) as f:
        f.write(
            yaml.safe_dump(
                {
                    instance: {
                        "cmd": 'echo "Taking a nap..." && sleep 1m && echo "Nap time over, back to work"',
                        "mem": 100,
                        "cpus": 0.1,
                        "instances": 1,
                    }
                }
            )
        )
    with open(os.path.join(context.soa_dir, service, "deployments.json"), "w") as f:
        json.dump(
            {
                "v1": {
                    f"{service}:paasta-{context.cluster}.{instance}": {
                        "docker_image": "busybox",
                        "desired_state": "start",
                        "force_bounce": None,
                    }
                },
                "v2": {
                    "deployments": {
                        f"{context.cluster}.{instance}": {
                            "docker_image": "busybox",
                            "git_sha": "deadbeef",
                        }
                    },
                    "controls": {
                        f"{service}:{context.cluster}.{instance}": {
                            "desired_state": "start",
                            "force_bounce": None,
                        }
                    },
                },
            },
            f,
        )


@when("we run native_mesos_scheduler.main()")
def run_native_mesos_scheduler_main(context):
    clear_mesos_tools_cache()
    context.main_schedulers = main(
        [
            "--soa-dir",
            context.soa_dir,
            "--stay-alive-seconds",
            "10",
            "--periodic-interval",
            "1",
        ]
    )


@then("there should be a framework registered with name {name}")
def should_be_framework_with_id(context, name):
    clear_mesos_tools_cache()
    assert name in [
        f.name for f in a_sync.block(mesos_tools.get_all_frameworks, active_only=True)
    ]


@then("there should not be a framework registered with name {name}")
def should_not_be_framework_with_name(context, name):
    clear_mesos_tools_cache()
    assert name not in [
        f.name for f in a_sync.block(mesos_tools.get_all_frameworks, active_only=True)
    ]


@when("we terminate that framework")
def terminate_that_framework(context):
    try:
        print("terminating framework %s" % context.scheduler.framework_id)
        mesos_tools.terminate_framework(context.scheduler.framework_id)
    except HTTPError as e:
        raise Exception(e.response.text)


@when("we stop that framework without terminating")
def stop_that_framework(context):
    context.driver.stop(True)
    context.driver.join()


@then("it should have the same ID as before")
def should_have_same_id(context):
    assert context.framework_ids[-2] == context.framework_ids[-1]


@then("it should have a different ID than before")
def should_have_different_id(context):
    assert context.framework_ids[-2] != context.framework_ids[-1]


@when("we sleep {wait} seconds")
def we_sleep_wait_seconds(context, wait):
    time.sleep(int(wait))


@when("we change force_bounce")
def we_change_the_config(context):
    branch_dict = context.scheduler.service_config.branch_dict
    context.old_force_bounce = branch_dict["force_bounce"]
    branch_dict["force_bounce"] = str(int(branch_dict["force_bounce"] or 0) + 1)


@when("we change force_bounce back")
def we_change_force_bounce_back(context):
    branch_dict = context.scheduler.service_config.branch_dict
    branch_dict["force_bounce"] = context.old_force_bounce


@then("it should eventually drain {num} tasks")
def it_should_drain_num_tasks(context, num):
    num = int(num)
    for _ in range(10):
        if len(drain_lib.TestDrainMethod.downed_task_ids) >= num:
            # set() to make a copy.
            context.drained_tasks = set(drain_lib.TestDrainMethod.downed_task_ids)
            return
        time.sleep(1)
    else:
        raise Exception(
            "Expected %d tasks to drain before timeout, saw %d"
            % (num, len(drain_lib.TestDrainMethod.downed_task_ids))
        )


@then(
    "it should undrain {num_undrain_expected} tasks and drain {num_drain_expected} more"
)
def it_should_undrain_and_drain(context, num_undrain_expected, num_drain_expected):
    num_undrain_expected = int(num_undrain_expected)
    num_drain_expected = int(num_drain_expected)

    for _ in range(10):
        print("currently drained: %r" % drain_lib.TestDrainMethod.downed_task_ids)
        print("drained previously: %r" % context.drained_tasks)
        num_drained = len(
            drain_lib.TestDrainMethod.downed_task_ids - context.drained_tasks
        )
        num_undrained = len(
            context.drained_tasks - drain_lib.TestDrainMethod.downed_task_ids
        )
        if num_drained >= num_drain_expected and num_undrained >= num_undrain_expected:
            return
        time.sleep(1)
    else:
        raise Exception(
            "Expected %d tasks to drain and %d to undrain, saw %d and %d"
            % (num_drain_expected, num_undrain_expected, num_drained, num_undrained)
        )


@then("it should eventually have only {num} tasks")
def it_should_eventually_have_only_num_tasks(context, num):
    num = int(num)

    for _ in range(60):
        actual_num = len(
            [
                p
                for p in context.scheduler.task_store.get_all_tasks().values()
                if p.mesos_task_state == TASK_RUNNING
            ]
        )
        if actual_num <= num:
            return
        time.sleep(1)

    raise Exception("Expected <= %d tasks before timeout, saw %d" % (num, actual_num))


@when("we call periodic")
def we_call_periodic(context):
    with mock.patch.object(context.scheduler, "load_config"):
        context.scheduler.periodic(context.driver)


@when("we change instances to {num}")
def we_change_instances_to_num(context, num):
    num = int(num)
    context.scheduler.service_config.config_dict["instances"] = num


@then("it should not start tasks for {num} seconds")
def should_not_start_tasks_for_num_seconds(context, num):
    time.sleep(int(num))

    assert [] == [
        p
        for p in context.scheduler.task_store.get_all_tasks().values()
        if (p.mesos_task_state in LIVE_TASK_STATES)
    ]


@then("periodic() should eventually be called")
def periodic_should_eventually_be_called(context):
    for _ in range(30):
        for scheduler in context.main_schedulers:
            if hasattr(scheduler, "periodic_was_called"):
                return
    else:
        raise Exception("periodic() not called on all schedulers")


@then(
    "our service should show up in paasta_native_services_running_here {expected_num:d} times on any of our slaves"
)
def service_should_show_up_in_pnsrh_n_times(context, expected_num):
    mesosslave_ips = {x[4][0] for x in socket.getaddrinfo("mesosslave", 5051)}

    results: List[Tuple[str, str, int]] = []
    for mesosslave_ip in mesosslave_ips:
        results.extend(
            paasta_native_services_running_here(
                hostname=mesosslave_ip,
                framework_id=context.scheduler.framework_id,  # Ignore anything from other itests.
            )
        )

    matching_results = [
        res for res in results if res == (context.service, context.instance, mock.ANY)
    ]
    assert (
        len(matching_results) == expected_num
    ), f"matching results {matching_results!r}, all results {results!r}"
