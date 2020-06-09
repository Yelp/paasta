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
from time import sleep

import mock
from behave import then
from behave import when
from itest_utils import get_service_connection_string
from itest_utils import update_context_marathon_config
from marathon.exceptions import MarathonHttpError

from paasta_tools import marathon_tools
from paasta_tools import mesos_maintenance
from paasta_tools import setup_marathon_job
from paasta_tools.autoscaling.autoscaling_service_lib import (
    set_instances_for_marathon_service,
)
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import SystemPaastaConfig


def run_setup_marathon_job_no_apps_found(context):
    update_context_marathon_config(context)
    with mock.patch.object(
        SystemPaastaConfig, "get_zk_hosts", autospec=True, return_value=context.zk_hosts
    ), mock.patch(
        "paasta_tools.setup_marathon_job.parse_args", autospec=True
    ) as mock_parse_args, mock.patch.object(
        MarathonServiceConfig,
        "format_marathon_app_dict",
        autospec=True,
        return_value=context.marathon_complete_config,
    ), mock.patch(
        "paasta_tools.setup_marathon_job.monitoring_tools.send_event", autospec=True
    ), mock.patch(
        "paasta_tools.setup_marathon_job.marathon_tools.get_all_marathon_apps",
        autospec=True,
        return_value=[],
    ):
        mock_parse_args.return_value = mock.Mock(
            verbose=True,
            soa_dir=context.soa_dir,
            service_instance_list=[context.job_id],
        )
        try:
            setup_marathon_job.main()
        except SystemExit:
            pass


def run_setup_marathon_job(context):
    update_context_marathon_config(context)
    with mock.patch.object(
        SystemPaastaConfig, "get_zk_hosts", autospec=True, return_value=context.zk_hosts
    ), mock.patch(
        "paasta_tools.setup_marathon_job.parse_args", autospec=True
    ) as mock_parse_args, mock.patch.object(
        MarathonServiceConfig,
        "format_marathon_app_dict",
        autospec=True,
        return_value=context.marathon_complete_config,
    ), mock.patch(
        "paasta_tools.setup_marathon_job.monitoring_tools.send_event", autospec=True
    ):
        mock_parse_args.return_value = mock.Mock(
            verbose=True,
            soa_dir=context.soa_dir,
            service_instance_list=[context.job_id],
        )
        try:
            setup_marathon_job.main()
        except (SystemExit, MarathonHttpError):
            pass


@when("we set up an app to use zookeeper scaling with {number:d} max instances")
def setup_zookeeper(context, number):
    context.max_instances = number


@when('we create a marathon app called "{job_id}" with {number:d} instance(s)')
def create_app_with_instances(context, job_id, number):
    create_app_with_instances_constraints(context, job_id, number, str(None))


@when(
    'we create a marathon app called "{job_id}" with {number:d} instance(s) with no apps found running'
)
def create_app_with_instances_with_race(context, job_id, number):
    create_app_with_instances_constraints(
        context, job_id, number, str(None), no_apps_running=True
    )


@when(
    'we create a marathon app called "{job_id}" with {number:d} instance(s) and constraints {constraints}'
)
def create_app_with_instances_constraints(
    context, job_id, number, constraints, no_apps_running=False
):
    set_number_instances(context, number)
    context.job_id = job_id
    (service, instance, _, __) = decompose_job_id(job_id)
    context.service = service
    context.instance = instance
    context.zk_hosts = "%s/mesos-testcluster" % get_service_connection_string(
        "zookeeper"
    )
    context.constraints = constraints
    update_context_marathon_config(context)
    context.app_id = context.marathon_complete_config["id"]
    context.new_id = (
        context.app_id
    )  # for compatibility with bounces_steps.there_are_num_which_tasks
    if no_apps_running:
        run_setup_marathon_job_no_apps_found(context)
    else:
        run_setup_marathon_job(context)


@when("we set the number of instances to {number:d}")
def set_number_instances(context, number):
    context.instances = number


@when("we run setup_marathon_job until it has {number:d} task(s)")
def run_until_number_tasks(context, number):
    for _ in range(20):
        with mock.patch(
            "paasta_tools.mesos_maintenance.load_credentials", autospec=True
        ) as mock_load_credentials:
            mock_load_credentials.side_effect = mesos_maintenance.load_credentials(
                mesos_secrets="/etc/mesos-slave-secret"
            )
            run_setup_marathon_job(context)
        sleep(0.5)
        if context.current_client.get_app(context.app_id).instances == number:
            return
    assert context.current_client.get_app(context.app_id).instances == number


@when(
    'we set the instance count in zookeeper for service "{service}" instance "{instance}" to {number:d}'
)
def zookeeper_scale_job(context, service, instance, number):
    with mock.patch.object(
        SystemPaastaConfig, "get_zk_hosts", autospec=True, return_value=context.zk_hosts
    ):
        set_instances_for_marathon_service(
            service, instance, number, soa_dir=context.soa_dir
        )


@then("we should see it in the list of apps on shard {shard_number:d}")
@then("we should see it in the list of apps")
def see_it_in_list(context, shard_number=None):
    full_list = []
    if shard_number is None:
        for client in context.marathon_clients.get_all_clients():
            full_list.extend(marathon_tools.list_all_marathon_app_ids(client))
    else:
        full_list.extend(
            marathon_tools.list_all_marathon_app_ids(
                context.marathon_clients.current[shard_number]
            )
        )

    assert context.app_id in full_list, (context.app_id, full_list)


@then("we should not see it in the list of apps on shard {shard_number:d}")
@then("we should not see it in the list of apps")
def not_see_it_in_list(context, shard_number=None):
    full_list = []
    if shard_number is None:
        for client in context.marathon_clients.get_all_clients():
            full_list.extend(marathon_tools.list_all_marathon_app_ids(client))
    else:
        full_list.extend(
            marathon_tools.list_all_marathon_app_ids(
                context.marathon_clients.current[shard_number]
            )
        )

    assert context.app_id not in full_list


@then("we can run get_app")
def can_run_get_app(context):
    assert context.current_client.get_app(context.app_id)


@then("we should see the number of instances become {number:d}")
def assert_instances_equals(context, number):
    attempts = 0
    while attempts < 10:
        try:
            assert context.current_client.get_app(context.app_id).instances == number
            return
        except AssertionError:
            attempts += 1
            sleep(5)
    assert context.current_client.get_app(context.app_id).instances == number


@when("we mark a host it is running on as at-risk")
def mark_host_running_on_at_risk(context):
    app = context.current_client.get_app(context.new_id)
    tasks = app.tasks
    host = tasks[0].host
    mark_host_at_risk(context, host)


@when('we mark the host "{host}" as at-risk')
def mark_host_at_risk(context, host):
    start = mesos_maintenance.datetime_to_nanoseconds(mesos_maintenance.now())
    duration = mesos_maintenance.parse_timedelta("1h")
    with mock.patch(
        "paasta_tools.mesos_maintenance.get_principal", autospec=True
    ) as mock_get_principal, mock.patch(
        "paasta_tools.mesos_maintenance.get_secret", autospec=True
    ) as mock_get_secret:
        credentials = mesos_maintenance.load_credentials(
            mesos_secrets="/etc/mesos-slave-secret"
        )
        mock_get_principal.return_value = credentials.principal
        mock_get_secret.return_value = credentials.secret
        mesos_maintenance.drain([host], start, duration)
        context.at_risk_host = host


@then("there should be {number:d} tasks on that at-risk host")
def tasks_on_that_at_risk_host_drained(context, number):
    tasks_on_host_drained(context, number, context.at_risk_host)


@then('there should be {number:d} tasks on the host "{host}"')
def tasks_on_host_drained(context, number, host):
    app_id = context.new_id
    tasks = context.current_client.list_tasks(app_id)
    count = 0
    for task in tasks:
        if task.host == host:
            count += 1
    assert count == number
