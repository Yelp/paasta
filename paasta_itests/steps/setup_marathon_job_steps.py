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
import contextlib
from time import sleep

import mesos.cli.master
import mock
from behave import then
from behave import when
from itest_utils import get_service_connection_string
from itest_utils import update_context_marathon_config
from marathon.exceptions import MarathonHttpError

from paasta_tools import marathon_tools
from paasta_tools import paasta_maintenance
from paasta_tools import setup_marathon_job
from paasta_tools.autoscaling_lib import set_instances_for_marathon_service
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import SystemPaastaConfig


def run_setup_marathon_job(context):
    update_context_marathon_config(context)
    with contextlib.nested(
        mock.patch.object(SystemPaastaConfig, 'get_zk_hosts', autospec=True, return_value=context.zk_hosts),
        mock.patch('paasta_tools.setup_marathon_job.parse_args', autospec=True),
        mock.patch.object(MarathonServiceConfig, 'format_marathon_app_dict', autospec=True,
                          return_value=context.marathon_complete_config),
        mock.patch('paasta_tools.setup_marathon_job.monitoring_tools.send_event', autospec=True),
    ) as (
        mock_get_zk_hosts,
        mock_parse_args,
        _,
        _,
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


@when(u'we set up an app to use zookeeper scaling with {number:d} max instances')
def setup_zookeeper(context, number):
    context.max_instances = number


@when(u'we create a marathon app called "{job_id}" with {number:d} instance(s)')
def create_app_with_instances(context, job_id, number):
    create_app_with_instances_constraints(context, job_id, number, str(None))


@when(u'we create a marathon app called "{job_id}" with {number:d} instance(s) and constraints {constraints}')
def create_app_with_instances_constraints(context, job_id, number, constraints):
    set_number_instances(context, number)
    context.job_id = job_id
    (service, instance, _, __) = decompose_job_id(job_id)
    context.service = service
    context.instance = instance
    context.zk_hosts = '%s/mesos-testcluster' % get_service_connection_string('zookeeper')
    context.constraints = constraints
    update_context_marathon_config(context)
    context.app_id = context.marathon_complete_config['id']
    run_setup_marathon_job(context)


@when(u'we set the number of instances to {number:d}')
def set_number_instances(context, number):
    context.instances = number


@when(u'we run setup_marathon_job until it has {number:d} task(s)')
def run_until_number_tasks(context, number):
    for _ in xrange(20):
        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True),
        ) as (
            _,
        ):
            run_setup_marathon_job(context)
        sleep(0.5)
        if marathon_tools.app_has_tasks(context.marathon_client, context.app_id, number, exact_matches_only=True):
            return
    assert marathon_tools.app_has_tasks(context.marathon_client, context.app_id, number, exact_matches_only=True)


@when(u'we set the instance count in zookeeper for service "{service}" instance "{instance}" to {number:d}')
def zookeeper_scale_job(context, service, instance, number):
    with contextlib.nested(
        mock.patch.object(SystemPaastaConfig, 'get_zk_hosts', autospec=True, return_value=context.zk_hosts)
    ) as (
        _,
    ):
        set_instances_for_marathon_service(service, instance, number, soa_dir=context.soa_dir)


@then(u'we should see it in the list of apps')
def see_it_in_list(context):
    assert context.app_id in marathon_tools.list_all_marathon_app_ids(context.marathon_client)


@then(u'we can run get_app')
def can_run_get_app(context):
    assert context.marathon_client.get_app(context.app_id)


@then(u'we should see the number of instances become {number:d}')
def assert_instances_equals(context, number):
    assert context.marathon_client.get_app(context.app_id).instances == number


@when(u'we mark the host "{host}" as at-risk')
def mark_host_at_risk(context, host):
    start = paasta_maintenance.datetime_to_nanoseconds(paasta_maintenance.now())
    duration = paasta_maintenance.parse_timedelta('1h')
    config = {
        'master': '%s' % get_service_connection_string('mesosmaster'),
        'scheme': 'http',
        'response_timeout': 5,
    }
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_maintenance.load_credentials', autospec=True),
        mock.patch.object(mesos.cli.master, 'CFG', config),
    ) as (
        mock_load_credentials,
        _,
    ):
        mock_load_credentials.side_effect = paasta_maintenance.load_credentials(mesos_secrets='/etc/mesos-slave-secret')
        paasta_maintenance.drain([host], start, duration)


@then(u'the tasks on the host "{host}" should be drained')
def tasks_on_host_drained(context, host):
    app_id = context.new_id
    tasks = context.marathon_client.list_tasks(app_id)
    for task in tasks:
        if task.host == host:
            return False
    return True
