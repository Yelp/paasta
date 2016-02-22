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
from time import sleep

import mock
from behave import then
from behave import when

from paasta_tools import marathon_tools
from paasta_tools import setup_marathon_job
from paasta_tools.utils import decompose_job_id


def create_complete_app(context):
    with contextlib.nested(
        mock.patch('paasta_tools.bounce_lib.create_app_lock', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.bounce_lib.bounce_lock_zookeeper', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.monitoring_tools.send_event', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.marathon_tools.create_complete_config', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.parse_args', autospec=True),
    ) as (
        _,
        _,
        _,
        mock_create_complete_config,
        mock_parse_args,
    ):
        mock_create_complete_config.return_value = {
            'id': context.app_id,
            'cmd': '/bin/sleep 1m',
            'instances': context.instances,
            'mem': 1,
            'args': [],
            'backoff_factor': 2,
            'cpus': 0.01,
            'backoff_seconds': 1,
            'constraints': None,
        }
        mock_parse_args.return_value = mock.Mock(
            soa_dir=context.soa_dir,
            service_instance=context.job_id,
        )
        try:
            setup_marathon_job.main()
        except SystemExit:
            pass


@when(u'we create a marathon app called "{job_id}" with {number:d} instance(s)')
def create_app_with_instances(context, job_id, number):
    context.job_id = job_id
    if 'app_id' not in context:
        (service, instance, _, __) = decompose_job_id(job_id)
        context.app_id = marathon_tools.create_complete_config(service, instance, None, soa_dir=context.soa_dir)['id']
    set_number_instances(context, number)
    create_complete_app(context)


@when(u'we set the number of instances to {number:d}')
def set_number_instances(context, number):
    context.instances = number


@when(u'we run setup_marathon_job until it has {number:d} task(s)')
def run_until_number_tasks(context, number):
    for _ in xrange(20):
        if marathon_tools.app_has_tasks(context.marathon_client, context.app_id, number, exact_matches_only=True):
            return
        else:
            sleep(0.5)
            create_complete_app(context)


@then(u'we should see it in the list of apps')
def see_it_in_list(context):
    assert context.app_id in marathon_tools.list_all_marathon_app_ids(context.marathon_client)


@then(u'we can run get_app')
def can_run_get_app(context):
    assert context.marathon_client.get_app(context.app_id)


@then(u'we should see the number of instances become {number:d}')
def assert_instances_equals(context, number):
    assert context.marathon_client.get_app(context.app_id).instances == number
