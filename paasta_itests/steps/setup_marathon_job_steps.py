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
import os
from time import sleep

import mock
import yaml
from behave import then
from behave import when
from itest_utils import get_service_connection_string

from paasta_tools import marathon_tools
from paasta_tools import setup_marathon_job
from paasta_tools.utils import decompose_job_id


def create_complete_app(context):
    with contextlib.nested(
        mock.patch('paasta_tools.utils.SystemPaastaConfig.get_zk_hosts', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.marathon_tools.create_complete_config', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.parse_args', autospec=True),
        mock.patch('paasta_tools.setup_marathon_job.monitoring_tools.send_event', autospec=True),
    ) as (
        mock_get_zk_hosts,
        mock_create_complete_config,
        mock_parse_args,
        _,
    ):
        mock_get_zk_hosts.return_value = '%s,mesos-testcluster' % get_service_connection_string('zookeeper')
        whitelist_keys = set(['id', 'cmd', 'instances', 'mem', 'cpus', 'backoff_factor',
                              'backoff_seconds', 'constraints', 'args', 'max_instances'])
        mock_create_complete_config.return_value = {key: value for key, value in marathon_tools.create_complete_config(
            context.service,
            context.instance,
            soa_dir=context.soa_dir,
        ).items() if key in whitelist_keys}
        mock_create_complete_config.return_value['cmd'] = '/bin/sleep 1m'
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
        context.service = service
        context.instance = instance
        context.app_id = marathon_tools.create_complete_config(service, instance, soa_dir=context.soa_dir)['id']
    set_number_instances(context, number)
    create_complete_app(context)


@when(u'we set the number of instances to {number:d}')
def set_number_instances(context, number):
    with open(os.path.join(context.soa_dir, context.service, "marathon-%s.yaml" % context.cluster), 'r+') as f:
        data = yaml.load(f.read())
        data[context.instance]['instances'] = number
        f.seek(0)
        f.write(yaml.safe_dump(data))
        f.truncate()


@when(u'we run setup_marathon_job until it has {number:d} task(s)')
def run_until_number_tasks(context, number):
    for _ in xrange(20):
        create_complete_app(context)
        sleep(0.5)
        if marathon_tools.app_has_tasks(context.marathon_client, context.app_id, number, exact_matches_only=True):
            return


@when(u'we increase the instance count in zookeeper for service "{service}" instance "{instance}" by {number:d}')
def zookeeper_scale_job(context, service, instance, number):
    with mock.patch('paasta_tools.utils.SystemPaastaConfig.get_zk_hosts', autospec=True) as mock_get_zk_hosts:
        mock_get_zk_hosts.return_value = '%s,mesos-testcluster' % get_service_connection_string('zookeeper')
        marathon_tools.update_instances_for_marathon_service(service, instance, number)


@then(u'we should see it in the list of apps')
def see_it_in_list(context):
    assert context.app_id in marathon_tools.list_all_marathon_app_ids(context.marathon_client)


@then(u'we can run get_app')
def can_run_get_app(context):
    assert context.marathon_client.get_app(context.app_id)


@then(u'we should see the number of instances become {number:d}')
def assert_instances_equals(context, number):
    assert context.marathon_client.get_app(context.app_id).instances == number
