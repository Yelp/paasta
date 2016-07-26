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
import os

import yaml
from behave import then
from behave import when
from marathon import MarathonApp

from paasta_tools import marathon_tools
from paasta_tools.utils import _run
from paasta_tools.utils import remove_ansi_escape_sequences

CONTAINER = {
    'type': 'DOCKER',
    'docker': {
        'network': 'BRIDGE',
        'image': 'busybox',
    },
}


@when(u'all zookeepers are unavailable')
def all_zookeepers_unavailable(context):
    pass


@when(u'all mesos masters are unavailable')
def all_mesos_masters_unavailable(context):
    pass


@when(u'an app with id "{app_id}" using high memory is launched')
def run_paasta_metastatus_high_mem(context, app_id):
    context.marathon_client.create_app(app_id, MarathonApp(cmd='/bin/sleep 1000', mem=490, instances=1,
                                                           container=CONTAINER))


@when(u'an app with id "{app_id}" using high disk is launched')
def run_paasta_metastatus_high_disk(context, app_id):
    context.marathon_client.create_app(app_id, MarathonApp(cmd='/bin/sleep 1000', disk=95, instances=1,
                                                           container=CONTAINER))


@when(u'a chronos job with name "{job_name}" is launched')
def chronos_job_launched(context, job_name):
    job = {'async': False, 'command': 'echo 1', 'epsilon': 'PT15M', 'name': job_name,
           'owner': 'me@foo.com', 'disabled': False, 'schedule': 'R/2014-01-01T00:00:00Z/PT60M'}
    context.chronos_client.add(job)


@when(u'an app with id "{app_id}" using high cpu is launched')
def run_paasta_metastatus_high_cpu(context, app_id):
    context.marathon_client.create_app(app_id, MarathonApp(cmd='/bin/sleep 1000', cpus=9, instances=1,
                                                           container=CONTAINER))


@when(u'a task belonging to the app with id "{app_id}" is in the task list')
def marathon_task_is_ready(context, app_id):
    """Wait for a task with a matching task name to be ready. time out in 60 seconds """
    marathon_tools.wait_for_app_to_launch_tasks(context.marathon_client, app_id, 1)


@then(u'paasta_metastatus{flags} exits with return code "{expected_return_code}" and output "{expected_output}"')
def check_metastatus_return_code_with_flags(context, flags, expected_return_code, expected_output):
    # We don't want to invoke the "paasta metastatus" wrapper because by
    # default it will check every cluster. This is also the way sensu invokes
    # this check.
    cmd = '../paasta_tools/paasta_metastatus.py%s' % flags
    env = dict(os.environ)
    env['MESOS_CLI_CONFIG'] = context.mesos_cli_config_filename
    print 'Running cmd %s with MESOS_CLI_CONFIG=%s' % (cmd, env['MESOS_CLI_CONFIG'])
    exit_code, output = _run(cmd, env=env)

    # we don't care about the colouring here, so remove any ansi escape sequences
    escaped_output = remove_ansi_escape_sequences(output)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    print '\n'

    assert exit_code == int(expected_return_code)
    assert expected_output in escaped_output


@then(u'paasta_metastatus exits with return code "{expected_return_code}" and output "{expected_output}"')
def check_metastatus_return_code_no_flags(context, expected_return_code, expected_output):
    check_metastatus_return_code_with_flags(
        context=context,
        flags='',
        expected_return_code=expected_return_code,
        expected_output=expected_output,
    )


@when('we wait for the service_instance "{service_instance}" to have the correct number of marathon tasks')
def wait_for_tasks(context, service_instance):
    service, instance = service_instance.split(".")[0], service_instance.split(".")[1]
    data = None
    with open(os.path.join(context.soa_dir, service, "marathon-%s.yaml" % context.cluster), 'r+') as f:
        data = yaml.load(f.read())
    expected_instances = data[instance]['instances']
    matching_app_id = marathon_tools.get_matching_appids(service, instance, context.marathon_client)[0]
    marathon_tools.wait_for_app_to_launch_tasks(context.marathon_client, matching_app_id, expected_instances)
