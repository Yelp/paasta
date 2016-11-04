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
import json

from behave import then
from behave import when

from paasta_tools.chronos_tools import compose_job_id
from paasta_tools.utils import _run


@when(('I launch {num_jobs} {state} jobs for the service "{service}"'
       ' with scheduled chronos instance "{job}"'))
def launch_jobs(context, num_jobs, state, service, job):
    client = context.chronos_client
    jobs = [{
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        'name': compose_job_id(service, job),
        'owner': 'paasta',
        'disabled': True,
        'schedule': 'R/2014-01-01T00:00:00Z/PT60M',
    } for x in range(0, int(num_jobs))]
    for job in jobs:
        try:
            print('attempting to create job %s' % job['name'])
            client.add(job)
        except Exception:
            print('Error creating test job: %s' % json.dumps(job))
            raise

    # a 'configured' job is one which has had the appropriate
    # yelp-soa configs into place.
    # an 'unconfigured' job represents a job which may at one stage
    # been a configured chronos job, but no longer has the
    # corresponding configuration in place the target for.
    # 'unconfigured' jobs are the target for cleanup_chronos_jobs
    if state == "configured":
        context.configured_job_names = [job['name'] for job in jobs]
    elif state == "unconfigured":
        context.unconfigured_job_names = [job['name'] for job in jobs]


@when('I launch {num_jobs} non-paasta jobs')
def launch_non_paasta_jobs(context, num_jobs):
    client = context.chronos_client
    jobs = [{
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        'name': 'foobar%d' % job,
        'owner': 'rogue',
        'disabled': True,
        'schedule': 'R/2014-01-01T00:00:00Z/PT60M',
    } for job in range(0, int(num_jobs))]
    context.non_paasta_jobs = [job['name'] for job in jobs]
    for job in jobs:
        try:
            print('attempting to create job %s' % job['name'])
            client.add(job)
        except Exception:
            print('Error creating test job: %s' % json.dumps(job))
            raise


@then(u'cleanup_chronos_jobs exits with return code "{expected_return_code}" and the correct output')
def check_cleanup_chronos_jobs_output(context, expected_return_code):
    cmd = '../paasta_tools/cleanup_chronos_jobs.py --soa-dir %s' % context.soa_dir
    exit_code, output = _run(cmd)
    print(context.unconfigured_job_names)
    print('Got exitcode %s with output:\n%s' % (exit_code, output))

    assert exit_code == int(expected_return_code)
    assert "Successfully Removed Tasks (if any were running) for:" in output
    assert "Successfully Removed Jobs:" in output
    for job in context.unconfigured_job_names:
        assert '  %s' % job in output


@when(u'we run cleanup_chronos_jobs')
def run_cleanup_chronos_jobs(context):
    cmd = '../paasta_tools/cleanup_chronos_jobs.py --soa-dir %s' % context.soa_dir
    context.exit_code, context.output = _run(cmd)
    print(context.output)


@then('the non-paasta jobs are not in the job list')
def check_non_paasta_jobs(context):
    jobs = context.chronos_client.list()
    running_job_names = [job['name'] for job in jobs]
    assert not any([job_name in running_job_names for job_name in context.non_paasta_jobs])


@then('the {state} chronos jobs are not in the job list')
def assert_jobs_not_in_job_list(context, state):
    jobs = context.chronos_client.list()
    if state == "configured":
        state_list = context.configured_job_names
    elif state == "unconfigured":
        state_list = context.unconfigured_job_names
    assert_no_job_names_in_list(state_list, jobs)


@then('the {state} chronos jobs are in the job list')
def assert_jobs_all_in_job_list(context, state):
    jobs = context.chronos_client.list()
    if state == "configured":
        state_list = context.configured_job_names
    elif state == "unconfigured":
        state_list = context.unconfigured_job_names
    assert_all_job_names_in_list(state_list, jobs)


def assert_no_job_names_in_list(names, jobs):
    running_job_names = [job['name'] for job in jobs]
    assert all([job_name not in running_job_names for job_name in names])


def assert_all_job_names_in_list(names, jobs):
    running_job_names = [job['name'] for job in jobs]
    assert all([job_name in running_job_names for job_name in names])
