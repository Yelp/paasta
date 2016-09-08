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
import copy

from behave import then
from behave import when

from paasta_tools import chronos_tools
from paasta_tools.utils import _run


@then(u'we should see a job for the service "{service}" and instance "{instance}" in the job list')
def job_exists(context, service, instance):
    matching_jobs = chronos_tools.lookup_chronos_jobs(
        client=context.chronos_client,
        service=service,
        instance=instance,
        include_temporary=True,
        include_disabled=True,
    )
    assert len(matching_jobs) == 1


@when(u'we run setup_chronos_job for service_instance "{service_instance}"')
def run_setup_chronos_job(context, service_instance):
    cmd = "../paasta_tools/setup_chronos_job.py %s -d %s" % (service_instance, context.soa_dir)
    exit_code, output = _run(cmd)
    context.exit_code, context.output = exit_code, output


@then(u'we should get exit code {expected_exit_code:d}')
def check_exit_code(context, expected_exit_code):
    try:
        assert context.exit_code == expected_exit_code, \
            "expected %d, got %d" % (expected_exit_code, context.exit_code)
    except AssertionError:
        # behave likes to back up by two lines and then print some stuff, which clobbers my output, so I stick some
        # extra newlines on here.
        print "Output of setup_chronos_job:\n" + context.output + "\n\n"
        raise


@when(u'we create {job_count:d} disabled jobs that look like the job stored as "{job_name}"')
def old_jobs_leftover(context, job_count, job_name):
    for i in xrange(job_count):
        job_definition = copy.deepcopy(context.jobs[job_name])
        # modify the name by replacing the last character in the config hash
        modified_name = "%s%s" % (job_definition['name'][:-1], i)
        job_definition['name'] = modified_name
        job_definition['disabled'] = True
        context.chronos_client.add(job_definition)


@then(u'there should be {job_count} {disabled} jobs for the service "{service}" and instance "{instance}"')
def should_be_disabled_jobs(context, disabled, job_count, service, instance):
    is_disabled = True if disabled == "disabled" else False
    all_jobs = chronos_tools.lookup_chronos_jobs(
        service=service,
        instance=instance,
        client=context.chronos_client,
        include_disabled=True,
        include_temporary=True,
    )
    filtered_jobs = [job for job in all_jobs if job["disabled"] is is_disabled]
    assert len(filtered_jobs) == int(job_count)


@then(u'setup_chronos_job exits with return code "{expected_return_code}"'
      u' and the output contains "{expected_output_substring}"')
def check_setup_chronos_job_output(context, expected_return_code, expected_output_substring):
    assert int(expected_return_code) == context.exit_code
    assert expected_output_substring in context.output
