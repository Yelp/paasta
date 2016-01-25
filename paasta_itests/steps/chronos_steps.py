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
from behave import then
from behave import when

from paasta_tools import chronos_tools


@when(u'we create a trivial chronos job called "{job_name}"')
def create_trivial_chronos_job(context, job_name):
    job_config = {
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        'name': 'job_name',
        'owner': '',
        'disabled': False,
        'schedule': 'R/20140101T00:00:00Z/PT60M',
    }
    context.jobs[job_name] = job_config
    context.chronos_client.add(job_config)
    context.chronos_job_name = job_config['name']


@when(u'we store the name of the job for the service {service} and instance {instance} as {job_name}')
def create_chronos_job_config_object_from_configs(context, service, instance, job_name):
    job_config = chronos_tools.create_complete_config(
        service=service,
        job_name=instance,
        soa_dir=context.soa_dir,
    )
    # s_c_l caches reads from the fs and doesn't offer a way to ignore the cache, so doesn't return
    # the most recent version on the file. I *hate* this, but need to move on.
    chronos_tools.service_configuration_lib._yaml_cache = {}
    context.jobs[job_name] = job_config


@when(u'we send the job to chronos')
def send_job_to_chronos(context):
    context.chronos_client.add(context.chronos_job_config)


@when(u'we wait for the chronos job stored as "{job_name}" to appear in the job list')
def chronos_job_is_ready(context, job_name):
    """Wait for a job with a matching job id to be ready."""
    chronos_tools.wait_for_job(context.chronos_client, context.jobs[job_name]['name'])


@then(u"we {should_or_not} be able to see it when we list jobs")
def list_chronos_jobs_has_job(context, should_or_not):
    jobs = context.chronos_client.list()
    job_names = [job['name'] for job in jobs]
    if should_or_not == "should not":
        assert context.chronos_job_name not in job_names
    else:
        assert context.chronos_job_name in job_names


# NOTE this is a placeholder until we are able to get per-job task information from Chronos
@then(u"the {old_or_new_job} {has_or_not} running tasks")
def chronos_check_running_tasks(context, old_or_new_job, has_or_not):
    # job_id = context.old_chronos_job_name if old_or_new_job == 'old job' else context.chronos_job_name
    # if has_or_not == "has no":
    #     assert job_id has no running tasks
    # else:  # has_or_not should be "has"
    #     assert job_id has running tasks
    assert True


@then(u'the field "{field}" for the job stored as "{job_name}" is set to "{value}"')
def chronos_check_job_state(context, field, job_name, value):
    job_id = context.jobs[job_name]['name']
    (service, instance, git_hash, config_hash) = chronos_tools.decompose_job_id(job_id)
    jobs = chronos_tools.lookup_chronos_jobs(
        service=service,
        instance=instance,
        git_hash=git_hash,
        config_hash=config_hash,
        client=context.chronos_client,
        include_disabled=True
    )
    assert len(jobs) == 1
    # we cast to a string so you can correctly assert that a value is True/False
    assert str(jobs[0][field]) == value


@then(u'the job stored as "{job_name}" is {disabled} in chronos')
def job_is_disabled(context, job_name, disabled):
    is_disabled = True if disabled == "disabled" else False
    full_job_name = context.jobs[job_name]["name"]
    all_jobs = context.chronos_client.list()
    filtered_jobs = [job for job in all_jobs if job["name"] == full_job_name]
    assert filtered_jobs[0]["disabled"] is is_disabled
