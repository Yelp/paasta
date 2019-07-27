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

from behave import then
from behave import when

from paasta_tools import chronos_tools
from paasta_tools.chronos_tools import SPACER
from paasta_tools.utils import _run


@when('we run chronos_rerun for service_instance "{service_instance}"')
def run_chronos_rerun(context, service_instance):
    run_chronos_rerun_with_args(context, service_instance, "")


@when(
    'we run chronos_rerun for service_instance "{service_instance}" with args {cli_args}'
)
def run_chronos_rerun_with_args(context, service_instance, cli_args):
    cmd = (
        "python ../paasta_tools/chronos_rerun.py -d %s %s '%s' " "2016-03-13T04:50:31"
    ) % (context.soa_dir, cli_args, service_instance)
    exit_code, output = _run(cmd)
    context.exit_code, context.output = exit_code, output


@then('the rerun job for "{service_instance}" is {disabled}')
def rerun_job_is_disabled(context, service_instance, disabled):
    is_disabled = disabled == "disabled"
    all_jobs = context.chronos_client.list()
    matching_jobs = [
        job
        for job in all_jobs
        if re.match(
            f"{chronos_tools.TMP_JOB_IDENTIFIER}-.* {service_instance}", job["name"]
        )
    ]
    assert matching_jobs[0]["disabled"] == is_disabled


@then('there is a temporary job for the service "{service}" and instance "{instance}"')
def temporary_job_exists(context, service, instance):
    all_jobs = context.chronos_client.list()
    matching_jobs = [
        job
        for job in all_jobs
        if re.match(
            f"{chronos_tools.TMP_JOB_IDENTIFIER}-.* {service} {instance}", job["name"]
        )
    ]
    assert len(matching_jobs) == 1
    return matching_jobs[0]


@then(
    'there is a temporary job for the service "{service}" and instance "{instance}" dependent on {parents_csv}'
)
def temporary_job_exists_and_dependency_check(context, service, instance, parents_csv):
    matching_job = temporary_job_exists(context, service, instance)

    if parents_csv == "None":
        parents = []
    else:
        parents = parents_csv.split(",")
    tmp_prefix, _, _ = matching_job["name"].split(SPACER)

    job_parents = set(matching_job.get("parents", []))
    expected_job_parents = {f"{tmp_prefix}{SPACER}{parent}" for parent in parents}
    assert job_parents == expected_job_parents
