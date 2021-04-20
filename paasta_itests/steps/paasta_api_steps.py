# Copyright 2015-2017 Yelp Inc.
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

from paasta_tools.cli.cmds.status import paasta_status_on_api_endpoint
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import load_system_paasta_config


@then('instance GET should return error code "{error_code}" for "{job_id}"')
def service_instance_status_error(context, error_code, job_id):
    (service, instance, _, __) = decompose_job_id(job_id)

    response = None
    try:
        response = context.paasta_api_client.service.status_instance(
            instance=instance, service=service
        )
    except context.paasta_api_client.api_error as exc:
        assert exc.status == int(error_code)

    assert not response


@then('resources GET should show "{resource}" has {used} used')
def resources_resource_used(context, resource, used):
    used = float(used)
    response = context.paasta_api_client.resources.resources().value
    assert response[0].to_dict()[resource].get("used") == used, response


@then(
    'resources GET with groupings "{groupings}" and filters "{filters}" should return {num:d} groups'
)
def resources_groupings_filters(context, groupings, filters, num):
    groupings = groupings.split(",")
    if len(filters) > 0:
        filters = filters.split("|")
    response = context.paasta_api_client.resources.resources(
        groupings=groupings, filter=filters
    )

    assert len(response.value) == num, response


@then('resources GET with groupings "{groupings}" should return {num:d} groups')
def resources_groupings(context, groupings, num):
    return resources_groupings_filters(context, groupings, [], num)


@then('paasta status via the API for "{service}.{instance}" should run successfully')
def paasta_status_via_api(context, service, instance):
    output = []
    system_paasta_config = load_system_paasta_config()
    exit_code = paasta_status_on_api_endpoint(
        cluster=system_paasta_config.get_cluster(),
        service=service,
        instance=instance,
        output=output,
        system_paasta_config=system_paasta_config,
        verbose=0,
    )
    print(f"Got exitcode {exit_code} with output:\n{output}")
    print()  # sacrificial line for behave to eat instead of our output

    assert exit_code == 0
    assert len(output) > 0
