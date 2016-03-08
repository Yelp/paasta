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
from behave import given
from behave import then
from behave import when
from docker.errors import APIError

from paasta_tools.utils import _run
from paasta_tools.utils import get_docker_client


@given(u'Docker is available')
def docker_is_available(context):
    docker_client = get_docker_client()
    assert docker_client.ping()
    context.docker_client = docker_client


@given(u'a running docker container with task id {task_id} and image {image_name}')
def create_docker_container(context, task_id, image_name):
    container_name = 'paasta-itest-execute-in-containers'
    try:
        context.docker_client.remove_container(container_name, force=True)
    except APIError:
        pass
    context.docker_client.pull(image_name)
    container = context.docker_client.create_container(
        name=container_name,
        image=image_name,
        command='/bin/sleep infinity',
        environment={'MESOS_TASK_ID': task_id},
    )
    context.docker_client.start(container=container.get('Id'))
    context.running_container_id = container.get('Id')


@when(u'we paasta_execute_docker_command a command with exit code {code} in container with task id {task_id}')
def run_command_in_container(context, code, task_id):
    cmd = '../paasta_tools/paasta_execute_docker_command.py -i %s -c "exit %s"' % (task_id, code)
    print 'Running cmd %s' % cmd
    exit_code, output = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    context.return_code = exit_code


@then(u'the exit code is {code}')
def paasta_execute_docker_command_result(context, code):
    assert int(code) == int(context.return_code)


@then(u'the docker container has at most {num} exec instances')
def check_container_exec_instances(context, num):
    """Modern docker versions remove ExecIDs after they finished, but older
    docker versions leave ExecIDs behind. This test is for assering that
    the ExecIDs are cleaned up one way or another"""
    container_info = context.docker_client.inspect_container(context.running_container_id)
    if container_info['ExecIDs'] is None:
        execs = []
    else:
        execs = container_info['ExecIDs']
    print 'Container info:\n%s' % container_info
    assert len(execs) <= int(num)
