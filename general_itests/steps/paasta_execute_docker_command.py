import sys

from behave import given, when, then
from docker import Client

sys.path.append('../')
from paasta_tools.utils import _run
from paasta_tools.utils import get_docker_host


@given(u'a running docker container with task id {task_id}')
def create_docker_container(context, task_id):
    base_docker_url = get_docker_host()
    docker_client = Client(base_url=base_docker_url)
    context.docker_client = docker_client
    container = docker_client.create_container(
        name='paasta-itest-execute-in-containers',
        image='docker-dev.yelpcorp.com/trusty_yelp:latest',
        command='/bin/sleep infinity',
        environment={'MESOS_TASK_ID': task_id},
    )
    docker_client.start(container=container.get('Id'))
    context.running_container_id = container.get('Id')


@when(u'we paasta_execute_docker_command a command with exit code {code} in container with task id {task_id}')
def run_command_in_container(context, code, task_id):
    cmd = '../paasta_tools/paasta_execute_docker_command.py -i %s -c "exit %s"' % (task_id, code)
    print 'Running cmd %s' % cmd
    (exit_code, output) = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    context.return_code = exit_code


@then(u'the exit code is {code}')
def paasta_execute_docker_command_result(context, code):
    assert int(code) == int(context.return_code)


@then(u'the docker container has {num} exec instances')
def check_container_exec_instances(context, num):
    container_info = context.docker_client.inspect_container(context.running_container_id)
    print 'Container info:\n%s' % container_info
    assert len(container_info['ExecIDs']) == int(num)
