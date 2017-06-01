from __future__ import absolute_import
from __future__ import unicode_literals

from subprocess import call
from subprocess import PIPE
from subprocess import Popen

from pytest_bdd import given
from pytest_bdd import parsers
from pytest_bdd import then
from pytest_bdd import when

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.sync import Sync


@given('docker-compose build')
def docker_compose_build():
    call(['docker-compose', 'down'])
    call(['docker-compose', 'pull'])
    call(['docker-compose', '--verbose', 'build'])


@given('mesos zookeeper')
def mesos_zookeeper():
    return Popen(
        ['docker-compose', 'run', '--rm', 'zookeeper'],
        stdout=PIPE,
        stderr=PIPE
    )


@given('mesos zookeeper')
def mesos_master():
    return Popen(
        ['docker-compose', 'run', '--rm', 'mesosmaster'],
        stdout=PIPE,
        stderr=PIPE
    )


@given(parsers.re(r'(?P<num_slaves>\d+) mesos slave(s?)'),
       converters=dict(n=int))
def mesos_slaves(num_slaves):
    return [
        Popen(
            ['docker-compose', 'run', '--rm',
             '-e', 'MESOS_PORT={}'.format(6050 + i),
             '-p', '{}:{}'.format(6050 + i),
             'mesosslave'],
            stdout=PIPE,
            stderr=PIPE
        )
        for i in range(num_slaves)
    ]


@given(
    parsers.re(r'working mesos platform (with (?P<num_slaves>\d+) slave(s?))'),
    converters=dict(num_slaves=int))
def mesos_platform(mesos_zookeeper, mesos_master, mesos_slaves, num_slaves):
    return {'zookeeper': mesos_zookeeper,
            'master': mesos_master,
            'slaves': mesos_slaves}


@given('mesos executor with {runner} runner')
def mesos_executor_runner(runner):
    executor = MesosExecutor()

    if runner == 'sync':
        runner_instance = Sync(executor=executor)
    else:
        raise "unknown runner: {}".format(runner)

    return {'executor': executor, 'runner': runner_instance}


@when('I launch a task')
def launch_task(mesos_executor_runner):
    print(mesos_executor_runner)
    return


@then('it should block until finished')
def block_until_finished():
    return
