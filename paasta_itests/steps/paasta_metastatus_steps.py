import os
import sys

from behave import when, then

sys.path.append('../')
from paasta_tools.utils import _run
from paasta_tools import marathon_tools
from paasta_tools import chronos_tools
from marathon import MarathonApp


@when(u'all zookeepers are unavailable')
def all_zookeepers_unavailable(context):
    pass


@when(u'all mesos masters are unavailable')
def all_mesos_masters_unavailable(context):
    pass


@when(u'an app with id "{app_id}" using high memory is launched')
def run_paasta_metastatus_high_mem(context, app_id):
    context.marathon_client.create_app(app_id, MarathonApp(cmd='/bin/sleep infinity', mem=490, instances=1))


@when(u'a chronos job with name "{job_name}" is launched')
def chronos_job_launched(context, job_name):
    job = {
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        'name': job_name,
        'owner': 'me@foo.com',
        'disabled': True,
        'schedule': 'R/2014-01-01T00:00:00Z/PT60M',
    }
    context.chronos_client.add(job)


@when(u'an app with id "{app_id}" using high cpu is launched')
def run_paasta_metastatus_high_cpu(context, app_id):
    context.marathon_client.create_app(app_id, MarathonApp(cmd='/bin/sleep infinity', cpus=9, instances=1))


@when(u'a task belonging to the app with id "{app_id}" is in the task list')
def task_is_ready(context, app_id):
    """ wait for a task with a matching task name to be ready. time out in 60 seconds """
    marathon_tools.wait_for_app_to_launch_tasks(context.marathon_client, app_id, 1)


@when(u'a job with the name "{job_name}" is in the job list')
def job_is_ready(context, job_name):
    """ wait for a job with a matching job id to be ready. """
    chronos_tools.wait_for_job(context.chronos_client, job_name)


@then(u'paasta_metastatus exits with return code "{expected_return_code}" and output "{expected_output}"')
def check_metastatus_return_code(context, expected_return_code, expected_output):
    # We don't want to invoke the "paasta metastatus" wrapper because by
    # default it will check every cluster. This is also the way sensu invokes
    # this check.
    cmd = '../paasta_tools/paasta_metastatus.py'
    env = dict(os.environ)
    env['MESOS_CLI_CONFIG'] = context.mesos_cli_config_filename
    print 'Running cmd %s with MESOS_CLI_CONFIG=%s' % (cmd, env['MESOS_CLI_CONFIG'])
    (exit_code, output) = _run(cmd, env=env)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)

    assert exit_code == int(expected_return_code)
    assert expected_output in output
