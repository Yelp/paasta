import os
import contextlib
import sys
from behave import when, then
import mock

sys.path.append('../')
from paasta_tools.utils import _run
from paasta_tools import marathon_tools
from paasta_tools import setup_marathon_job

fake_service_name = 'fake_complete_service'
fake_instance_name = 'fake_instance'
fake_appid = 'fake--complete--service.gitdeadbeef.configdeadbeef2'
fake_service_config = {
        'id': 'bounce.test1.newapp',
        'cmd': '/bin/sleep infinity',
        'instances': 1,
        'backoff_seconds': 0.1,
        'backoff_factor': 1,
        'mem': 480,
    }


@when(u'all zookeepers are unavailable')
def all_zookeepers_unavailable(context):
    pass


@when(u'all mesos masters are unavailable')
def all_mesos_masters_unavailable(context):
    pass

@when(u'a task consuming most available memory is launched')
def run_paasta_metastatus_high_load(context):
    with contextlib.nested(
        mock.patch(
            'paasta_tools.bounce_lib.get_happy_tasks',
            autospec=True,
            side_effect=lambda t, _, __, **kwargs: t[:context.max_happy_tasks],
        ),
        mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', autospec=True),
        mock.patch('paasta_tools.bounce_lib.create_app_lock', autospec=True),
        mock.patch('paasta_tools.bounce_lib.time.sleep', autospec=True),
        mock.patch(
            'paasta_tools.setup_marathon_job.marathon_tools.get_cluster',
            autospec=True,
            return_value=context.cluster,
        ),
    ):
        setup_marathon_job.deploy_service(
            fake_service_name,
            fake_instance_name,
            fake_appid,
            fake_service_config,
            context.client,
            'upthendown',
            fake_instance_name,
            {},
        )
        context.client.list_tasks()


@then(u'metastatus returns 2')
def check_metastatus_return(context):
    # We don't want to invoke the "paasta metastatus" wrapper because by
    # default it will check every cluster. This is also the way sensu invokes
    # this check.
    cmd = '../paasta_tools/paasta_metastatus.py'
    env = dict(os.environ)
    env['MESOS_CLI_CONFIG'] = context.mesos_cli_config_filename
    print 'Running cmd %s with MESOS_CLI_CONFIG=%s' % (cmd, env['MESOS_CLI_CONFIG'])
    (returncode, output) = _run(cmd, env=env)
    print 'Got returncode %s with output:' % returncode
    print output

    assert returncode == 2
