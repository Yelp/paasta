import os

from behave import when, then

from paasta_tools.utils import _run

@when(u'all zookeepers are unavailable')
def all_zookeepers_unavailable(context):
    pass


@when(u'all mesos masters are unavailable')
def all_mesos_masters_unavailable(context):
    pass


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
