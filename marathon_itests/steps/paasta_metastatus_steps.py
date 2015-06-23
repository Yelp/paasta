from behave import when, then

from paasta_tools.utils import _run


# get zk path from fig
# write that into a mesos-cli.json
# pass path to written mesos-cli.json via env var to paasta_metatstatus (which imports mesos-cli which uses the env var to find its configuration)

# somehow block access to mesos master

@when(u'we kill a master')
def kill_master(context):
    pass


@then(u'metastatus returns 2')
def check_metastatus_return(context):
    # We don't want to invoke the "paasta metastatus" wrapper because by
    # default it will check every cluster. This is also the way sensu invokes
    # this check.
    cmd = ("../paasta_tools/paasta_metastatus.py")
    print "Running cmd %s" % cmd
    (returncode, output) = _run(cmd)
    print "Got returncode %s with output:" % returncode
    print output

    assert returncode == 2
