import sys

from behave import when, then

sys.path.append('../')
from paasta_tools.utils import _run


@when(u'we run the chronos job test-service.job')
def run_chronos_test_job(context):
    pass


@then(u'paasta_chronos_serviceinit status should return "Healthy"')
def status_returns_healthy(context):
    cmd = '../paasta_tools/paasta_chronos_serviceinit.py test-service.job status'
    print 'Running cmd %s' % cmd
    (exit_code, output) = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)

    assert exit_code == 0
    assert "Healthy" in output
