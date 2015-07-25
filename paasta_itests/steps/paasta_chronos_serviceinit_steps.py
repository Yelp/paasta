import sys

from behave import when, then

sys.path.append('../')
from paasta_tools.utils import _run


@when(u'we run the chronos job test-service.job')
def run_chronos_test_job(context):
    job = {
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        'name': 'test-service job',
        'owner': '',
        'disabled': True,
        'schedule': 'R/2014-01-01T00:00:00Z/PT60M',
    }
    context.chronos_client.add(job)


@then(u'paasta_chronos_serviceinit status should return "Healthy"')
def status_returns_healthy(context):
    cmd = '../paasta_tools/paasta_chronos_serviceinit.py test-service.job status'
    print 'Running cmd %s' % cmd
    (exit_code, output) = _run(cmd)
    print 'Got exitcode %s with output:\n%s' % (exit_code, output)
    print 'extra line that gets clobbered by behave :('
    import pdb; pdb.set_trace()

    assert exit_code == 0
    assert "Healthy" in output
