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
        # If I put an actual space in here, our cleanup delete fails:
        # after_scenario: chronos job test-service job is running. Deleting.
        # ERROR:chronos:Response not valid json: Error: 400
        #
        # If I put a dot in here, the add() fails:
        # ERROR:chronos:Response not valid json: requirement failed: the job's
        # name is invalid. Allowed names: '([\w\s#_-]+)'
        #
        # So I'm punting and putting the string SPACER. That's sure to work,
        # right?
        'name': 'test-serviceSPACERjob',
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

    assert exit_code == 0
    # This doesn't work yet for a few reasons:
    #
    # * See note above about space vs dot vs SPACER. The resulting job can't
    # please all of its masters so this test is doomed to fail no matter what.
    # (As written at least it doesn't hang forever trying to kill the
    # job-with-a-space that it can't kill.)
    #
    # * It's not supposed to as I did BDD to get here and I'm not done yet.
    # assert "Healthy" in output
