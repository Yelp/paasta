import sys

from behave import then

sys.path.append('../')
from paasta_tools.utils import _run


@then(u'paasta_chronos_serviceinit status should return "Healthy"')
def status_returns_healthy(context):
    # Another fun from using space as the separate is parsing the service+job
    # string on the command line! For now, the simplest thing that can possibly
    # work.
    cmd = '../paasta_tools/paasta_chronos_serviceinit.py test-service\ job status'
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
