import signal

from behave import when, then

from paasta_tools.utils import _run


@when(u'we run a trivial command with timeout {timeout} seconds')
def run_command(context, timeout):
    fake_cmd = 'sleep 1'
    context.rc, context.output = _run(fake_cmd, timeout=float(timeout))


@then(u'the command is killed with signal {killsignal}')
def check_exit_code(context, killsignal):
    assert context.rc == -1 * getattr(signal, killsignal)
