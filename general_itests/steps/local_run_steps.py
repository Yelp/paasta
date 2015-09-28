import os

from behave import given, when, then
from path import Path

from paasta_tools.utils import _run


@given(u'a simple service to test')
def given_simple_service(context):
    context.fake_service_name = "fake_simple_service"
    assert os.path.isfile(os.path.join(context.fake_service_name, "Dockerfile"))
    assert os.path.isfile(os.path.join(context.fake_service_name, "Makefile"))


@when(u'we run paasta local-run in non-interactive mode with environment variable "{var}" set to "{val}"')
def non_interactive_local_run(context, var, val):
    with Path("fake_simple_service"):
        # The local-run invocation here is designed to run and return a sentinel
        # exit code that we can look out for. It also sleeps a few seconds
        # because the local-run code currently crashes when the docker
        # container dies before it gets a chance to lookup the continerid
        # (which causes jenkins flakes) The sleep can be removed once local-run
        # understands that containers can die quickly.
        localrun_cmd = ("paasta_cli.py local-run "
                        "--yelpsoa-root ../fake_soa_configs_local_run/ "
                        "-s fake_simple_service "
                        "--cluster test-cluster "
                        "--cmd '/bin/sh -c \"echo \"%s=$%s\" && sleep 2s && exit 42\"'" % (var, val))
        context.local_run_return_code, context.local_run_output = _run(command=localrun_cmd, timeout=30)


@then(u'we should see the expected return code')
def see_expected_return_code(context):
    print context.local_run_output
    print context.local_run_return_code
    assert context.local_run_return_code == 42


@then(u'we should see the environment variable "{var}" with the value "{val}" in the ouput')
def env_var_in_output(context, var, val):
    assert "%s=%s" % (var, val) in context.local_run_output
