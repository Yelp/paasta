import os

from behave import given, when, then
from path import Path

from paasta_tools.utils import _run


@given(u'a simple service to test')
def given_simple_service(context):
    context.fake_service_name = "fake_simple_service"
    assert os.path.isfile(os.path.join(context.fake_service_name, "Dockerfile"))


@when(u'we run paasta local-run in non-interactive mode')
def non_interactive_local_run(context):
    with Path("fake_simple_service"):
        localrun_cmd = ("paasta_cli.py local-run "
                        "--yelpsoa-root ../fake_soa_configs_local_run/ "
                        "-s fake_simple_service "
                        "--cluster test-cluster "
                        "--cmd '/bin/sh -c \"exit 42\"'")
        context.local_run_return_code, context.local_run_output = _run(command=localrun_cmd, timeout=30)


@then(u'we should see the expected return code')
def see_expected_return_code(context):
    print context.local_run_output
    print context.local_run_return_code
    assert context.local_run_return_code == 42
