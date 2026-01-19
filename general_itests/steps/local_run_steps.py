# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from behave import given
from behave import then
from behave import when
from path import Path

from paasta_tools.cli.cmds.local_run import RequiredEnvironment
from paasta_tools.utils import _run


@given("a simple service to test")
def given_simple_service(context):
    context.fake_service_name = "fake_simple_service"
    assert os.path.isfile(os.path.join(context.fake_service_name, "Dockerfile"))
    assert os.path.isfile(os.path.join(context.fake_service_name, "Makefile"))


@when(
    "we run paasta local-run on a Kubernetes service in non-interactive mode "
    'with environment variable "{var}" set to "{val}"'
)
def non_interactive_local_run(context, var, val):
    with Path("fake_simple_service"):
        # The local-run invocation here is designed to run and return a sentinel
        # exit code that we can look out for. It also sleeps a few seconds
        # because the local-run code currently crashes when the docker
        # container dies before it gets a chance to lookup the containerid
        # (which causes jenkins flakes) The sleep can be removed once local-run
        # understands that containers can die quickly.
        #
        # We dump the full environment via /proc/self/environ so we can verify
        # all contract env vars are present.
        localrun_cmd = (
            "paasta local-run "
            "--yelpsoa-config-root ../fake_soa_configs_local_run/ "
            "--service fake_simple_service "
            "--cluster test-cluster "
            "--instance main "
            "--build "
            """--cmd '/bin/sh -c "cat /proc/self/environ | tr \\"\\\\0\\" \\"\\\\n\\" && sleep 2s && exit 42"' """
        )
        context.return_code, context.output = _run(command=localrun_cmd, timeout=90)


@then(
    'we should see the environment variable "{var}" with the value "{val}" in the output'
)
def env_var_in_output(context, var, val):
    assert f"{var}={val}" in context.output


@then("all PaaSTA contract env vars should be present")
def all_contract_env_vars_present(context):
    """Verify all RequiredEnvironment keys have non-empty values in the output."""
    for key in RequiredEnvironment.__required_keys__:
        assert f"{key}=" in context.output, f"Missing contract env var: {key}"


@when("we run paasta local-run on an interactive job")
def local_run_on_adhoc_job(context):
    with Path("fake_simple_service"):
        local_run_cmd = (
            "paasta local-run "
            "--yelpsoa-config-root ../fake_soa_configs_local_run/ "
            "--service fake_simple_service "
            "--cluster test-cluster "
            "--instance sample_adhoc_job "
            "--build "
        )
        context.return_code, context.output = _run(command=local_run_cmd, timeout=90)


@when("we run paasta local-run on a tron action")
def local_run_on_tron_action(context):
    with Path("fake_simple_service"):
        local_run_cmd = (
            "paasta local-run "
            "--yelpsoa-config-root ../fake_soa_configs_local_run/ "
            "--service fake_simple_service "
            "--cluster test-cluster "
            "--instance sample_tron_job.action1 "
            "--build "
        )
        context.return_code, context.output = _run(command=local_run_cmd, timeout=90)
