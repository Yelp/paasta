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

from paasta_tools.utils import _run


@given(u'a simple service to test')
def given_simple_service(context):
    context.fake_service_name = "fake_simple_service"
    assert os.path.isfile(os.path.join(context.fake_service_name, "Dockerfile"))
    assert os.path.isfile(os.path.join(context.fake_service_name, "Makefile"))


@when(u'we run paasta local-run on a Marathon service in non-interactive mode '
      'with environment variable "{var}" set to "{val}"')
def non_interactive_local_run(context, var, val):
    with Path("fake_simple_service"):
        # The local-run invocation here is designed to run and return a sentinel
        # exit code that we can look out for. It also sleeps a few seconds
        # because the local-run code currently crashes when the docker
        # container dies before it gets a chance to lookup the continerid
        # (which causes jenkins flakes) The sleep can be removed once local-run
        # understands that containers can die quickly.
        localrun_cmd = ("paasta local-run "
                        "--yelpsoa-config-root ../fake_soa_configs_local_run/ "
                        "--service fake_simple_service "
                        "--cluster test-cluster "
                        "--instance main "
                        "--build "
                        '''--cmd '/bin/sh -c "echo \\"%s=$%s\\" && sleep 2s && exit 42"' ''' % (var, val))
        context.local_run_return_code, context.local_run_output = _run(command=localrun_cmd, timeout=60)


@then(u'we should see the expected return code')
def see_expected_return_code(context):
    print(context.local_run_output)
    print(context.local_run_return_code)
    assert context.local_run_return_code == 42


@then(u'we should see the environment variable "{var}" with the value "{val}" in the ouput')
def env_var_in_output(context, var, val):
    assert "%s=%s" % (var, val) in context.local_run_output


@when(u'we run paasta local-run in non-interactive mode on a chronos job')
def local_run_on_chronos_job(context):
    with Path("fake_simple_service"):
        # The local-run invocation here is designed to run and return a sentinel
        # exit code that we can look out for. It also sleeps a few seconds
        # because the local-run code currently crashes when the docker
        # container dies before it gets a chance to lookup the continerid
        # (which causes jenkins flakes) The sleep can be removed once local-run
        # understands that containers can die quickly.
        local_run_cmd = ("paasta local-run "
                         "--yelpsoa-config-root ../fake_soa_configs_local_run/ "
                         "--service fake_simple_service "
                         "--cluster test-cluster "
                         "--instance chronos_job "
                         "--build "
                         "--cmd '/bin/sh -c \"sleep 2s && exit 42\"'")
        context.local_run_return_code, context.local_run_output = _run(command=local_run_cmd, timeout=60)


@when(u'we run paasta local-run on an interactive job')
def local_run_on_adhoc_job(context):
    with Path("fake_simple_service"):
        local_run_cmd = ("paasta local-run "
                         "--yelpsoa-config-root ../fake_soa_configs_local_run/ "
                         "--service fake_simple_service "
                         "--cluster test-cluster "
                         "--instance sample_adhoc_job "
                         "--build ")
        context.local_run_return_code, context.local_run_output = _run(command=local_run_cmd, timeout=60)


@when(u'we run paasta local-run in non-interactive mode on a chronos job with cmd set to \'echo hello && sleep 5\'')
def local_run_on_chronos_job_with_cmd(context):
    with Path("fake_simple_service"):
        local_run_cmd = ("paasta local-run "
                         "--yelpsoa-config-root ../fake_soa_configs_local_run/ "
                         "--service fake_simple_service "
                         "--cluster test-cluster "
                         "--instance chronos_job_with_cmd "
                         "--build ")
        context.local_run_return_code, context.local_run_output = _run(command=local_run_cmd, timeout=60)
