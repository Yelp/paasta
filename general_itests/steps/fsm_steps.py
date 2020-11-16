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
import shutil
import tempfile

import mock
from behave import given
from behave import then
from behave import when
from service_configuration_lib import read_services_configuration

from paasta_tools.cli.fsm_cmd import paasta_fsm
from paasta_tools.utils import SystemPaastaConfig


@given("a fake yelpsoa-config-root with an existing service")
def step_impl_given(context):
    # Cleaned up in after_scenario()
    context.tmpdir = tempfile.mkdtemp("paasta_tools_fsm_itest")
    context.fake_yelpsoa_configs = os.path.join(context.tmpdir, "yelpsoa-configs")
    fake_yelpsoa_configs_src = os.path.join("fake_soa_configs_fsm")
    shutil.copytree(
        src=fake_yelpsoa_configs_src, dst=context.fake_yelpsoa_configs, symlinks=True
    )


def _load_yelpsoa_configs(context, service):
    all_services = read_services_configuration(soa_dir=context.fake_yelpsoa_configs)
    context.my_config = all_services[service]


@when("we fsm a new service")
def step_impl_when_fsm_auto(context):
    service = "my-cool-service"

    fake_args = mock.Mock(yelpsoa_config_root=context.fake_yelpsoa_configs)
    with mock.patch(
        "paasta_tools.cli.fsm_cmd.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig(
            config={}, directory=context.fake_yelpsoa_configs
        )
        paasta_fsm(fake_args)

    _load_yelpsoa_configs(context, service)


@then("the new yelpsoa-configs directory has a valid smartstack proxy_port")
def step_impl_then_proxy_port(context):
    port = context.my_config["smartstack"]["main"]["proxy_port"]
    assert port >= 19000
    assert port <= 21000
    assert port != 20666
