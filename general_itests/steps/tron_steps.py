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
from behave import given
from behave import when

from paasta_tools.utils import _run


@given("some tronfig")
def step_some_tronfig(context):
    context.soa_dir = "fake_soa_configs_tron"


@when("we run paasta_setup_tron_namespace in dry-run mode")
def step_run_paasta_setup_tron_namespace_dry_run(context):
    cmd = (
        f"paasta_setup_tron_namespace --dry-run -a --soa-dir {context.soa_dir}"
        f" --cluster test-cluster"
    )
    context.return_code, context.output = _run(command=cmd)
