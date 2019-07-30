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
import signal

from behave import then
from behave import when

from paasta_tools.utils import _run


@when("we run a trivial command with timeout {timeout} seconds")
def run_command(context, timeout):
    fake_cmd = "sleep 1"
    context.rc, context.output = _run(fake_cmd, timeout=float(timeout))


@then("the command is killed with signal {killsignal}")
def check_exit_code(context, killsignal):
    assert context.rc == -1 * getattr(signal, killsignal)
