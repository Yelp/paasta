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
from behave import then
from behave import when

from paasta_tools.utils import _run
from paasta_tools.utils import remove_ansi_escape_sequences

CONTAINER = {"type": "DOCKER", "docker": {"network": "BRIDGE", "image": "busybox"}}


@when("all zookeepers are unavailable")
def all_zookeepers_unavailable(context):
    pass


@when("all mesos masters are unavailable")
def all_mesos_masters_unavailable(context):
    pass


@then(
    'paasta_metastatus{flags} exits with return code "{expected_return_code}" and output "{expected_output}"'
)
def check_metastatus_return_code_with_flags(
    context, flags, expected_return_code, expected_output
):
    # We don't want to invoke the "paasta metastatus" wrapper because by
    # default it will check every cluster. This is also the way sensu invokes
    # this check.
    cmd = "python -m paasta_tools.paasta_metastatus%s" % flags
    print("Running cmd %s" % (cmd))
    exit_code, output = _run(cmd)

    # we don't care about the colouring here, so remove any ansi escape sequences
    escaped_output = remove_ansi_escape_sequences(output)
    print(f"Got exitcode {exit_code} with output:\n{output}")
    print()

    assert exit_code == int(expected_return_code)
    assert expected_output in escaped_output


@then(
    'paasta_metastatus exits with return code "{expected_return_code}" and output "{expected_output}"'
)
def check_metastatus_return_code_no_flags(
    context, expected_return_code, expected_output
):
    check_metastatus_return_code_with_flags(
        context=context,
        flags="",
        expected_return_code=expected_return_code,
        expected_output=expected_output,
    )
