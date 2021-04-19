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
from tempfile import NamedTemporaryFile

from behave import given
from behave import then

from paasta_tools.utils import _run


APP_ID = "test--marathon--app.instance.git01234567.configabcdef01"


@given('a capacity check overrides file with contents "{contents}"')
def write_overrides_file(context, contents):
    with NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(contents)
        context.overridefile = f.name


@then(
    'capacity_check "{check_type}" --crit "{crit:d}" --warn "{warn:d}" should return "{status}" with code "{code:d}"'
)
def capacity_check_status_crit_warn(context, check_type, crit, warn, status, code):
    print(check_type, crit, warn)
    cmd = f"../paasta_tools/monitoring/check_capacity.py {check_type} --crit {crit} --warn {warn}"
    print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    print(output)
    assert exit_code == code
    assert status in output


@then('capacity_check "{check_type}" should return "{status}" with code "{code:d}"')
def capacity_check_type_status(context, check_type, status, code):
    cmd = "../paasta_tools/monitoring/check_capacity.py %s" % check_type
    print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    print(output)
    assert exit_code == code
    assert status in output


@then(
    'capacity_check with override file "{check_type}" and attributes "{attrs}" '
    'should return "{status}" with code "{code:d}"'
)
def capacity_check_type_status_overrides(context, check_type, attrs, status, code):
    cmd = "../paasta_tools/monitoring/check_capacity.py {} --overrides {} --attributes {}".format(
        check_type, context.overridefile, attrs
    )
    print("Running cmd %s" % cmd)
    exit_code, output = _run(cmd)
    print(output)
    assert exit_code == code
    assert status in output
