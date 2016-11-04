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
from behave import then
from behave import when

from paasta_tools.cli.utils import x_mark
from paasta_tools.utils import _run


@given(u'a "{service_type}" service')
@given(u'an "{service_type}" service')
def given_service(context, service_type):
    context.service = 'fake_%s_service' % service_type
    context.soa_dir = 'fake_soa_configs_validate'


@when(u'we run paasta validate')
def run_paasta_validate(context):
    validate_cmd = ("paasta validate "
                    "--yelpsoa-config-root %s "
                    "--service %s " % (context.soa_dir, context.service))
    context.validate_return_code, context.validate_output = _run(command=validate_cmd)
    context.validate_output = context.validate_output.decode('utf-8')


@then(u'it should have a return code of "{code:d}"')
def see_expected_return_code(context, code):
    print(context.validate_output)
    print(context.validate_return_code)
    print()
    assert context.validate_return_code == code


@then(u'everything should pass')
def validate_status_all_pass(context):
    assert not context.validate_output or x_mark().decode('utf-8') not in context.validate_output


@then(u'it should report an error in the output')
def validate_status_something_fail(context):
    assert x_mark().decode('utf-8') in context.validate_output


@then(u'the output should contain \'{output_string}\'')
def output_contains(context, output_string):
    print(output_string)
    assert output_string in context.validate_output
