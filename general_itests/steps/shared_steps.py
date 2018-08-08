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

from paasta_tools.utils import paasta_print


@then('it should have a return code of "{code:d}"')
def see_expected_return_code(context, code):
    paasta_print(context.output)
    paasta_print(context.return_code)
    paasta_print()
    assert context.return_code == code


@then('the output should contain "{output_string}"')
def output_contains(context, output_string):
    paasta_print(output_string)
    assert output_string in context.output
