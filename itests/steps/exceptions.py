# Copyright 2019 Yelp Inc.
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
import behave
from hamcrest import assert_that
from hamcrest import equal_to


@behave.then('a (?P<error_type>.*Error|.*Exception) is raised')
def check_exception(context, error_type):
    assert_that(type(context.exception).__name__, equal_to(error_type))


@behave.then('no exception is raised')
def check_no_exception(context):
    if hasattr(context, 'exception'):
        raise context.exception
