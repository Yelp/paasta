# Copyright 2015 Yelp Inc.
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

import sys

from behave import when, then
import mock

sys.path.append('../')
import paasta_tools


@when(u'we create a trivial marathon app')
def create_trivial_marathon_app(context):
    app_config = {
        'id': 'test--marathon--app',
        'cmd': '/bin/true',
    }
    with mock.patch('paasta_tools.bounce_lib.create_app_lock'):
        paasta_tools.bounce_lib.create_marathon_app(app_config['id'], app_config, context.marathon_client)


@then(u'we should see it running in marathon')
def list_marathon_apps_has_trivial_app(context):
    assert 'test--marathon--app' in paasta_tools.marathon_tools.list_all_marathon_app_ids(context.marathon_client)
    assert context.marathon_client.get_app('/test--marathon--app')
