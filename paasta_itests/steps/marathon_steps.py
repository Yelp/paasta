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
import time

import mock
from behave import then
from behave import when

import paasta_tools

APP_ID = 'test--marathon--app.instance.git01234567.configabcdef01'


@when(u'we create a trivial marathon app')
def create_trivial_marathon_app(context):
    app_config = {
        'id': APP_ID,
        'cmd': '/bin/sleep 30',
        'instances': 1,
    }
    with mock.patch('paasta_tools.bounce_lib.create_app_lock'):
        paasta_tools.bounce_lib.create_marathon_app(app_config['id'], app_config, context.marathon_client)


@then(u'we should see it running in marathon')
def list_marathon_apps_has_trivial_app(context):
    actual = paasta_tools.marathon_tools.list_all_marathon_app_ids(context.marathon_client)
    assert APP_ID in actual
    assert context.marathon_client.get_app('/%s' % APP_ID)


@then(u'it should show up in marathon_services_running_here')
def marathon_services_running_here_works(context):
    with mock.patch('paasta_tools.mesos_tools.socket.getfqdn', return_value='mesosslave'):
        discovered = paasta_tools.marathon_tools.marathon_services_running_here()
        assert len(discovered) == 1, repr(discovered)

    discovered_service = discovered[1]
    assert discovered_service[0] == u'test_marathon_app', repr(discovered)
    assert discovered_service[1] == 'instance'


@when(u'the task has started')
def when_the_task_has_started(context):
    # 120 * 0.5 = 60 seconds
    for _ in xrange(120):
        app = context.marathon_client.get_app(APP_ID, embed_tasks=True)
        happy_count = len(app.tasks)
        if happy_count >= 1:
            return
        time.sleep(0.5)

    raise Exception("timed out waiting for task to start")
