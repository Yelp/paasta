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
import time

import mock
from behave import then
from behave import when

import paasta_tools


APP_ID = "test--marathon--app.instance.git01234567.configabcdef01"


@when("we create a trivial marathon app")
def create_trivial_marathon_app(context):
    app_config = {
        "id": APP_ID,
        "cmd": "/bin/sleep 30",
        "container": {
            "type": "DOCKER",
            "docker": {"network": "BRIDGE", "image": "busybox"},
        },
        "instances": 3,
        "constraints": [["hostname", "UNIQUE"]],
    }
    paasta_tools.bounce_lib.create_marathon_app(
        app_config["id"], app_config, context.marathon_clients.current[0]
    )


@then("we should see it running in marathon")
def list_marathon_apps_has_trivial_app(context):
    actual = paasta_tools.marathon_tools.list_all_marathon_app_ids(
        context.marathon_clients.current[0]
    )
    assert APP_ID in actual
    assert context.marathon_clients.current[0].get_app("/%s" % APP_ID)


@then("it should show up in marathon_services_running_here")
def marathon_services_running_here_works(context):
    with mock.patch(
        "paasta_tools.mesos_tools.socket.getfqdn",
        return_value="mesosslave",
        autospec=True,
    ):
        discovered = paasta_tools.marathon_tools.marathon_services_running_here()
        assert discovered == [("test_marathon_app", "instance", mock.ANY)]


@when("the task has started")
def when_the_task_has_started(context):
    # 120 * 0.5 = 60 seconds
    for _ in range(120):
        app = context.marathon_clients.current[0].get_app(APP_ID)
        happy_count = app.tasks_running
        if happy_count >= 3:
            return
        time.sleep(0.5)

    raise Exception("timed out waiting for task to start")
