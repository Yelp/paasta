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
import contextlib

import mock
from behave import then
from behave import when
from itest_utils import get_service_connection_string

import paasta_tools.mesos.master
from paasta_tools import check_mesos_resource_utilization


@when(u'we check mesos utilization with a threshold of {percent} percent')
def check_mesos_utilization(context, percent):
    config = {
        "master": "%s" % get_service_connection_string('mesosmaster'),
        "scheme": "http",
        "response_timeout": 5,
    }

    with contextlib.nested(
        mock.patch('paasta_tools.check_mesos_resource_utilization.send_event'),
        mock.patch.object(paasta_tools.mesos.master, 'CFG', config),
    ) as (
        mock_events,
        mock_cfg,
    ):
        context.mesos_util_check = check_mesos_resource_utilization.check_thresholds(int(percent))


@then(u'the result is {result}')
def mesos_util_result(context, result):
    assert result in context.mesos_util_check
