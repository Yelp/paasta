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
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import mock
from behave import given
from behave import then
from behave import when
from itest_utils import get_service_connection_string

from paasta_tools import drain_lib


@given('a working hacheck container')
def a_working_hacheck_container(context):
    connection_string = get_service_connection_string('hacheck')
    context.hacheck_host, context.hacheck_port = connection_string.split(':')
    context.hacheck_port = int(context.hacheck_port)


@given('a fake task to drain')
def a_fake_task_to_drain(context):
    context.fake_task = mock.Mock(id='fake_task_for_itest', host=context.hacheck_host, ports=[5])


@given('a HacheckDrainMethod object with delay {delay}')
def a_HacheckDrainMethod_object_with_delay(context, delay):
    context.drain_method = drain_lib.HacheckDrainMethod(
        service="service",
        instance="instance",
        nerve_ns="namespace",
        delay=delay,
        hacheck_port=context.hacheck_port,
    )


@when('we down a task')
def we_down_a_service(context):
    context.down_time = time.time()
    context.drain_method.drain(context.fake_task)


@then('the task should be downed')
def the_task_should_be_downed(context):
    assert context.drain_method.is_draining(context.fake_task)


@then('the task should be safe to kill after {wait_time} seconds')
def should_be_safe_to_kill(context, wait_time):
    with mock.patch('time.time', return_value=(context.down_time + float(wait_time))):
        assert context.drain_method.is_safe_to_kill(context.fake_task)


@then('the task should not be safe to kill after {wait_time} seconds')
def should_not_be_safe_to_kill(context, wait_time):
    with mock.patch('time.time', return_value=(context.down_time + float(wait_time))):
        assert not context.drain_method.is_safe_to_kill(context.fake_task)
