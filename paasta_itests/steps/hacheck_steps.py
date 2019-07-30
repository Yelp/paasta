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
from behave import given
from behave import then
from behave import when
from itest_utils import get_service_connection_string

from paasta_tools import drain_lib


@given("a working hacheck container")
def a_working_hacheck_container(context):
    connection_string = get_service_connection_string("hacheck")
    context.hacheck_host, context.hacheck_port = connection_string.split(":")
    context.hacheck_port = int(context.hacheck_port)


@given("a working httpdrain container")
def a_working_httpdrain_container(context):
    connection_string = get_service_connection_string("httpdrain")
    context.hacheck_host, context.hacheck_port = connection_string.split(":")
    context.hacheck_port = int(context.hacheck_port)


@given("a fake task to drain")
def a_fake_task_to_drain(context):
    context.fake_task = mock.Mock(
        id="fake_task_for_itest",
        host=context.hacheck_host,
        ports=[context.hacheck_port],
    )


@given("a HacheckDrainMethod object with delay {delay}")
def a_HacheckDrainMethod_object_with_delay(context, delay):
    context.drain_method = drain_lib.HacheckDrainMethod(
        service="service",
        instance="instance",
        registrations=["one", "two"],
        delay=delay,
        hacheck_port=context.hacheck_port,
    )


@given("a HTTPDrainMethod object")
def a_HttpDrainMethod_object(context):
    context.drain_method = drain_lib.HTTPDrainMethod(
        service="service",
        instance="instance",
        registrations=["one", "two"],
        drain={
            "url_format": "http://{host}:{port}/drain?nerve_ns={nerve_ns}",
            "method": "GET",
            "success_codes": 200,
        },
        stop_draining={
            "url_format": "http://{host}:{port}/drain/stop?nerve_ns={nerve_ns}",
            "method": "GET",
            "success_codes": 200,
        },
        is_draining={
            "url_format": "http://{host}:{port}/drain/status?nerve_ns={nerve_ns}",
            "method": "GET",
            "success_codes": 200,
        },
        is_safe_to_kill={
            "url_format": "http://{host}:{port}/drain/safe_to_kill?nerve_ns={nerve_ns}",
            "method": "GET",
            "success_codes": 200,
        },
    )


@when("we down a task")
def we_down_a_service(context):
    context.down_time = time.time()
    context.event_loop.run_until_complete(context.drain_method.drain(context.fake_task))


@when("we up a task")
def we_up_a_service(context):
    context.event_loop.run_until_complete(
        context.drain_method.stop_draining(context.fake_task)
    )


@then("the task should be downed")
def the_task_should_be_downed(context):
    assert context.event_loop.run_until_complete(
        context.drain_method.is_draining(context.fake_task)
    )


@then("the task should not be downed")
def the_task_should_not_be_downed(context):
    assert not context.event_loop.run_until_complete(
        context.drain_method.is_draining(context.fake_task)
    )


@then("the hacheck task should be safe to kill after {wait_time} seconds")
def hacheck_should_be_safe_to_kill(context, wait_time):
    with mock.patch(
        "time.time", return_value=(context.down_time + float(wait_time)), autospec=True
    ):
        assert context.event_loop.run_until_complete(
            context.drain_method.is_safe_to_kill(context.fake_task)
        )


@then("the task should be safe to kill after {wait_time} seconds")
def should_be_safe_to_kill(context, wait_time):
    time.sleep(int(wait_time))
    assert context.event_loop.run_until_complete(
        context.drain_method.is_safe_to_kill(context.fake_task)
    )


@then("the task should not be safe to kill after {wait_time} seconds")
def should_not_be_safe_to_kill(context, wait_time):
    with mock.patch(
        "time.time", return_value=(context.down_time + float(wait_time)), autospec=True
    ):
        assert not context.event_loop.run_until_complete(
            context.drain_method.is_safe_to_kill(context.fake_task)
        )


@then("every registration should be {status} in hacheck")
def every_registration_should_be_down(context, status):
    res = context.event_loop.run_until_complete(
        context.drain_method.for_each_registration(
            context.fake_task, drain_lib.get_spool
        )
    )
    assert [r["state"] == status for r in res]
