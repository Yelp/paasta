from behave import given
from behave import then
from behave import when

from paasta_tools.autoscaling import autoscaling_service_lib


@given("some fake historical load data")
def make_fake_historical_load_data(context):
    context.fake_historical_load_data = [(1, 10), (2, 20), (3, 10), (4, 1337)]


@when("I save the fake historical load data")
def save_fake_historical_load_data(context):
    autoscaling_service_lib.save_historical_load(
        context.fake_historical_load_data, "/itest/fake_historical_load_data"
    )


@then("I should get the same fake historical load data back when I fetch it")
def load_fake_historical_load_data(context):
    actual = autoscaling_service_lib.fetch_historical_load(
        "/itest/fake_historical_load_data"
    )
    expected = context.fake_historical_load_data
    assert actual == expected
