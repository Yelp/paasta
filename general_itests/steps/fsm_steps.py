from behave import when, then, given

@given(u'a fake yelpsoa-config-root')
def step_impl_given(context):
    assert False

@when(u'we fsm a new service with --auto')
def step_impl_when_auto(context):
    assert False

@then(u'the new yelpsoa-configs directory has sane values')
def step_impl_then(context):
    assert False
