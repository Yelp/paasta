from behave import when, then


@when(u'we run the chronos job test-service.main')
def run_chronos_test_job(context):
    pass


@then(u'paasta_chronos_serviceinit status should return "Healthy"')
def status_returns_healthy(context):
    pass
