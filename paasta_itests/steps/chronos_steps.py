from behave import when, then


@when(u'we create a trivial chronos job')
def create_trivial_chronos_job(context):
    job = {
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        'name': 'test_chronos_job',
        'owner': '',
        'disabled': True,
        'schedule': 'R/2014-01-01T00:00:00Z/PT60M',
    }
    context.chronos_client.add(job)


@then(u'we should be able to see it when we list jobs')
def list_chronos_jobs_has_trivial_job(context):
    jobs = context.chronos_client.list()
    job_names = [job['name'] for job in jobs]
    assert 'test_chronos_job' in job_names
