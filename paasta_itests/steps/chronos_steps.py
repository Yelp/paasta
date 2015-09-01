from behave import when, then

from paasta_tools import chronos_tools


@when(u'we create a trivial chronos job')
def create_trivial_chronos_job(context):
    job_config = {
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        # If I put an actual space in here, our cleanup delete fails:
        # after_scenario: chronos job test-service job is running. Deleting.
        # ERROR:chronos:Response not valid json: Error: 400
        #
        # If I put a dot in here, the add() fails:
        # ERROR:chronos:Response not valid json: requirement failed: the job's
        # name is invalid. Allowed names: '([\w\s#_-]+)'
        #
        # So I'm punting and putting the string SPACER. That's sure to work,
        # right?
        'name': 'test-serviceSPACERjob',
        'owner': '',
        'disabled': True,
        'schedule': 'R/2014-01-01T00:00:00Z/PT60M',
    }
    context.chronos_client.add(job_config)


@when(u'the trivial chronos job appears in the job list')
def chronos_job_is_ready(context):
    """Wait for a job with a matching job id to be ready. """
    chronos_tools.wait_for_job(context.chronos_client, 'test-serviceSPACERjob')


@then(u'we should be able to see it when we list jobs')
def list_chronos_jobs_has_trivial_job(context):
    jobs = context.chronos_client.list()
    job_names = [job['name'] for job in jobs]
    assert 'test-serviceSPACERjob' in job_names
