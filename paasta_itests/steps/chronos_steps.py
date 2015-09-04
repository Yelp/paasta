from behave import when, then

from paasta_tools import chronos_tools


@when(u'we create a trivial chronos job')
def create_trivial_chronos_job(context):
    job_config = {
        'async': False,
        'command': 'echo 1',
        'epsilon': 'PT15M',
        'name': 'test-service job git12345678 config90abcdef',
        'owner': '',
        'disabled': True,
        'schedule': 'R/2014-01-01T00:00:00Z/PT60M',
    }
    context.chronos_client.add(job_config)
    context.chronos_job_name = job_config['name']


@when(u'we create a chronos job from the configs for instance "{instance_name}" of service "{service_name}"')
def create_chronos_job_from_configs(context, instance_name, service_name):
    chronos_job_config = chronos_tools.create_complete_config(service_name, instance_name, context.soa_dir)
    context.chronos_client.add(chronos_job_config)
    context.chronos_job_name = chronos_job_config['name']


@when(u'the chronos job appears in the job list')
def chronos_job_is_ready(context):
    """Wait for a job with a matching job id to be ready. """
    chronos_tools.wait_for_job(context.chronos_client, context.chronos_job_name)


@then(u'we should be able to see it when we list jobs')
def list_chronos_jobs_has_job(context):
    jobs = context.chronos_client.list()
    job_names = [job['name'] for job in jobs]
    assert context.chronos_job_name in job_names
