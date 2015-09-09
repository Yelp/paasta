from behave import when, then

from paasta_tools import chronos_tools


# TODO this should be replaced by create_chronos_job_from_configs
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
    context.chronos_job_config = chronos_job_config
    context.chronos_job_name = chronos_job_config['name']


@when(u'we send the job to chronos')
def send_job_to_chronos(context):
    context.chronos_client.add(context.chronos_job_config)


@when(u'we wait for the chronos job to appear in the job list')
def chronos_job_is_ready(context):
    """Wait for a job with a matching job id to be ready. """
    chronos_tools.wait_for_job(context.chronos_client, context.chronos_job_name)


@then(u"we {should_or_not} be able to see it when we list jobs")
def list_chronos_jobs_has_job(context, should_or_not):
    jobs = context.chronos_client.list()
    job_names = [job['name'] for job in jobs]
    if should_or_not == "should not":
        assert context.chronos_job_name not in job_names
    else:
        assert context.chronos_job_name in job_names


# NOTE this is a placeholder until we are able to get per-job task information from Chronos
@then(u"the job {has_or_not} running tasks")
def chronos_check_running_tasks(context, has_or_not):
    # if has_or_not == "has no":
    #     assert job has no running tasks
    # else:  # has_or_not should be "has"
    #     assert job has running tasks
    assert True


@then(u"the job is {disabled} in chronos")
def chronos_check_job_state(context, disabled):
    desired_disabled = (disabled == 'disabled')
    jobs = chronos_tools.lookup_chronos_jobs(context.chronos_job_name, context.chronos_client, 1, desired_disabled)
    assert jobs != []
    for job in jobs:
        assert job['disabled'] == desired_disabled
