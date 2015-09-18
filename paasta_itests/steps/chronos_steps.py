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


@when(u'we load the configs for instance "{instance_name}" of service "{service_name}" into a ChronosJobConfig')
def create_chronos_job_config_object_from_configs(context, instance_name, service_name):
    context.chronos_job_config_obj = chronos_tools.load_chronos_job_config(
        service_name,
        instance_name,
        context.cluster,
        context.soa_dir
    )


@when(u'we create a chronos job dict from the configs for instance "{instance_name}" of service "{service_name}"')
def create_chronos_job_from_configs(context, instance_name, service_name):
    chronos_job_config = chronos_tools.create_complete_config(service_name, instance_name, context.soa_dir)
    context.chronos_job_config = chronos_job_config
    context.chronos_job_name = chronos_job_config['name']


@when(u'we set the bounce_method of the ChronosJobConfig to "{bounce_method}"')
def set_bounce_method_chronos_job_config(context, bounce_method):
    context.chronos_job_config_obj.config_dict['bounce_method'] = bounce_method


@when(u'we update the tag for the service "{service_name}" with {disabled} chronos instance "{instance_name}"')
def update_job_tag(context, service_name, disabled, instance_name):
    context.old_chronos_job_name = context.chronos_job_name
    context.tag_version = context.tag_version + 1
    context.execute_steps('Given I have a deployments.json for the service "%s" with %s chronos instance "%s"'
                          % (service_name, disabled, instance_name))


@when(u'we send the job to chronos')
def send_job_to_chronos(context):
    context.chronos_client.add(context.chronos_job_config)


@when(u'we wait for the chronos job to appear in the job list')
def chronos_job_is_ready(context):
    """Wait for a job with a matching job id to be ready."""
    chronos_tools.wait_for_job(context.chronos_client, context.chronos_job_name)


@when(u'we manually start the job')
def chronos_manually_run_job(context):
    context.chronos_client.run(context.chronos_job_name)


@then(u"we {should_or_not} be able to see it when we list jobs")
def list_chronos_jobs_has_job(context, should_or_not):
    jobs = context.chronos_client.list()
    job_names = [job['name'] for job in jobs]
    if should_or_not == "should not":
        assert context.chronos_job_name not in job_names
    else:
        assert context.chronos_job_name in job_names


# NOTE this is a placeholder until we are able to get per-job task information from Chronos
@then(u"the {old_or_new_job} {has_or_not} running tasks")
def chronos_check_running_tasks(context, old_or_new_job, has_or_not):
    # job_id = context.old_chronos_job_name if old_or_new_job == 'old job' else context.chronos_job_name
    # if has_or_not == "has no":
    #     assert job_id has no running tasks
    # else:  # has_or_not should be "has"
    #     assert job_id has running tasks
    assert True


@then(u"the {old_or_new_job} is {disabled} in chronos")
def chronos_check_job_state(context, old_or_new_job, disabled):
    desired_disabled = (disabled == 'disabled')
    job_id = context.old_chronos_job_name if old_or_new_job == 'old job' else context.chronos_job_name
    jobs = chronos_tools.lookup_chronos_jobs(
        job_id,
        context.chronos_client,
        max_expected=1,
        include_disabled=desired_disabled
    )
    assert jobs != []
    for job in jobs:
        assert job['disabled'] == desired_disabled
