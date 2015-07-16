import isodate
import service_configuration_lib

DEFAULT_SHELL = True
DEFAULT_EPSILON = 'PT60S'
DEFAULT_EXECUTOR = ''
DEFAULT_RETRIES = 2
DEFAULT_ASYNC = False
DEFAULT_CPUS = 0.1
DEFAULT_MEM = 128
DEFAULT_DISK = 256
DEFAULT_DISABLED = False
DEFAULT_DATAJOB = False


class InvalidChronosConfig(Exception):
    pass


# TODO Chronos config should probably be a class like MarathonServiceConfig
def create_chronos_config(service_name, cluster, soa_dir):
    config = read_chronos_soa_config(service_name, cluster, soa_dir)
    parsed_config = parse_chronos_config(config)
    return parsed_config


def read_chronos_soa_config(service_name, cluster, soa_dir):
    chronos_conf_file = 'chronos-%s' % cluster

    # TODO log.info("Reading Chronos configuration file: %s.yaml", chronos_conf_file)
    service_chronos_config = service_configuration_lib.read_extra_service_information(
        service_name,
        chronos_conf_file,
        soa_dir=soa_dir
    )

    # TODO why is this here? an empty config is not acceptable
    # keeps the function returning lists when the yaml file is empty
    # if service_chronos_config == {}:
    #     service_chronos_config = []

    return service_chronos_config


# TODO break this into separate methods for scheduled jobs, dependent jobs, and Docker jobs
# scheduled job required params: name, command, schedule, scheduleTimeZone (if not in schedule), epsilon, owner, async
# dependent job required params: same as 'scheduled job' except req dependencies, NO SCHEDULE
# Docker job    required params: same as 'scheduled job' except req either dependencies OR schedule
def parse_chronos_config(job_config):
    config = {
        'name': get_name(job_config),
        'command': get_command(job_config),
        'epsilon': get_epsilon(job_config),
        # 'executor': get_executor(job_config),
        # 'executorFlags': get_executor_flags(job_config),
        'retries': get_retries(job_config),
        'owner': get_owner(job_config),
        'async': get_async(job_config),
        'cpus': get_cpus(job_config),
        'mem': get_mem(job_config),
        'disk': get_disk(job_config),
        'disabled': get_disabled(job_config),
        # 'uris': get_uris(job_config),
        'schedule': get_schedule(job_config),
    }

    check_scheduled_job_reqs(config)  # TODO once this method is broken up this should only be in scheduled jobs parser

    return config


def check_scheduled_job_reqs(config):
    # TODO there's probably a much more elegant way of doing this
    if (
            config['name'] is None
            or config['command'] is None
            or config['schedule'] is None
            # or config['scheduleTimeZone'] is None  # TODO time zone can alternatively be specified in schedule
            or config['epsilon'] is None
            or config['owner'] is None
            or config['async'] is None
    ):
        raise InvalidChronosConfig('Your Chronos config does not contain all the necessary params for a scheduled job.')
        # TODO print which params are missing


def check_dependent_job_reqs(config):
    # TODO there's probably a much more elegant way of doing this
    if (
            config['name'] is None
            or config['command'] is None
            or config['parents'] is None
            # or config['scheduleTimeZone'] is None  # TODO time zone can alternatively be specified in schedule
            or config['epsilon'] is None
            or config['owner'] is None
            or config['async'] is None
    ):
        raise InvalidChronosConfig('Your Chronos config does not contain all the necessary params for a dependent job.')
        # TODO print which params are missing
    elif config['schedule'] is not None and config['dependents'] is not None:
        raise InvalidChronosConfig('Your Chronos config contains both a schedule and dependents. Only one is allowed.')
    elif config['schedule'] is None and config['dependents'] is None:
        raise InvalidChronosConfig('Your Chronos config contains neither a schedule nor dependents. One is required.')


def check_docker_job_reqs(config):
    # TODO there's probably a much more elegant way of doing this
    if (
            config['name'] is None
            or config['command'] is None
            or config['schedule'] is None
            # or config['scheduleTimeZone'] is None  # TODO time zone can alternatively be specified in schedule
            or config['epsilon'] is None
            or config['owner'] is None
            or config['async'] is None
            or config['container'] is None  # TODO validate contents of container dict
    ):
        raise InvalidChronosConfig('Your Chronos config does not contain all the necessary params for a Docker job.')
        # TODO print which params are missing


def get_name(job_config):
    return job_config.get('name', None)


def get_description(job_config):
    return job_config.get('description', None)


def get_command(job_config):
    return job_config.get('command', None)


def get_shell(job_config):
    return job_config.get('shell', DEFAULT_SHELL)


def get_epsilon(job_config):
    epsilon = job_config.get('epsilon', DEFAULT_EPSILON)
    isodate.parse_duration(epsilon)  # throws isodate.ISO8601Error << TODO then we should deal with it
    return epsilon


def get_executor(job_config):
    return job_config.get('executor', DEFAULT_EXECUTOR)


# TODO check that this format is correct, leave out for now since it's not necessary
# def get_executor_flags(job_config):
#     flags = {
#         'container': {
#             'image': get_docker_url_for_image(job_config['docker_image']),
#             'options': job_config.get('docker_volumes', [])
#         }
#     }
#     return json.dumps(flags)


def get_retries(job_config):
    return int(job_config.get('retries', DEFAULT_RETRIES))  # TODO is the int conversion necessary?


def get_owner(job_config):
    return job_config.get('failure_contact_email', None)


def get_owner_name(job_config):  # TODO is this necessary and what is it named in the REST API?
    return job_config.get('owner', None)


def get_async(job_config):
    """Async Chronos jobs seem like the sort of thing that we should
    explore and have a framework for before we allow them"""
    # return job_config.get('async', DEFAULT_ASYNC)
    return False  # TODO do we want to support async Chronos jobs?


# TODO why are successCount, errorCount, lastSuccess, and lastError in job config in the docs?


def get_cpus(job_config):
    """Python likes to output floats with as much precision as possible.
    The chronos API seems to round, so be aware that some difference may
    occur"""
    return float(job_config.get('cpus', DEFAULT_CPUS))


def get_mem(job_config):
    return int(job_config.get('mem', DEFAULT_MEM))  # TODO is the int conversion necessary?


def get_disk(job_config):
    return int(job_config.get('disk', DEFAULT_DISK))  # TODO is the int conversion necessary?


def get_disabled(job_config):
    return job_config.get('disabled', DEFAULT_DISABLED)


# TODO why is this looking at a Marathon config and running setup_marathon_job?
# def get_docker_url_for_image(docker_image):
#     marathon_config = setup_marathon_job.get_main_marathon_config()
#     return marathon_tools.get_docker_url(marathon_config['docker_registry'], docker_image)


# TODO implement these later, just support basic scheduled task configs for now
# def get_uris(job_config):
#     return [get_docker_url_for_image(job_config['docker_image'])]


# def validate_repeat(repeat_string):
# TODO check that it conforms to the Chronos docs
# by compiling regex like isodate does and match to regex
#     return True


# TODO need to check if parents is specifed and if so, ensure schedule is not specified (like args/cmd in marathon)
def get_schedule(job_config):
    schedule = job_config.get('schedule', None)
    if schedule is not None:
        repeat, start_time, interval = str.split(schedule, '/')  # separate the parts isodate can and can't validate
        isodate.parse_datetime(start_time)  # we don't need to parse into a different format, this is just validation
        # TODO check that it won't be an issue that isodate can return either a datetime.timedelta or Duration object
        isodate.parse_duration(interval)  # 'interval' and 'duration' are interchangeable terms
        # TODO: should we catch ISO8601Error exceptions and print some additional debug info
        # or fallback to the built-in message?
        # TODO validate the 'repeat' portion of the schedule
        # validate_repeat(repeat)
        return schedule
    else:
        return None


def get_schedule_time_zone(job_config):
    time_zone = job_config.get('schedule_time_zone', None)
    if time_zone is not None:
        time_zone = isodate.parse_tzinfo(time_zone)
        # TODO we should use pytz for best possible tz info and validation
        # TODO if tz specified in start_time, compare to the schedule_time_zone and warn if they differ
        # TODO if tz not specified in start_time, set it to time_zone
    else:
        return None


# TODO implement these later, just support basic scheduled task configs for now
# def get_parents(job_config):
# def get_user_to_run_as(job_config):
# def get_container(job_config):
# def get_data_job(job_config):
# def get_env(job_config):
# def get_constraints(job_config):
