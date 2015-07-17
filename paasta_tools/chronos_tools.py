import isodate
import service_configuration_lib

SCHEDULED_JOB_REQS = ['name', 'command', 'schedule', 'epsilon', 'owner', 'async']
DEPENDENT_JOB_REQS = ['name', 'command', 'parents', 'epsilon', 'owner', 'async']
DOCKER_JOB_REQS = ['name', 'command', 'container', 'epsilon', 'owner', 'async']

# Default values for options if they aren't specified in the config, per the Chronos API docs:
# https://github.com/mesos/chronos/blob/master/docs/docs/api.md
DEFAULT_SHELL = True
DEFAULT_EPSILON = 'PT60S'  # if the job can't be run at its scheduled time, retry within 60 seconds
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


def load_chronos_job_config(service_name, cluster, soa_dir):
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


class ChronosJobConfig(dict):

    def get_name(self):
        return self.get('name', None)

    def get_description(self):
        return self.get('description', None)

    def get_command(self):
        return self.get('command', None)

    def get_shell(self):
        return self.get('shell', DEFAULT_SHELL)

    def get_epsilon(self):
        epsilon = self.get('epsilon', DEFAULT_EPSILON)
        isodate.parse_duration(epsilon)  # throws isodate.ISO8601Error << TODO then we should deal with it
        return epsilon

    def get_executor(self):
        return self.get('executor', DEFAULT_EXECUTOR)

    # TODO check that this format is correct, leave out for now since it's not necessary
    # def get_executor_flags(self):
    #     flags = {
    #         'container': {
    #             'image': get_docker_url_for_image(self['docker_image']),
    #             'options': self.get('docker_volumes', [])
    #         }
    #     }
    #     return json.dumps(flags)

    def get_retries(self):
        return int(self.get('retries', DEFAULT_RETRIES))

    def get_owner(self):
        return self.get('failure_contact_email', None)

    def get_owner_name(self):  # TODO is this necessary and what is it named in the REST API?
        return self.get('owner', None)

    def get_async(self):
        """Async Chronos jobs seem like the sort of thing that we should
        explore and have a framework for before we allow them"""
        # return self.get('async', DEFAULT_ASYNC)
        return False  # TODO do we want to support async Chronos jobs?

    def get_cpus(self):
        """Python likes to output floats with as much precision as possible.
        The chronos API seems to round, so be aware that some difference may
        occur"""
        return float(self.get('cpus', DEFAULT_CPUS))

    def get_mem(self):
        return int(self.get('mem', DEFAULT_MEM))

    def get_disk(self):
        return int(self.get('disk', DEFAULT_DISK))

    def get_disabled(self):
        return self.get('disabled', DEFAULT_DISABLED)

    # TODO why is this looking at a Marathon config and running setup_marathon_job?
    # def get_docker_url_for_image(docker_image):
    #     marathon_config = setup_marathon_job.get_main_marathon_config()
    #     return marathon_tools.get_docker_url(marathon_config['docker_registry'], docker_image)

    # TODO implement these later, just support basic scheduled task configs for now
    # def get_uris(self):
    #     return [get_docker_url_for_image(self['docker_image'])]

    # def validate_repeat(repeat_string):
    # TODO check that it conforms to the Chronos docs
    # by compiling regex like isodate does and match to regex
    #     return True

    # TODO need to check if parents is specifed and if so, ensure schedule is not specified (like args/cmd in marathon)
    def get_schedule(self):
        schedule = self.get('schedule', None)
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

    def get_schedule_time_zone(self):
        time_zone = self.get('schedule_time_zone', None)
        if time_zone is not None:
            time_zone = isodate.parse_tzinfo(time_zone)
            # TODO we should use pytz for best possible tz info and validation
            # TODO if tz specified in start_time, compare to the schedule_time_zone and warn if they differ
            # TODO if tz not specified in start_time, set it to time_zone
        else:
            return None

    # TODO implement these later, just support basic scheduled task configs for now
    # def get_args(self):
    # def get_parents(self):
    # def get_user_to_run_as(self):
    # def get_container(self):
    # def get_data_job(self):
    # def get_env(self):
    # def get_constraints(self):

    def check_scheduled_job_reqs(self):
        # TODO add scheduleTimeZone
        for param in SCHEDULED_JOB_REQS:
            if self[param] is None:
                raise InvalidChronosConfig('Your Chronos config is missing \'%s\', \
                                           a required parameter for a scheduled job.' % param)

    def check_dependent_job_reqs(self):
        # TODO add scheduleTimeZone
        for param in DEPENDENT_JOB_REQS:
            if self[param] is None:
                raise InvalidChronosConfig('Your Chronos config is missing \'%s\', \
                                           a required parameter for a dependent job.' % param)

    def check_docker_job_reqs(self):
        # TODO add scheduleTimeZone
        for param in DOCKER_JOB_REQS:
            if self[param] is None:
                raise InvalidChronosConfig('Your Chronos config is missing \'%s\', \
                                           a required parameter for a Docker job.' % param)
        if self['schedule'] is None and self['parents'] is None:
            raise InvalidChronosConfig('Your Chronos config contains neither a schedule nor parents. One is required.')
        elif self['schedule'] is not None and self['parents'] is not None:
            raise InvalidChronosConfig('Your Chronos config contains both schedule and parents. Only one is allowed.')

    # scheduled job required params: name, command, schedule, scheduleTimeZone (if not in schedule), epsilon, owner, async
    # dependent job required params: same as 'scheduled job' except req dependencies, NO SCHEDULE
    # Docker job    required params: same as 'scheduled job' except req either dependencies OR schedule
    def format_chronos_job_dict(self):
        config = {
            'name': get_name(self),
            'command': get_command(self),
            'epsilon': get_epsilon(self),
            # 'executor': get_executor(self),
            # 'executorFlags': get_executor_flags(self),
            'retries': get_retries(self),
            'owner': get_owner(self),
            'async': get_async(self),
            'cpus': get_cpus(self),
            'mem': get_mem(self),
            'disk': get_disk(self),
            'disabled': get_disabled(self),
            # 'uris': get_uris(self),
            'schedule': get_schedule(self),
        }

        check_scheduled_job_reqs(config)  # TODO better to have this here or in the constructor?
        return ChronosJobConfig(config)
