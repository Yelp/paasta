import isodate
import logging

import service_configuration_lib

DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger('__main__')

SCHEDULED_JOB_REQS = ['name', 'command', 'schedule', 'epsilon', 'owner', 'async']
# DEPENDENT_JOB_REQS = ['name', 'command', 'parents', 'epsilon', 'owner', 'async']
# DOCKER_JOB_REQS = ['name', 'command', 'container', 'epsilon', 'owner', 'async']

# Default values for options if they aren't specified in the config, per the Chronos API docs:
# https://github.com/mesos/chronos/blob/master/docs/docs/api.md
DEFAULT_SHELL = 'true'  # this is a boolean value, not a shell name like /bin/true or /bin/sh
DEFAULT_EPSILON = 'PT60S'  # if the job can't be run at its scheduled time, retry within 60 seconds
DEFAULT_EXECUTOR = ''
DEFAULT_RETRIES = 2
DEFAULT_ASYNC = 'false'
DEFAULT_CPUS = 0.1
DEFAULT_MEM = 128
DEFAULT_DISK = 256
DEFAULT_DISABLED = 'false'
DEFAULT_DATAJOB = 'false'


class InvalidChronosConfig(Exception):
    pass


def load_chronos_job_config(service_name, cluster, soa_dir=DEFAULT_SOA_DIR):
    chronos_conf_file = 'chronos-%s' % cluster

    log.info("Reading Chronos configuration file: %s.yaml", chronos_conf_file)
    service_chronos_config = service_configuration_lib.read_extra_service_information(
        service_name,
        chronos_conf_file,
        soa_dir=soa_dir
    )

    return service_chronos_config


class ChronosJobConfig(dict):

    def get_name(self):
        return self.get('name', None)

    def get_description(self):
        return self.get('description', None)

    def get_command(self):
        return self.get('command', None)

    def get_epsilon(self):
        """If Chronos misses the scheduled run time for any reason,
        it will still run the job if the time is within this interval.
        Epsilon must be formatted like an ISO 8601 Duration.
        See https://en.wikipedia.org/wiki/ISO_8601#Durations
        """
        epsilon = self.get('epsilon', DEFAULT_EPSILON)
        isodate.parse_duration(epsilon)  # throws isodate.ISO8601Error
        # TODO might want to wrap the isodate call in try/except to provide more helpful info if format except thrown
        return epsilon

    def get_retries(self):
        return int(self.get('retries', DEFAULT_RETRIES))

    def get_owner(self):
        return self.get('owner', None)

    def get_async(self):
        """We don't support async jobs with Chronos, so this only ever returns the default 'false'.
        This method only exists because the Chronos API specifies async as a required option,
        so we need to include it as a parameter when talking to the Chronos API.
        """
        # return self.get('async', DEFAULT_ASYNC)  # if we ever want to support async, use this
        return DEFAULT_ASYNC

    def get_cpus(self):
        """Python likes to output floats with as much precision as possible.
        The chronos API seems to round, so be aware that some difference may
        occur"""
        return float(self.get('cpus', DEFAULT_CPUS))

    def get_mem(self):
        return float(self.get('mem', DEFAULT_MEM))

    def get_disk(self):
        return float(self.get('disk', DEFAULT_DISK))

    def get_disabled(self):
        return self.get('disabled', DEFAULT_DISABLED)

    # def validate_repeat(repeat_string):
    # TODO check that it conforms to the Chronos docs
    # by compiling regex like isodate does and match to regex
    #     return True

    def get_schedule(self):
        """Must be specified in the cryptic ISO 8601 format: https://en.wikipedia.org/wiki/ISO_8601"""
        schedule = self.get('schedule', None)
        if schedule is not None:
            repeat, start_time, interval = str.split(schedule, '/')  # separate the parts isodate can/can't validate
            isodate.parse_datetime(start_time)  # we don't need the time in a different format, this is just validation
            # TODO check that it won't be an issue that isodate can return either a datetime.timedelta or Duration
            isodate.parse_duration(interval)  # 'interval' and 'duration' are interchangeable terms
            # TODO: should we catch ISO8601Error exceptions and print some additional debug info
            # or fallback to the built-in message?
            # TODO validate the 'repeat' portion of the schedule
            # validate_repeat(repeat)
            return schedule
        else:
            return None

    # TODO support this later, for now just specify time zone in schedule
    # def get_schedule_time_zone(self):
    #     time_zone = self.get('schedule_time_zone', None)
    #     if time_zone is not None:
    #         time_zone = isodate.parse_tzinfo(time_zone)
    #         # TODO we should use pytz for best possible tz info and validation
    #         # TODO if tz specified in start_time, compare to the schedule_time_zone and warn if they differ
    #         # TODO if tz not specified in start_time, set it to time_zone
    #     else:
    #         return None

    def check_scheduled_job_reqs(self):
        # TODO add schedule_time_zone
        for param in SCHEDULED_JOB_REQS:
            if self[param] is None:
                raise InvalidChronosConfig('Your Chronos config is missing \'%s\', \
                                           a required parameter for a scheduled job.' % param)

    # TODO for now just support scheduled jobs
    # def check_dependent_job_reqs(self):
    #     # TODO add schedule_time_zone
    #     for param in DEPENDENT_JOB_REQS:
    #         if self[param] is None:
    #             raise InvalidChronosConfig('Your Chronos config is missing \'%s\', \
    #                                        a required parameter for a dependent job.' % param)

    # TODO for now just support scheduled jobs
    # def check_docker_job_reqs(self):
    #     # TODO add schedule_time_zone
    #     for param in DOCKER_JOB_REQS:
    #         if self[param] is None:
    #             raise InvalidChronosConfig('Your Chronos config is missing \'%s\', \
    #                                        a required parameter for a Docker job.' % param)
    #     if self['schedule'] is None and self['parents'] is None:
    #       raise InvalidChronosConfig('Your Chronos config contains neither a schedule nor parents. One is required.')
    #     elif self['schedule'] is not None and self['parents'] is not None:
    #       raise InvalidChronosConfig('Your Chronos config contains both schedule and parents. Only one is allowed.')

    # scheduled job req'd params: name, command, schedule, scheduleTimeZone (if not in schedule), epsilon, owner, async
    # dependent job req'd params: same as 'scheduled job' except it requires dependencies, NO SCHEDULE
    # Docker job    req'd params: same as 'scheduled job' except it requires either dependencies OR schedule
    def format_chronos_job_dict(self):
        self['name'] = self.get_name()
        self['description'] = self.get_description()
        self['command'] = self.get_command()
        self['epsilon'] = self.get_epsilon()
        self['retries'] = self.get_retries()
        self['owner'] = self.get_owner()
        self['async'] = self.get_async()
        self['cpus'] = self.get_cpus()
        self['mem'] = self.get_mem()
        self['disk'] = self.get_disk()
        self['disabled'] = self.get_disabled()
        self['schedule'] = self.get_schedule()

        self.check_scheduled_job_reqs()  # TODO better to have this here or in the constructor?
