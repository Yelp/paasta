import copy
import logging
import re

import isodate

import service_configuration_lib


DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger('__main__')


class InvalidChronosConfigError(Exception):
    pass


# TODO support multiple jobs in one config file? (like service instances for Marathon)
def load_chronos_job_config(service_name, cluster, soa_dir=DEFAULT_SOA_DIR):
    chronos_conf_file = 'chronos-%s' % cluster

    log.info("Reading Chronos configuration file: %s.yaml", chronos_conf_file)

    # TODO we need to decide what info from the service configs is relevant to chronos,
    # and move the related functionality out of marathon_tools into utils or something similar
    branch_dict = {  # FIXME this is just a placeholder until we define what we need outside of the job config yaml
        'full_branch': 'paasta-%s-%s' % (service_name, cluster),
    }

    service_chronos_config = service_configuration_lib.read_extra_service_information(
        service_name,
        chronos_conf_file,
        soa_dir=soa_dir
    )

    return ChronosJobConfig(service_name, service_chronos_config, branch_dict)


class ChronosJobConfig(dict):

    def __init__(self, service_name, config_dict, branch_dict):
        self.service_name = service_name
        self.config_dict = config_dict
        self.branch_dict = branch_dict

    def get(self, param):
        config_dict_params = ['name', 'description', 'command', 'args', 'shell', 'epsilon', 'executor',
                              'executor_flags', 'retries', 'owner', 'owner_name', 'async', 'cpus', 'mem',
                              'disk', 'disabled', 'uris', 'schedule', 'schedule_time_zone', 'parents',
                              'user_to_run_as', 'container', 'data_job', 'environment_variables', 'constraints']
        branch_dict_params = ['full_branch']  # TODO fill in with the actual params in branch_dict

        if param in config_dict_params:
            return self.config_dict.get(param)
        elif param in branch_dict_params:
            return self.branch_dict.get(param)
        elif param == 'service_name':
            return self.service_name
        else:
            return None

    # TODO put these docstrings somewhere useful, like a spec for the yaml
        """If Chronos misses the scheduled run time for any reason,
        it will still run the job if the time is within this interval.
        Epsilon must be formatted like an ISO 8601 Duration.
        See https://en.wikipedia.org/wiki/ISO_8601#Durations
        """
        """Python likes to output floats with as much precision as possible.
        The chronos API seems to round, so be aware that some difference may
        occur"""
        """Must be specified in the cryptic ISO 8601 format: https://en.wikipedia.org/wiki/ISO_8601"""

    # TODO maybe these should be private (e.g. _check_mem) since only check_param should call them?
    def check_epsilon(self):
        try:
            isodate.parse_duration(self.get('epsilon'))
        except isodate.ISO8601Error:
            return False, ('The specified epsilon value \'%s\' does not conform to the ISO8601 format.'
                           % self.get('epsilon'))
        return True, ''

    def check_retries(self):
        if self.get('retries') is not None:
            # TODO is this Pythonic? is there a better way to check if int?
            if not isinstance(self.get('retries'), int):
                return False, 'The specified retries value \'%s\' is not a valid int.' % self.get('retries')
        return True, ''

    def check_async(self):
        if self.get('async') is not None:
            if self.get('async') is True:
                return False, 'The config specifies that the job is async, which we don\'t support.'
        return True, ''

    def check_cpus(self):
        if self.get('cpus') is not None:
            # TODO is this Pythonic? is there a better way to check if float?
            if (not isinstance(self.get('cpus'), float)
                    and not isinstance(self.get('cpus'), int)):
                return False, 'The specified cpus value \'%s\' is not a valid float.' % self.get('cpus')
        return True, ''

    def check_mem(self):
        if self.get('mem') is not None:
            # TODO is this Pythonic? is there a better way to check if float?
            if (not isinstance(self.get('mem'), float)
                    and not isinstance(self.get('mem'), int)):
                return False, 'The specified mem value \'%s\' is not a valid float.' % self.get('mem')
        return True, ''

    def check_disk(self):
        if self.get('disk') is not None:
            # TODO is this Pythonic? is there a better way to check if float?
            if (not isinstance(self.get('disk'), float)
                    and not isinstance(self.get('disk'), int)):
                return False, 'The specified disk value \'%s\' is not a valid float.' % self.get('disk')
        return True, ''

    # a valid 'repeat_string' is 'R' or 'Rn', where n is a positive integer representing the number of times to repeat
    # more info: https://en.wikipedia.org/wiki/ISO_8601#Repeating_intervals
    def _check_schedule_repeat_helper(self, repeat_string):
        pattern = re.compile('^R\d*$')
        return pattern.match(repeat_string) is not None

    def check_schedule(self):
        msgs = []
        schedule = self.get('schedule')

        if schedule is not None:
            repeat, start_time, interval = str.split(schedule, '/')  # the parts have separate validators
            if start_time != '':  # an empty start time is not valid ISO8601 but Chronos accepts it: '' == current time
                # TODO isodate accepts a time without time zone given (like 19:20:30 vs. 19:20:30-0800) as local time
                # do we want a time zone to be explicitly specified or is this default okay?
                try:
                    isodate.parse_datetime(start_time)
                except isodate.ISO8601Error as exc:
                    msgs.append('The specified start time \'%s\' in schedule \'%s\' '
                                'does not conform to the ISO 8601 format:\n%s' % (start_time, schedule, str(exc)))

            try:
                isodate.parse_duration(interval)  # 'interval' and 'duration' are interchangeable terms
            except isodate.ISO8601Error:
                msgs.append('The specified interval \'%s\' in schedule \'%s\' '
                            'does not conform to the ISO 8601 format.' % (interval, schedule))

            if not self._check_schedule_repeat_helper(repeat):
                msgs.append('The specified repeat \'%s\' in schedule \'%s\' '
                            'does not conform to the ISO 8601 format.' % (repeat, schedule))

        return len(msgs) == 0, '\n'.join(msgs)

    # TODO we should use pytz for best possible tz info and validation
    # TODO if tz specified in start_time, compare to the schedule_time_zone and warn if they differ
    # TODO if tz not specified in start_time, set it to time_zone
    # NOTE confusingly, the accepted time zone format for 'schedule_time_zone' is different than in 'schedule'!
    # 'schedule_time_zone': tz database format (https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
    # 'schedule': ISO 8601 format (https://en.wikipedia.org/wiki/ISO_8601#Time_zone_designators)
    # TODO maybe we don't even want to support this a a separate parameter? instead require it to be specified
    # as a component of the 'schedule' parameter?
    def check_schedule_time_zone(self):
        time_zone = self.get('schedule_time_zone')
        if time_zone is not None:
            return True, ''
            # try:
            #     # TODO validate tz format
            # except isodate.ISO8601Error as exc:
            #     return False, ('The specified time zone \'%s\' does not conform to the tz database format:\n%s'
            #                    % (time_zone, str(exc)))
        return True, ''

    def check(self, param):
        check_methods = {
            'epsilon': self.check_epsilon,
            'retries': self.check_retries,
            'async': self.check_async,
            'cpus': self.check_cpus,
            'mem': self.check_mem,
            'disk': self.check_disk,
            'schedule': self.check_schedule,
            'schedule_time_zone': self.check_schedule_time_zone,
        }
        supported_params_without_checks = ['name', 'description', 'command', 'owner', 'disabled']
        if param in check_methods:
            return check_methods[param]()
        elif param in supported_params_without_checks:
            return True, ''
        else:
            return False, 'Your Chronos config specifies \'%s\', an unsupported parameter.' % param


# defaults taken from the Chronos API docs https://mesos.github.io/chronos/docs/api.html#job-configuration
def set_missing_params_to_defaults(chronos_job_config):
    new_chronos_job_config = copy.deepcopy(chronos_job_config)
    chronos_config_defaults = {
        # 'shell': 'true',  # we don't support this param, but it does have a default specified by the Chronos docs
        'epsilon': 'PT60S',
        'retries': 2,
        'async': False,  # we don't support this param, but it does have a default specified by the Chronos docs
        'cpus': 0.1,
        'mem': 128,
        'disk': 256,
        'disabled': False,
        # 'data_job': False,  # we don't support this param, but it does have a default specified by the Chronos docs
    }

    for param in chronos_config_defaults.keys():
        if new_chronos_job_config.get(param) is None:
            new_chronos_job_config.config_dict[param] = chronos_config_defaults[param]
            # TODO if we want defaults for values outside of config_dict, we need add'l logic to handle them
    return new_chronos_job_config


def _check_scheduled_job_reqs_helper(chronos_job_config, job_type):
    missing_param_msg = 'Your Chronos config is missing \'%s\', a required parameter for a \'%s job\'.'
    msgs = []

    if chronos_job_config.get('schedule') is None:
        msgs.append(missing_param_msg % ('schedule', job_type))
    if chronos_job_config.get('parents') is not None:
        msgs.append('Your Chronos config specifies \'parents\', an invalid parameter for a \'scheduled job\'.')

    return msgs


def _check_dependent_job_reqs_helper(chronos_job_config, job_type):
    missing_param_msg = 'Your Chronos config is missing \'%s\', a required parameter for a \'%s job\'.'
    msgs = []

    if chronos_job_config.get('parents') is None:
        msgs.append(missing_param_msg % ('parents', job_type))
    if chronos_job_config.get('schedule') is not None:
        msgs.append('Your Chronos config specifies \'schedule\', an invalid parameter for a \'dependent job\'.')

    return msgs


def _check_docker_job_reqs_helper(chronos_job_config, job_type):
    missing_param_msg = 'Your Chronos config is missing \'%s\', a required parameter for a \'%s job\'.'
    msgs = []

    if chronos_job_config.get('container') is None:
        msgs.append(missing_param_msg % ('container', job_type))
    if chronos_job_config.get('schedule') is None and chronos_job_config.get('parents') is None:
        msgs.append('Your Chronos config contains neither \'schedule\' nor \'parents\'. '
                    'One is required for a \'docker job\'.')
    elif chronos_job_config.get('schedule') is not None and chronos_job_config.get('parents') is not None:
        msgs.append('Your Chronos config contains both \'schedule\' and \'parents\'. '
                    'Only one may be specified for a \'docker job\'.')

    return msgs


# 'scheduled job' requirements: https://mesos.github.io/chronos/docs/api.html#adding-a-scheduled-job
# 'dependent job' requirements: https://mesos.github.io/chronos/docs/api.html#adding-a-dependent-job
# 'docker job' requirements: https://mesos.github.io/chronos/docs/api.html#adding-a-docker-job
def check_job_reqs(chronos_job_config, job_type):
    missing_param_msg = 'Your Chronos config is missing \'%s\', a required parameter for a \'%s job\'.'
    msgs = []

    if job_type == 'scheduled':
        msgs += _check_scheduled_job_reqs_helper(chronos_job_config, job_type)
    elif job_type == 'dependent':
        msgs += _check_dependent_job_reqs_helper(chronos_job_config, job_type)
    elif job_type == 'docker':
        msgs += _check_docker_job_reqs_helper(chronos_job_config, job_type)
    else:
        return False, '\'%s\' is not a supported job type. Aborting job requirements check.' % job_type

    # TODO add schedule_time_zone
    for param in ['name', 'command', 'epsilon', 'owner', 'async']:
        if chronos_job_config.get(param) is None:
            msgs.append(missing_param_msg % (param, job_type))

    return len(msgs) == 0, msgs


def format_chronos_job_dict(chronos_job_config, job_type):
    complete_config_dict = dict()
    complete_chronos_job_config = set_missing_params_to_defaults(chronos_job_config)
    error_msgs = []

    # TODO once we use multiple config files, this needs to accomodate that
    for param in complete_chronos_job_config.config_dict.keys():
        check_passed, check_msg = complete_chronos_job_config.check(param)
        if check_passed:
            complete_config_dict[param] = complete_chronos_job_config.get(param)
        else:
            error_msgs.append(check_msg)

    reqs_passed, reqs_msgs = check_job_reqs(complete_chronos_job_config, job_type)
    if not reqs_passed:
        error_msgs += reqs_msgs

    if len(error_msgs) > 0:
        raise InvalidChronosConfigError('\n'.join(error_msgs))

    return complete_config_dict
