import copy
import json
import logging
import os
import re
import urlparse

import chronos
import isodate

import service_configuration_lib
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR

from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_default_branch
from paasta_tools.utils import get_docker_url

# In Marathon spaces are not allowed, in Chronos periods are not allowed.
# In the Chronos docs a space is suggested as the natural separator
SPACER = " "
PATH_TO_CHRONOS_CONFIG = os.path.join(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, 'chronos.json')
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger('__main__')


class ChronosNotConfigured(Exception):
    pass


class ChronosConfig(dict):

    def __init__(self, config, path):
        self.path = path
        super(ChronosConfig, self).__init__(config)

    def get_url(self):
        """:returns: The Chronos API endpoint"""
        try:
            return self['url']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos url in system chronos config: %s' % self.path)

    def get_username(self):
        """:returns: The Chronos API username"""
        try:
            return self['user']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos user in system chronos config: %s' % self.path)

    def get_password(self):
        """:returns: The Chronos API password"""
        try:
            return self['password']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos password in system chronos config: %s' % self.path)


def load_chronos_config(path=PATH_TO_CHRONOS_CONFIG):
    try:
        with open(path) as f:
            return ChronosConfig(json.load(f), path)
    except IOError as e:
        raise ChronosNotConfigured("Could not load chronos config file %s: %s" % (e.filename, e.strerror))


def get_chronos_client(config):
    """Returns a chronos client object for interacting with the API"""
    chronos_url = config.get_url()[0]
    chronos_hostname = urlparse.urlsplit(chronos_url).netloc
    log.info("Connecting to Chronos server at: %s", chronos_url)
    return chronos.connect(hostname=chronos_hostname,
                           username=config.get_username(),
                           password=config.get_password())


def get_job_id(service, instance, tag=None):
    output = "%s%s%s" % (service, SPACER, instance)
    if tag:
        output = "%s%s%s%s%s" % (service, SPACER, instance, SPACER, tag)
    return output


class InvalidChronosConfigError(Exception):
    pass


def load_chronos_job_config(service_name, job_name, cluster, soa_dir=DEFAULT_SOA_DIR):
    chronos_conf_file = 'chronos-%s' % cluster

    log.info("Reading Chronos configuration file: %s.yaml", chronos_conf_file)
    service_chronos_jobs = service_configuration_lib.read_extra_service_information(
        service_name,
        chronos_conf_file,
        soa_dir=soa_dir
    )

    if job_name not in service_chronos_jobs:
        raise InvalidChronosConfigError('No job named "%s" in config file %s.yaml' % (job_name, chronos_conf_file))

    deployments_json = load_deployments_json(service_name, soa_dir=soa_dir)
    branch = get_default_branch(cluster, job_name)
    branch_dict = deployments_json.get_branch_dict(service_name, branch)

    return ChronosJobConfig(service_name, job_name, service_chronos_jobs[job_name], branch_dict)


class ChronosJobConfig(dict):

    def __init__(self, service_name, job_name, config_dict, branch_dict):
        self.service_name = service_name
        self.job_name = job_name
        self.config_dict = config_dict
        self.branch_dict = branch_dict

    def __eq__(self, other):
        return ((self.service_name == other.service_name)
                and (self.job_name == other.job_name)
                and (self.config_dict == other.config_dict)
                and (self.branch_dict == other.branch_dict))

    def get(self, param):
        config_dict_params = ['description', 'command', 'args', 'shell', 'epsilon', 'executor',
                              'executor_flags', 'retries', 'owner', 'owner_name', 'async', 'cpus', 'mem',
                              'disk', 'disabled', 'uris', 'schedule', 'schedule_time_zone', 'parents',
                              'user_to_run_as', 'container', 'data_job', 'environment_variables', 'constraints']
        branch_dict_params = ['docker_image', 'desired_state', 'force_bounce']

        if param in config_dict_params:
            return self.config_dict.get(param)
        elif param in branch_dict_params:
            return self.branch_dict.get(param)
        elif param == 'service_name':
            return self.service_name
        elif param == 'job_name':
            return self.job_name
        else:
            return None

    # TODO maybe these should be private (e.g. _check_mem) since only check_param should call them?
    def check_epsilon(self):
        epsilon = self.get('epsilon')
        try:
            isodate.parse_duration(epsilon)
        except isodate.ISO8601Error:
            return False, 'The specified epsilon value "%s" does not conform to the ISO8601 format.' % epsilon
        return True, ''

    def check_retries(self):
        retries = self.get('retries')
        if retries is not None:
            if not isinstance(self.get('retries'), int):
                return False, 'The specified retries value "%s" is not a valid int.' % retries
        return True, ''

    def check_async(self):
        async = self.get('async')
        if async is not None:
            if async is True:
                return False, 'The config specifies that the job is async, which we don\'t support.'
        return True, ''

    def check_cpus(self):
        cpus = self.get('cpus')
        if cpus is not None:
            if not isinstance(cpus, float) and not isinstance(cpus, int):
                return False, 'The specified cpus value "%s" is not a valid float.' % cpus
        return True, ''

    def check_mem(self):
        mem = self.get('mem')
        if mem is not None:
            if not isinstance(mem, float) and not isinstance(mem, int):
                return False, 'The specified mem value "%s" is not a valid float.' % mem
        return True, ''

    def check_disk(self):
        disk = self.get('disk')
        if disk is not None:
            if not isinstance(disk, float) and not isinstance(disk, int):
                return False, 'The specified disk value "%s" is not a valid float.' % disk
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
                # NOTE isodate accepts a time without time zone given (like 19:20:30 vs. 19:20:30-0800) as local time
                # do we want a time zone to be explicitly specified or is this default okay?
                try:
                    isodate.parse_datetime(start_time)
                except isodate.ISO8601Error as exc:
                    msgs.append('The specified start time "%s" in schedule "%s" '
                                'does not conform to the ISO 8601 format:\n%s' % (start_time, schedule, str(exc)))

            try:
                isodate.parse_duration(interval)  # 'interval' and 'duration' are interchangeable terms
            except isodate.ISO8601Error:
                msgs.append('The specified interval "%s" in schedule "%s" '
                            'does not conform to the ISO 8601 format.' % (interval, schedule))

            if not self._check_schedule_repeat_helper(repeat):
                msgs.append('The specified repeat "%s" in schedule "%s" '
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
            # TODO validate tz format
            # except isodate.ISO8601Error as exc:
            #     return False, ('The specified time zone "%s" does not conform to the tz database format:\n%s'
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
        supported_params_without_checks = ['description', 'command', 'owner', 'disabled']
        if param in check_methods:
            return check_methods[param]()
        elif param in supported_params_without_checks:
            return True, ''
        else:
            return False, 'Your Chronos config specifies "%s", an unsupported parameter.' % param


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


# 'docker job' requirements: https://mesos.github.io/chronos/docs/api.html#adding-a-docker-job
def check_job_reqs(chronos_job_config):
    missing_param_msg = 'Your Chronos config is missing "%s", which is a required parameter.'
    msgs = []

    if chronos_job_config.get('schedule') is None and chronos_job_config.get('parents') is None:
        msgs.append('Your Chronos config contains neither "schedule" nor "parents".')
    elif chronos_job_config.get('schedule') is not None and chronos_job_config.get('parents') is not None:
        msgs.append('Your Chronos config contains both "schedule" and "parents".')

    # TODO add schedule_time_zone
    for param in ['command', 'epsilon', 'owner', 'async']:
        if chronos_job_config.get(param) is None:
            msgs.append(missing_param_msg % param)

    return len(msgs) == 0, msgs


def format_chronos_job_dict(chronos_job_config, docker_url, docker_volumes):
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

    # 'name' is the term Chronos uses, we store that value as 'job_name' to differentiate from 'service_name'
    complete_config_dict['name'] = complete_chronos_job_config.get('job_name')
    complete_config_dict['container'] = {
        'image': docker_url,
        'network': 'BRIDGE',
        'type': 'DOCKER',
        'volumes': docker_volumes
    }
    reqs_passed, reqs_msgs = check_job_reqs(complete_chronos_job_config)
    if not reqs_passed:
        error_msgs += reqs_msgs

    if len(error_msgs) > 0:
        raise InvalidChronosConfigError('\n'.join(error_msgs))

    return complete_config_dict


def get_service_job_list(service_name, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Enumerate the Chronos jobs defined for a service as a list of tuples.

    :param name: The service name
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, job) for each job defined for the service name"""
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    chronos_conf_file = "chronos-%s" % cluster
    log.info("Enumerating all jobs from config file: %s/*/%s.yaml", soa_dir, chronos_conf_file)
    jobs = service_configuration_lib.read_extra_service_information(
        service_name,
        chronos_conf_file,
        soa_dir=soa_dir
    )
    job_list = []
    for job in jobs:
        job_list.append((service_name, job))
    log.debug("Enumerated the following jobs: %s", job_list)
    return job_list


def get_chronos_jobs_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retrieve all Chronos jobs defined to run on a cluster.

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service_name, job_name)"""
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    rootdir = os.path.abspath(soa_dir)
    log.info("Retrieving all Chronos job names from %s for cluster %s", rootdir, cluster)
    job_list = []
    for srv_dir in os.listdir(rootdir):
        job_list += get_service_job_list(srv_dir, cluster, soa_dir)
    return job_list


def create_complete_config(service, job_name, soa_dir=DEFAULT_SOA_DIR):
    """Generates a complete dictionary to be POST'ed to create a job on Chronos"""
    system_paasta_config = load_system_paasta_config()
    chronos_job_config = load_chronos_job_config(
        service, job_name, system_paasta_config.get_cluster(), soa_dir=soa_dir)
    docker_url = get_docker_url(
        system_paasta_config.get_docker_registry(), chronos_job_config.get('docker_image'))

    complete_config = format_chronos_job_dict(
        chronos_job_config,
        docker_url,
        system_paasta_config.get_volumes(),
    )
    code_sha = get_code_sha_from_dockerurl(docker_url)
    config_hash = get_config_hash(complete_config)
    tag = "%s%s%s" % (code_sha, SPACER, config_hash)
    full_id = get_job_id(service, job_name, tag)
    complete_config['name'] = full_id
    return complete_config
