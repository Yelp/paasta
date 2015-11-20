# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import logging
import os
import re
import urlparse
from time import sleep

import chronos
import dateutil
import isodate
from tron import command_context

import monitoring_tools
import service_configuration_lib
from paasta_tools.utils import compose_job_id as utils_compose_job_id
from paasta_tools.utils import decompose_job_id as utils_decompose_job_id
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import get_docker_url
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR
from paasta_tools.utils import timeout


# In Marathon spaces are not allowed, in Chronos periods are not allowed.
# In the Chronos docs a space is suggested as the natural separator
SPACER = " "
# Until Chronos supports dots in the job name, we use this separator internally
INTERNAL_SPACER = '.'
# Chronos creates Mesos tasks with an id composed of some arbitrary strings,
# the app's full name, a spacer, and a timestamp. This variable is that
# spacer. Note that we don't control this spacer, i.e. you can't change it
# here and expect the world to change with you. We need to know what it is so
# we can decompose Mesos task ids.
MESOS_TASK_SPACER = ':'

VALID_BOUNCE_METHODS = ['graceful']
PATH_TO_CHRONOS_CONFIG = os.path.join(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, 'chronos.json')
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger('__main__')


class LastRunState:
    """Cheap enum to represent the state of the last run"""
    Success, Fail, NotRun = range(0, 3)


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
    chronos_hosts = config.get_url()
    chronos_hostnames = [urlparse.urlsplit(hostname).netloc for hostname in chronos_hosts]
    log.info("Attempting to connect to Chronos servers: %s" % chronos_hosts)
    return chronos.connect(servers=chronos_hostnames,
                           username=config.get_username(),
                           password=config.get_password())


def compose_job_id(service, instance, git_hash=None, config_hash=None):
    """Thin wrapper around generic compose_job_id to use our local SPACER."""
    return utils_compose_job_id(service, instance, git_hash, config_hash, spacer=SPACER)


def decompose_job_id(job_id):
    """Thin wrapper around generic decompose_job_id to use our local SPACER."""
    return utils_decompose_job_id(job_id, spacer=SPACER)


class InvalidChronosConfigError(Exception):
    pass


def read_chronos_jobs_for_service(service, cluster, soa_dir=DEFAULT_SOA_DIR):
    chronos_conf_file = 'chronos-%s' % cluster
    log.info("Reading Chronos configuration file: %s/%s/chronos-%s.yaml" % (soa_dir, service, cluster))

    return service_configuration_lib.read_extra_service_information(
        service,
        chronos_conf_file,
        soa_dir=soa_dir
    )


def load_chronos_job_config(service, instance, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR):
    service_chronos_jobs = read_chronos_jobs_for_service(service, cluster, soa_dir=soa_dir)
    if instance not in service_chronos_jobs:
        raise InvalidChronosConfigError('No job named "%s" in config file chronos-%s.yaml' % (instance, cluster))
    branch_dict = {}
    if load_deployments:
        deployments_json = load_deployments_json(service, soa_dir=soa_dir)
        branch = get_paasta_branch(cluster=cluster, instance=instance)
        branch_dict = deployments_json.get_branch_dict(service, branch)
    return ChronosJobConfig(service, instance, service_chronos_jobs[instance], branch_dict)


class ChronosJobConfig(InstanceConfig):

    def __init__(self, service, job_name, config_dict, branch_dict):
        super(ChronosJobConfig, self).__init__(config_dict, branch_dict)
        self.service = service
        self.job_name = job_name
        self.config_dict = config_dict
        self.branch_dict = branch_dict

    def __eq__(self, other):
        return ((self.service == other.service)
                and (self.job_name == other.job_name)
                and (self.config_dict == other.config_dict)
                and (self.branch_dict == other.branch_dict))

    def get_service(self):
        return self.service

    def get_job_name(self):
        return self.job_name

    def get_cmd(self):
        original_cmd = super(ChronosJobConfig, self).get_cmd()
        if original_cmd:
            return parse_time_variables(original_cmd)
        else:
            return original_cmd

    def get_owner(self):
        overrides = self.get_monitoring()
        return monitoring_tools.get_team(overrides=overrides, service=self.get_service())

    def get_bounce_method(self):
        """Returns the bounce method specified for the Chronos job.

        Options are:
        * ``graceful``: disables the old version but allows it to finish its current run
        * ``brutal``: disables the old version and immediately kills any running tasks it has
        If unspecified, defaults to ``graceful``.
        """
        return self.config_dict.get('bounce_method', 'graceful')

    def get_env(self):
        """The expected input env for PaaSTA is a dictionary of key/value pairs
        Chronos requires an array of dictionaries in a very specific format:
        https://mesos.github.io/chronos/docs/api.html#sample-job"""
        original_env = super(ChronosJobConfig, self).get_env()
        return [{"name": key, "value": value} for key, value in original_env.iteritems()]

    def get_constraints(self):
        return self.config_dict.get('constraints')

    def check_bounce_method(self):
        bounce_method = self.get_bounce_method()
        if bounce_method not in VALID_BOUNCE_METHODS:
            return False, ('The specified bounce method "%s" is invalid. It must be one of (%s).'
                           % (bounce_method, ', '.join(VALID_BOUNCE_METHODS)))
        return True, ''

    def get_epsilon(self):
        return self.config_dict.get('epsilon', 'PT60S')

    def get_retries(self):
        return self.config_dict.get('retries', 2)

    def get_disabled(self):
        return self.config_dict.get('disabled', False)

    def get_schedule(self):
        return self.config_dict.get('schedule')

    def get_schedule_time_zone(self):
        return self.config_dict.get('schedule_time_zone')

    def get_shell(self):
        """Per https://mesos.github.io/chronos/docs/api.html, ``shell`` defaults
        to true, but if arguments are set, they will be ignored. If arguments are
        set in our config, then we need to set shell: False so that they will
        activate."""
        args = self.get_args()
        return args == [] or args is None

    def check_epsilon(self):
        epsilon = self.get_epsilon()
        try:
            isodate.parse_duration(epsilon)
        except isodate.ISO8601Error:
            return False, 'The specified epsilon value "%s" does not conform to the ISO8601 format.' % epsilon
        return True, ''

    def check_retries(self):
        retries = self.get_retries()
        if retries is not None:
            if not isinstance(retries, int):
                return False, 'The specified retries value "%s" is not a valid int.' % retries
        return True, ''

    # a valid 'repeat_string' is 'R' or 'Rn', where n is a positive integer representing the number of times to repeat
    # more info: https://en.wikipedia.org/wiki/ISO_8601#Repeating_intervals
    def _check_schedule_repeat_helper(self, repeat_string):
        pattern = re.compile('^R\d*$')
        return pattern.match(repeat_string) is not None

    def check_schedule(self):
        msgs = []
        schedule = self.get_schedule()

        if schedule is not None:
            try:
                repeat, start_time, interval = str.split(schedule, '/')  # the parts have separate validators
            except ValueError:
                return False, 'The specified schedule "%s" is invalid' % schedule

            # an empty start time is not valid ISO8601 but Chronos accepts it: '' == current time
            if start_time == '':
                msgs.append('The specified schedule "%s" does not contain a start time' % schedule)
            else:
                # Check if start time contains time zone information
                try:
                    dt = isodate.parse_datetime(start_time)
                    if not hasattr(dt, 'tzinfo'):
                        msgs.append('The specified start time "%s" must contain a time zone' % start_time)
                except isodate.ISO8601Error as exc:
                    msgs.append('The specified start time "%s" in schedule "%s" '
                                'does not conform to the ISO 8601 format:\n%s' % (start_time, schedule, str(exc)))

            parsed_interval = None
            try:
                # 'interval' and 'duration' are interchangeable terms
                parsed_interval = isodate.parse_duration(interval)
            except isodate.ISO8601Error:
                msgs.append('The specified interval "%s" in schedule "%s" '
                            'does not conform to the ISO 8601 format.' % (interval, schedule))

            # until we make this configurable, throw an
            # error if we have a schedule < 60 seconds (the default schedule_horizone for chronos)
            # https://github.com/mesos/chronos/issues/508
            if parsed_interval and parsed_interval < datetime.timedelta(seconds=60):
                msgs.append('Unsupported interval "%s": jobs must be run at an interval of > 60 seconds' % interval)

            if not self._check_schedule_repeat_helper(repeat):
                msgs.append('The specified repeat "%s" in schedule "%s" '
                            'does not conform to the ISO 8601 format.' % (repeat, schedule))
        else:
            msgs.append('You must specify a "schedule" in your configuration')

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
        time_zone = self.get_schedule_time_zone()
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
            'bounce_method': self.check_bounce_method,
            'epsilon': self.check_epsilon,
            'retries': self.check_retries,
            'cpus': self.check_cpus,
            'mem': self.check_mem,
            'schedule': self.check_schedule,
            'scheduleTimeZone': self.check_schedule_time_zone,
        }
        supported_params_without_checks = ['description', 'command', 'owner', 'disabled']
        if param in check_methods:
            return check_methods[param]()
        elif param in supported_params_without_checks:
            return True, ''
        else:
            return False, 'Your Chronos config specifies "%s", an unsupported parameter.' % param

    def format_chronos_job_dict(self, docker_url, docker_volumes):
        valid, error_msgs = self.validate()
        if not valid:
            raise InvalidChronosConfigError("\n".join(error_msgs))

        complete_config = {
            'name': self.get_job_name().encode('utf_8'),
            'container': {
                'image': docker_url,
                'network': 'BRIDGE',
                'type': 'DOCKER',
                'volumes': docker_volumes
            },
            'uris': ['file:///root/.dockercfg', ],
            'environmentVariables': self.get_env(),
            'mem': self.get_mem(),
            'cpus': self.get_cpus(),
            'constraints': self.get_constraints(),
            'command': self.get_cmd(),
            'arguments': self.get_args(),
            'epsilon': self.get_epsilon(),
            'retries': self.get_retries(),
            'async': False,  # we don't support async jobs
            'disabled': self.get_disabled(),
            'owner': self.get_owner(),
            'schedule': self.get_schedule(),
            'scheduleTimeZone': self.get_schedule_time_zone(),
            'shell': self.get_shell(),
        }
        return complete_config

    # 'docker job' requirements: https://mesos.github.io/chronos/docs/api.html#adding-a-docker-job
    def validate(self):
        error_msgs = []
        # Use InstanceConfig to validate shared config keys like cpus and mem
        error_msgs.extend(super(ChronosJobConfig, self).validate())

        for param in ['epsilon', 'retries', 'cpus', 'mem', 'schedule', 'scheduleTimeZone']:
            check_passed, check_msg = self.check(param)
            if not check_passed:
                error_msgs.append(check_msg)

        return len(error_msgs) == 0, error_msgs

    def get_healthcheck_mode(self, _):
        # Healthchecks are not supported yet in chronos
        return None


# TODO just use utils.get_service_instance_list(cluster, instance_type='chronos', soa_dir)
def list_job_names(service, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Enumerate the Chronos jobs defined for a service as a list of tuples.

    :param name: The service name
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, job) for each job defined for the service name"""
    job_list = []
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    chronos_conf_file = "chronos-%s" % cluster
    log.info("Enumerating all jobs from config file: %s/*/%s.yaml" % (soa_dir, chronos_conf_file))

    for job in read_chronos_jobs_for_service(service, cluster, soa_dir=soa_dir):
        job_list.append((service, job))
    log.debug("Enumerated the following jobs: %s" % job_list)
    return job_list


# TODO just use utils.get_services_for_cluster(cluster, instance_type='chronos', soa_dir)
def get_chronos_jobs_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retrieve all Chronos jobs defined to run on a cluster.

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, job_name)"""
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    rootdir = os.path.abspath(soa_dir)
    log.info("Retrieving all Chronos job names from %s for cluster %s" % (rootdir, cluster))
    job_list = []
    for service in os.listdir(rootdir):
        job_list.extend(list_job_names(service, cluster, soa_dir))
    return job_list


def create_complete_config(service, job_name, soa_dir=DEFAULT_SOA_DIR):
    """Generates a complete dictionary to be POST'ed to create a job on Chronos"""
    system_paasta_config = load_system_paasta_config()
    chronos_job_config = load_chronos_job_config(
        service, job_name, system_paasta_config.get_cluster(), soa_dir=soa_dir)
    docker_url = get_docker_url(
        system_paasta_config.get_docker_registry(), chronos_job_config.get_docker_image())
    docker_volumes = system_paasta_config.get_volumes() + chronos_job_config.get_extra_volumes()

    complete_config = chronos_job_config.format_chronos_job_dict(
        docker_url,
        docker_volumes,
    )
    code_sha = get_code_sha_from_dockerurl(docker_url)
    config_hash = get_config_hash(complete_config)

    # Chronos clears the history for a job whenever it is updated, so we use a new job name for each revision
    # so that we can keep history of old job revisions rather than just the latest version
    full_id = compose_job_id(service, job_name, code_sha, config_hash)
    complete_config['name'] = full_id
    desired_state = chronos_job_config.get_desired_state()

    # If the job was previously stopped, we should stop the new job as well
    # NOTE this clobbers the 'disabled' param specified in the config file!
    if desired_state == 'start':
        complete_config['disabled'] = False
    elif desired_state == 'stop':
        complete_config['disabled'] = True

    log.debug("Complete configuration for instance is: %s" % complete_config)
    return complete_config


def _safe_parse_datetime(dt):
    """
    Parse a datetime, swallowing exceptions.

    :param dt: A string containing a datetime
    :returns: A datetime.datetime object representing ``dt``. If a datetime
    string is unparseable, it is represented as a datetime.datetime set to the
    epoch in UTC (i.e. a value which will never be the most recent).
    """
    epoch = datetime.datetime(1970, 1, 1, tzinfo=dateutil.tz.tzutc())
    try:
        parsed_dt = isodate.parse_datetime(dt)
    # I tried to limit this to isodate.ISO8601Error but parse_datetime() can
    # also throw "AttributeError: 'NoneType' object has no attribute 'split'",
    # and presumably other exceptions.
    except Exception as exc:
        log.debug("Failed to parse datetime '%s'" % dt)
        log.debug(exc)
        parsed_dt = epoch
    return parsed_dt


def cmp_datetimes(first, second):
    """Compare two datetime strings and sort by most recent.

    :param first: A string containing a datetime
    :param second: A string containing a datetime
    :returns: -1 if ``first`` is more recent, 1 if ``second`` is more recent, or 0
    if they are equivalent.
    """
    parsed_first = _safe_parse_datetime(first)
    parsed_second = _safe_parse_datetime(second)
    if parsed_first > parsed_second:
        return -1
    elif parsed_first == parsed_second:
        return 0
    else:
        return 1


def filter_enabled_jobs(jobs):
    """Given a list of chronos jobs, find those which are not disabled"""
    return [job for job in jobs if job['disabled'] is False]


def last_success_for_job(job):
    """
    Given a job, find the last time it was successful. In the case that it hasn't
    completed a successful run, return None.
    """
    return job.get('lastSuccess', None)


def last_failure_for_job(job):
    """
    Given a job, find the last time it failed. In the case that it has never failed,
    return None.
    """
    return job.get('lastError', None)


def get_status_last_run(job):
    """
    Return the time of the last run of a job and the appropriate LastRunState.
    """
    last_success = last_success_for_job(job)
    last_failure = last_failure_for_job(job)
    if not last_success and not last_failure:
        return (None, LastRunState.NotRun)
    elif not last_failure:
        return (last_success, LastRunState.Success)
    elif not last_success:
        return (last_failure, LastRunState.Fail)
    else:
        if cmp_datetimes(last_success, last_failure) <= 0:
            return (last_success, LastRunState.Success)
        else:
            return (last_failure, LastRunState.Fail)


def sort_jobs(jobs):
    """Takes a list of chronos jobs and returns a sorted list where the job
    with the most recent result is first.

    :param jobs: list of dicts of job configuration, as returned by the chronos client
    """
    def get_key(job):
        failure = last_failure_for_job(job)
        success = last_success_for_job(job)
        newest = failure if cmp_datetimes(failure, success) < 0 else success
        return _safe_parse_datetime(newest)

    return sorted(
        jobs,
        key=get_key,
        reverse=True,
    )


def lookup_chronos_jobs(client, service=None, instance=None, git_hash=None, config_hash=None, include_disabled=False):
    """Discovers Chronos jobs and filters them with ``filter_chronos_jobs()``.

    :param client: Chronos client object
    :param service: passed on to ``filter_chronos_jobs()``
    :param instance: passed on to ``filter_chronos_jobs()``
    :param git_hash: passed on to ``filter_chronos_jobs()``
    :param config_hash: passed on to ``filter_chronos_jobs()``
    :param include_disabled: passed on to ``filter_chronos_jobs()``
    :returns: list of job dicts discovered by ``client`` and filtered by
    ``filter_chronos_jobs()`` using the other parameters
    """
    jobs = client.list()
    return filter_chronos_jobs(
        jobs=jobs,
        service=service,
        instance=instance,
        git_hash=git_hash,
        config_hash=config_hash,
        include_disabled=include_disabled,
    )


def filter_chronos_jobs(jobs, service, instance, git_hash, config_hash, include_disabled):
    """Filters a list of Chronos jobs based on several criteria.

    :param jobs: a list of jobs, as calculated in ``lookup_chronos_jobs()``
    :param service: service we're looking for. If None, don't filter based on this key.
    :param instance: instance we're looking for. If None, don't filter based on this key.
    :param git_hash: git_hash we're looking for. If None, don't filter based on
    this key.
    :param config_hash: config_hash we're looking for. If None, don't filter
    based on this key.
    :param include_disabled: boolean indicating if disabled jobs should be
    included in the returned list
    :returns: list of job dicts whose name matches the arguments (if any)
    provided
    """
    matching_jobs = []
    for job in jobs:
        try:
            (job_service, job_instance, job_git_hash, job_config_hash) = decompose_job_id(job['name'])
        except InvalidJobNameError:
            continue
        if (
            (service is None or job_service == service)
            and (instance is None or job_instance == instance)
            and (git_hash is None or job_git_hash == git_hash)
            and (config_hash is None or job_config_hash == config_hash)
        ):
            if job['disabled'] and not include_disabled:
                continue
            else:
                matching_jobs.append(job)
    return matching_jobs


@timeout()
def wait_for_job(client, job_name):
    """Wait for a job to launch.

    :param client: The Chronos client
    :param job_name: The name of the job to wait for
    """
    found = False
    while not found:
        found = job_name in [job['name'] for job in client.list()]
        if found:
            return True
        else:
            print "waiting for job %s to launch. retrying" % (job_name)
            sleep(0.5)


def parse_time_variables(input_string, parse_time=None):
    """Parses an input string and uses the Tron-style dateparsing
    to replace time variables. Currently supports only the date/time
    variables listed in the tron documentation:
    https://pythonhosted.org/tron/command_context.html#built-in-command-context-variables

    :param input_string: input string to be parsed
    :param parse_time: Reference Datetime object to parse the date and time strings, defaults to now.
    :returns: A string with the date and time variables replaced
    """
    if parse_time is None:
        parse_time = datetime.datetime.now()
    # We build up a tron context object that has the right
    # methods to parse tron-style time syntax
    job_context = command_context.JobRunContext(command_context.CommandContext())
    # The tron context object needs the run_time attibute set so it knows
    # how to interpret the date strings
    job_context.job_run.run_time = parse_time
    # The job_context object works like a normal dictionary for string replacement
    return input_string % job_context


def disable_job(client, job):
    job["disabled"] = True
    log.debug("Disabling job: %s" % job)
    client.update(job)


def delete_job(client, job):
    log.debug("Deleting job: %s" % job["name"])
    client.delete(job["name"])


def create_job(client, job):
    log.debug("Creating job: %s" % job)
    client.add(job)
