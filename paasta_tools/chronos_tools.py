# Copyright 2015-2016 Yelp Inc.
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
import argparse
import csv
import datetime
import logging
import re
from collections import defaultdict
from time import sleep
from urllib.parse import urlsplit

import chronos
import dateutil
import isodate
import pytz
import service_configuration_lib
from croniter import croniter
from crontab import CronSlices

from paasta_tools import monitoring_tools
from paasta_tools.mesos_tools import get_mesos_network_for_net
from paasta_tools.mesos_tools import mesos_services_running_here
from paasta_tools.secret_tools import get_secret_hashes
from paasta_tools.tron_tools import parse_time_variables
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import time_cache
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

# this is the identifier prepended to a job name for use
# with temporary jobs, like those created by ``chronos_rerun.py``
TMP_JOB_IDENTIFIER = "tmp"

VALID_BOUNCE_METHODS = ['graceful']
EXECUTION_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

log = logging.getLogger(__name__)

# we never want to hear from this - this means
# that callers don't have to worry about setting this
logging.getLogger("crontab").setLevel(logging.CRITICAL)


# Variable used by DFS algorithm
_ENTERED, _ENTERED_AND_EXITED = range(0, 2)


class LastRunState:
    """Cheap enum to represent the state of the last run"""
    Success, Fail, NotRun = range(0, 3)


class JobType:
    """ An enum to represent job types """
    Dependent, Scheduled = range(0, 2)


class ChronosNotConfigured(Exception):
    pass


class InvalidParentError(Exception):
    pass


class ChronosConfig(dict):

    def __init__(self, config):
        super().__init__(config)

    def get_url(self):
        """:returns: The Chronos API endpoint"""
        try:
            return self['url']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos url in system chronos config')

    def get_username(self):
        """:returns: The Chronos API username"""
        try:
            return self['user']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos user in system chronos config')

    def get_password(self):
        """:returns: The Chronos API password"""
        try:
            return self['password']
        except KeyError:
            raise ChronosNotConfigured('Could not find chronos password in system chronos config')


def load_chronos_config():
    try:
        return ChronosConfig(load_system_paasta_config().get_chronos_config())
    except PaastaNotConfiguredError:
        raise ChronosNotConfigured("Could not find chronos_config in configuration directory")


class CachingChronosClient(chronos.ChronosClient):

    @time_cache(ttl=20)
    def list(self):
        return super().list()

    @time_cache(ttl=20)
    def metrics(self):
        return super().metrics()

    @time_cache(ttl=20)
    def scheduler_graph(self):
        return super().scheduler_graph()


def get_chronos_client(config, cached=False):
    """Returns a chronos client object for interacting with the API"""
    chronos_hosts = config.get_url()
    chronos_hostnames = [urlsplit(hostname).netloc for hostname in chronos_hosts]
    log.info("Attempting to connect to Chronos servers: %s" % chronos_hosts)
    if cached:
        return CachingChronosClient(
            servers=chronos_hostnames,
            username=config.get_username(),
            password=config.get_password(),
            scheduler_api_version=None,
        )
    else:
        return chronos.connect(
            servers=chronos_hostnames,
            username=config.get_username(),
            password=config.get_password(),
            scheduler_api_version=None,
        )


def compose_job_id(service, instance):
    """In chronos, a job is just service{SPACER}instance """
    return f"{service}{SPACER}{instance}"


def decompose_job_id(job_id):
    """ A custom implementation of utils.decompose_job_id, accounting for the
    possibility of TMP_JOB_IDENTIFIER being prepended to the job name """
    decomposed = job_id.split(SPACER)
    if len(decomposed) == 3:
        if not decomposed[0].startswith(TMP_JOB_IDENTIFIER):
            raise InvalidJobNameError('invalid job id %s' % job_id)
        else:
            return (decomposed[1], decomposed[2])
    elif len(decomposed) == 2:
        return (decomposed[0], decomposed[1])
    else:
        raise InvalidJobNameError('invalid job id %s' % job_id)


class InvalidChronosConfigError(Exception):
    pass


@time_cache(ttl=30)
def read_chronos_jobs_for_service(service, cluster, soa_dir=DEFAULT_SOA_DIR):
    chronos_conf_file = 'chronos-%s' % cluster
    return service_configuration_lib.read_extra_service_information(
        service,
        chronos_conf_file,
        soa_dir=soa_dir,
    )


def load_chronos_job_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> 'ChronosJobConfig':
    general_config = service_configuration_lib.read_service_configuration(
        service,
        soa_dir=soa_dir,
    )

    if instance.startswith('_'):
        raise InvalidJobNameError(
            f"Unable to load chronos job config for {service}.{instance} as instance name starts with '_'",
        )
    service_chronos_jobs = read_chronos_jobs_for_service(service, cluster, soa_dir=soa_dir)
    if instance not in service_chronos_jobs:
        raise NoConfigurationForServiceError(f'No job named "{instance}" in config file chronos-{cluster}.yaml')
    branch_dict = None
    general_config = deep_merge_dictionaries(overrides=service_chronos_jobs[instance], defaults=general_config)

    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = ChronosJobConfig(
            service=service,
            cluster=cluster,
            instance=instance,
            config_dict=general_config,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        branch = temp_instance_config.get_branch()
        deploy_group = temp_instance_config.get_deploy_group()
        branch_dict = deployments_json.get_branch_dict(service, branch, deploy_group)

    return ChronosJobConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


class ChronosJobConfig(InstanceConfig):
    config_filename_prefix = 'chronos'

    def __init__(self, service, instance, cluster, config_dict, branch_dict, soa_dir=DEFAULT_SOA_DIR):
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def get_service(self):
        return self.service

    def get_job_name(self):
        return self.instance

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
        original_env = super().get_env()
        return [{"name": key, "value": value} for key, value in original_env.items()]

    def get_calculated_constraints(self, system_paasta_config):
        constraints = self.get_constraints()
        if constraints is not None:
            return constraints
        else:
            constraints = self.get_extra_constraints()
            constraints.extend(
                self.get_deploy_constraints(
                    blacklist=self.get_deploy_blacklist(),
                    whitelist=self.get_deploy_whitelist(),
                    system_deploy_blacklist=system_paasta_config.get_deploy_blacklist(),
                    system_deploy_whitelist=system_paasta_config.get_deploy_whitelist(),
                ),
            )
            constraints.extend(self.get_pool_constraints())
        assert isinstance(constraints, list)
        return [[str(val) for val in constraint] for constraint in constraints]

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

    def get_schedule_interval_in_seconds(self, seconds_ago=0):
        """Return the job interval in seconds or None if there is no interval

        :params seconds_ago: return an interval the job had in the past
        """
        schedule = self.get_schedule()
        if schedule is None:
            return None

        if CronSlices.is_valid(schedule):
            try:
                job_tz = pytz.timezone(self.get_schedule_time_zone())
            except (pytz.exceptions.UnknownTimeZoneError, AttributeError):
                job_tz = pytz.utc
            c = croniter(schedule, datetime.datetime.now(job_tz) - datetime.timedelta(seconds=seconds_ago))
            return c.get_next() - c.get_prev()
        else:
            try:
                _, _, interval = self.get_schedule().split('/')
                return int(float(isodate.parse_duration(interval).total_seconds()))
            except (isodate.ISO8601Error, ValueError):
                return None

    def get_schedule_time_zone(self):
        return self.config_dict.get('schedule_time_zone')

    def get_parents(self):
        parents = self.config_dict.get('parents', [])
        if parents is not None and not isinstance(parents, list):
            return [parents]
        else:
            return parents

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

    def check_parents(self):
        parents = self.get_parents()
        if parents is not None:
            results = [(parent, check_parent_format(parent)) for parent in parents]
            badly_formatted = [res[0] for res in results if res[1] is False]
            if len(badly_formatted) > 0:
                return False, ('The job name(s) %s is not formatted correctly: expected service.instance'
                               % ", ".join(badly_formatted))
        return True, ''

    def check_cmd(self):
        command = self.get_cmd()
        if command:
            try:
                parse_time_variables(command=command)
                return True, ""
            except (ValueError, KeyError, TypeError):
                return False, ("Unparseable command '%s'. Hint: do you need to escape {} chars?") % command
        else:
            return True, ""

    # a valid 'repeat_string' is 'R' or 'Rn', where n is a positive integer representing the number of times to repeat
    # more info: https://en.wikipedia.org/wiki/ISO_8601#Repeating_intervals
    def _check_schedule_repeat_helper(self, repeat_string):
        pattern = re.compile(r'^R\d*$')
        return pattern.match(repeat_string) is not None

    def check_schedule(self):
        msgs = []
        schedule = self.get_schedule()

        if schedule is not None:
            if not CronSlices.is_valid(schedule):
                try:
                    repeat, start_time, interval = schedule.split('/')  # the parts have separate validators
                except ValueError:
                    return (
                        False, (
                            'The specified schedule "%s" is neither a valid cron schedule nor a valid'
                            ' ISO 8601 schedule' % schedule
                        ),
                    )

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
                        msgs.append('The specified start time "%s" in schedule "%s" does '
                                    'not conform to the ISO 8601 format:\n%s' % (start_time, schedule, str(exc)))

                parsed_interval = None
                try:
                    # 'interval' and 'duration' are interchangeable terms
                    parsed_interval = isodate.parse_duration(interval)
                except isodate.ISO8601Error:
                    msgs.append('The specified interval "%s" in schedule "%s" '
                                'does not conform to the ISO 8601 format.' % (interval, schedule))

                # don't allow schedules more frequent than every minute we have
                # to be careful here, since the isodate library returns
                # different datatypes according to whether there is a
                # yearly/monthly period (and if that year or month period is
                # 0).  unfortunately, the isodate library *is* okay with you
                # specifying fractional and negative periods. Chronos's parser
                # will barf at a fractional period, but be okay with a negative
                # one, so if someone does try to do something like "R1//P0.01M"
                # then the API request to upload the job will fail.  TODO:
                # detect when someone is trying to add a fractional period?
                if (parsed_interval and isinstance(parsed_interval, datetime.timedelta) and
                        parsed_interval < datetime.timedelta(seconds=60)):
                    msgs.append('Unsupported interval "%s": jobs must be run at an interval of > 60 seconds' % interval)

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
            'disk': self.check_disk,
            'schedule': self.check_schedule,
            'scheduleTimeZone': self.check_schedule_time_zone,
            'security': self.check_security,
            'dependencies_reference': self.check_dependencies_reference,
            'parents': self.check_parents,
            'cmd': self.check_cmd,
        }
        supported_params_without_checks = ['description', 'owner', 'disabled']
        if param in check_methods:
            return check_methods[param]()
        elif param in supported_params_without_checks:
            return True, ''
        else:
            return False, 'Your Chronos config specifies "%s", an unsupported parameter.' % param

    def format_chronos_job_dict(self, docker_url, docker_volumes, docker_cfg_location, constraints):
        valid, error_msgs = self.validate()
        if not valid:
            raise InvalidChronosConfigError("\n".join(error_msgs))

        net = get_mesos_network_for_net(self.get_net())
        command = parse_time_variables(self.get_cmd()) if self.get_cmd() else self.get_cmd()

        complete_config = {
            'name': self.get_job_name(),
            'container': {
                'image': docker_url,
                'network': net,
                'type': 'DOCKER',
                'volumes': docker_volumes,
                'parameters': self.format_docker_parameters(),
            },
            'uris': [docker_cfg_location],
            'environmentVariables': self.get_env(),
            'mem': self.get_mem(),
            'cpus': self.get_cpus(),
            'disk': self.get_disk(),
            'constraints': constraints,
            'command': command,
            'arguments': self.get_args(),
            'epsilon': self.get_epsilon(),
            'retries': self.get_retries(),
            'async': False,  # we don't support async jobs
            'disabled': self.get_disabled(),
            'owner': self.get_owner(),
            'scheduleTimeZone': self.get_schedule_time_zone(),
            'shell': self.get_shell(),
        }
        if self.get_schedule() is not None:
            complete_config['schedule'] = self.get_schedule()
        else:
            # The input to parents is the normal paasta syntax, but for chronos we have to
            # convert it to what chronos expects, which uses its own spacer
            complete_config["parents"] = [paasta_to_chronos_job_name(parent) for parent in self.get_parents()]
        return complete_config

    # 'docker job' requirements: https://mesos.github.io/chronos/docs/api.html#adding-a-docker-job
    def validate(self):
        error_msgs = []
        # Use InstanceConfig to validate shared config keys like cpus and mem
        error_msgs.extend(super().validate())

        for param in [
            'epsilon', 'retries', 'cpus', 'mem', 'disk',
            'schedule', 'scheduleTimeZone', 'parents', 'cmd',
            'security', 'dependencies_reference',
        ]:
            check_passed, check_msg = self.check(param)
            if not check_passed:
                error_msgs.append(check_msg)

        return len(error_msgs) == 0, error_msgs

    def get_healthcheck_mode(self, _):
        # Healthchecks are not supported yet in chronos
        return None

    def get_desired_state_human(self):
        desired_state = self.get_desired_state()
        if desired_state == 'start':
            return PaastaColors.bold('Scheduled')
        elif desired_state == 'stop':
            return PaastaColors.bold('Disabled')
        else:
            return PaastaColors.red('Unknown (desired_state: %s)' % desired_state)

    def get_nerve_namespace(self):
        """Currently not implemented yet, as we don't have the code
        to announce chronos tasks in nerve"""
        return None


def paasta_to_chronos_job_name(job_name):
    """Converts a paasta-style job name service.main to what chronos
    will have in its api as the job name: "service foo" """
    return job_name.replace(INTERNAL_SPACER, SPACER)


def list_job_names(service, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """A chronos-specific wrapper around utils.get_service_instance_list.

    :param name: The service name
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, job) for each job defined for the service name
    """
    return get_service_instance_list(service, cluster, 'chronos', soa_dir)


def get_chronos_jobs_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """A chronos-specific wrapper around utils.get_services_for_cluster

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, job_name)"""
    return get_services_for_cluster(cluster, 'chronos', soa_dir)


def create_complete_config(service, job_name, soa_dir=DEFAULT_SOA_DIR):
    """Generates a complete dictionary to be POST'ed to create a job on Chronos"""
    system_paasta_config = load_system_paasta_config()
    chronos_job_config = load_chronos_job_config(
        service, job_name, system_paasta_config.get_cluster(), soa_dir=soa_dir,
    )
    docker_url = chronos_job_config.get_docker_url()
    docker_volumes = chronos_job_config.get_volumes(system_volumes=system_paasta_config.get_volumes())

    constraints = chronos_job_config.get_calculated_constraints(
        system_paasta_config=system_paasta_config,
    )

    complete_config = chronos_job_config.format_chronos_job_dict(
        docker_url=docker_url,
        docker_volumes=docker_volumes,
        docker_cfg_location=system_paasta_config.get_dockercfg_location(),
        constraints=constraints,
    )

    complete_config['name'] = compose_job_id(service, job_name)

    # resolve conflicts between the 'desired_state' and soa_configs disabled
    # flag.
    desired_state = chronos_job_config.get_desired_state()
    soa_disabled_state = complete_config['disabled']

    resolved_disabled_state = determine_disabled_state(
        desired_state,
        soa_disabled_state,
    )
    complete_config['disabled'] = resolved_disabled_state

    config_for_hash = get_config_for_bounce_hash(
        complete_config=complete_config,
        service=service,
        soa_dir=soa_dir,
        system_paasta_config=system_paasta_config,
    )
    # we use the undocumented description field to store a hash of the chronos config.
    # this makes it trivial to compare configs and know when to bounce.
    complete_config['description'] = get_config_hash(
        config=config_for_hash,
        force_bounce=chronos_job_config.get_force_bounce(),
    )

    log.debug("Complete configuration for instance is: %s" % complete_config)
    return complete_config


def get_config_for_bounce_hash(complete_config, service, soa_dir, system_paasta_config):
    environment_variables = {x['name']: x['value'] for x in complete_config['environmentVariables']}
    secret_hashes = get_secret_hashes(
        environment_variables=environment_variables,
        secret_environment=system_paasta_config.get_vault_environment(),
        service=service,
        soa_dir=soa_dir,
    )
    if secret_hashes:
        complete_config['paasta_secrets'] = secret_hashes
    return complete_config


def determine_disabled_state(branch_state, soa_disabled_state):
    """Determine the *real* disabled state for a job. There are two sources of truth for
    the disabled/enabled state of a job: the 'disabled' flag in soa-configs,
    and the state set in deployments.json by `paasta stop` or `paasta start`.
    If the two conflict, then this determines the 'real' state for it.

    We take a defensive stance here: if either source has disabled as True,
    then we go with that - better safe than sorry I guess?

    Note that this means that `paasta start` and `paasta emergency-start` are noop if you
    have 'disabled:True' in your soa-config.

    :param branch_state: the desired state on the branch - either start or
        stop.
    :param soa_disabled_state: the value of the disabled state in soa-configs.
    """
    if branch_state not in ['start', 'stop']:
        raise ValueError('Expected branch_state to be start or stop')

    if branch_state == 'start' and soa_disabled_state is True:
        return True
    elif branch_state == 'stop' and soa_disabled_state is False:
        return True
    else:
        return soa_disabled_state


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


def get_job_type(job):
    """ Given a job, return a chronos_tools.JobType """
    if job.get('schedule') is not None:
        return JobType.Scheduled
    elif job.get('parents') is not None:
        return JobType.Dependent
    else:
        raise ValueError('Expected either schedule field or parents field')


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


def get_chronos_status_for_job(client, service, instance):
    """
    Returns the status (queued, idle, running, etc.) for a specific job from
    the undocumented Chronos csv api that has the following format:

    node,example_job,success,queued
    node,parent_job,success,idle
    node,child_job,success,idle
    link,parent_job,child_job

    :param client: Chronos client object
    :param service: service name
    :param instance: instance name
    """
    resp = client.scheduler_graph()
    lines = csv.reader(resp.splitlines())
    for line in lines:
        if line[0] == "node" and line[1] == compose_job_id(service, instance):
            return line[3]


def lookup_chronos_jobs(client, service=None, instance=None, include_disabled=False, include_temporary=False):
    """Discovers Chronos jobs and filters them with ``filter_chronos_jobs()``.

    :param client: Chronos client object
    :param service: passed on to ``filter_chronos_jobs()``
    :param instance: passed on to ``filter_chronos_jobs()``
    :param include_disabled: passed on to ``filter_chronos_jobs()``
    :returns: list of job dicts discovered by ``client`` and filtered by
        ``filter_chronos_jobs()`` using the other parameters
    """
    jobs = client.list()
    return filter_chronos_jobs(
        jobs=jobs,
        service=service,
        instance=instance,
        include_disabled=include_disabled,
        include_temporary=include_temporary,
    )


def filter_non_temporary_chronos_jobs(jobs):
    """
    Given a list of Chronos jobs, as pulled from the API, remove those
    which are defined as 'temporary' jobs.

    :param jobs: a list of chronos jobs
    :returns: a list of chronos jobs containing the same jobs as those provided
        by the ``jobs`` parameter, but with temporary jobs removed.
    """
    return [job for job in jobs if not job['name'].startswith(TMP_JOB_IDENTIFIER)]


def filter_chronos_jobs(jobs, service, instance, include_disabled, include_temporary):
    """Filters a list of Chronos jobs based on several criteria.

    :param jobs: a list of jobs, as calculated in ``lookup_chronos_jobs()``
    :param service: service we're looking for. If None, don't filter based on this key.
    :param instance: instance we're looking for. If None, don't filter based on this key.
    :param include_disabled: boolean indicating if disabled jobs should be
        included in the returned list
    :returns: list of job dicts whose name matches the arguments (if any)
        provided
    """
    matching_jobs = []
    for job in jobs:
        try:
            job_service, job_instance = decompose_job_id(job['name'])
        except InvalidJobNameError:
            continue
        if (
            (service is None or job_service == service) and
            (instance is None or job_instance == instance)
        ):
            if job['disabled'] and not include_disabled:
                continue
            else:
                matching_jobs.append(job)
    if include_temporary is True:
        return matching_jobs
    else:
        return filter_non_temporary_chronos_jobs(matching_jobs)


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
            paasta_print("waiting for job %s to launch. retrying" % (job_name))
            sleep(0.5)


def parse_execution_date(execution_date_string):
    """Parses execution_date_string into a datetime object.
    Can be used as an ArgumentParser `type=` hint.
    :param execution_date_string: string
    :return: a datetime.datetime instance
    :raise: ArgumentTypeError
    """
    try:
        parsed = datetime.datetime.strptime(execution_date_string, EXECUTION_DATE_FORMAT)
    except ValueError:
        raise argparse.ArgumentTypeError('must be in the format "%s"' % EXECUTION_DATE_FORMAT)
    return parsed


def uses_time_variables(chronos_job):
    """
    Given a chronos job config, returns True the if the command uses time variables
    """
    current_command = chronos_job.get_cmd()
    interpolated_command = parse_time_variables(
        command=current_command,
        parse_time=datetime.datetime.utcnow(),
    )
    return current_command != interpolated_command


def check_parent_format(parent):
    """ A predicate defining if the parent field string is formatted correctly """
    return len(parent.split(".")) == 2


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


def update_job(client, job):
    log.debug("Updating job: %s" % job)
    client.update(job)


def get_jobs_for_service_instance(service, instance, include_disabled=True, include_temporary=False):
    """A wrapper for lookup_chronos_jobs that includes building the necessary chronos client.
    :param service: the service to be queried
    :param instance: the instance to be queried
    :param include_disabled: include disabled jobs in the search. default: True
    :param include_temporary: include temporary in the search. default: False
    :returns: the jobs for a given service instance
    """
    chronos_config = load_chronos_config()
    matching_jobs = lookup_chronos_jobs(
        client=get_chronos_client(chronos_config),
        service=service,
        instance=instance,
        include_disabled=include_disabled,
        include_temporary=include_temporary,
    )
    return sort_jobs(matching_jobs)


def compose_check_name_for_service_instance(check_name, service, instance):
    """Compose a sensu check name for a given job"""
    return f'{check_name}.{service}{INTERNAL_SPACER}{instance}'


def get_temporary_jobs_for_service_instance(client, service, instance):
    """ Given a service and instance, find any temporary jobs
    for that job, as created by chronos_rerun.
    """
    temporary_jobs = []
    all_jobs = lookup_chronos_jobs(
        client=client,
        service=service,
        instance=instance,
        include_disabled=True,
        include_temporary=True,
    )
    for job in all_jobs:
        if job['name'].startswith(TMP_JOB_IDENTIFIER):
            temporary_jobs.append(job)
    return temporary_jobs


def is_temporary_job(job):
    """Assert whether a job is a temporary one, identifiable
    by it's name starting with TMP_JOB_IDENTIFIER
    :param job: the chronos job to check
    :returns: a boolean indicating if a job is a temporary job
    """
    return job['name'].startswith(TMP_JOB_IDENTIFIER)


def chronos_services_running_here():
    """See what chronos services are being run by a mesos-slave on this host.
    :returns: A list of triples of (service, instance, port)"""

    return mesos_services_running_here(
        framework_filter=lambda fw: fw['name'].startswith('chronos'),
        parse_service_instance_from_executor_id=lambda task_id: decompose_job_id(task_id.split(MESOS_TASK_SPACER)[3]),
    )


ENTERED, ENTERED_AND_EXITED = range(0, 2)


def dfs(node, neighbours_mapping, ignore_cycles=False, node_status=None):
    """
    Execute Deep-First search on a graph starting from a specific `node`.

    `neighbours_mapping` contains the graph links as adjacency list
        neighbours_mapping[x] = [y, z] means that the node `x` as two outgoing edges/links directed to `y` and `z`

    :param node: starting node for DFS execution
    :param neighbours_mapping: graph adjacency list
    :type neighbours_mapping: dict
    :param ignore_cycles: raise an exception if a cycle is identified in the graph
    :type ignore_cycles: bool
    :param node_status: dictionary that keep tracks of the already visited nodes.
        NOTE: this is meant for internal usage only, please do NOT set it during method call
    :type node_status: dict

    :return: list of visited nodes sorted by exit order
    :type: list
    """
    visited_nodes = []

    if node_status is None:
        node_status = {}

    if node_status.get(node) == _ENTERED_AND_EXITED:
        return []

    node_status[node] = _ENTERED
    for current_node in neighbours_mapping[node]:
        current_node_state = node_status.get(current_node, None)
        if current_node_state == _ENTERED:
            if ignore_cycles:
                continue
            else:
                raise ValueError("cycle")
        if current_node_state == _ENTERED_AND_EXITED:
            continue

        visited_nodes.extend(
            dfs(
                node=current_node,
                neighbours_mapping=neighbours_mapping,
                node_status=node_status,
                ignore_cycles=ignore_cycles,
            ),
        )

    visited_nodes.append(node)
    node_status[node] = _ENTERED_AND_EXITED
    return visited_nodes


@time_cache(ttl=30)
def _get_related_jobs_and_configs(cluster, soa_dir=DEFAULT_SOA_DIR):
    """
    For all the Chronos jobs defined in cluster, extract the list of topologically sorted related Chronos.
    Two individual jobs are considered related each other if exists a dependency relationship between them.

    :param cluster: cluster from which extracting the jobs
    :return: tuple(related jobs mapping, jobs configuration)
    """
    chronos_configs = {}
    for service in service_configuration_lib.read_services_configuration(soa_dir=soa_dir):
        for instance in read_chronos_jobs_for_service(service=service, cluster=cluster, soa_dir=soa_dir):
            if instance.startswith('_'):  # some dependencies are templates, ignore
                continue
            try:
                chronos_configs[(service, instance)] = load_chronos_job_config(
                    service=service,
                    instance=instance,
                    cluster=cluster,
                    soa_dir=soa_dir,
                )
            except NoDeploymentsAvailable:
                pass

    adjacency_list = defaultdict(set)  # List of adjacency used by dfs algorithm
    for instance, config in chronos_configs.items():
        for parent in config.get_parents():
            parent = decompose_job_id(paasta_to_chronos_job_name(parent))
            # Map the graph a undirected graph to simplify identification of connected components
            adjacency_list[parent].add(instance)
            adjacency_list[instance].add(parent)

    def cached_dfs(known_dfs_results, node, neighbours_mapping, ignore_cycles=False):
        """
        Cached version of DFS algorithm (tuned for extraction of connected components).

        :param known_dfs_results: previous results of DFS
        :type known_dfs_results: list
        :param node: starting node for DFS execution
        :param neighbours_mapping: graph adjacency list
        :type neighbours_mapping: dict
        :param ignore_cycles: raise an exception if a cycle is identified in the graph
        :type ignore_cycles: bool

        :return: set of nodes part of the same connected component of `node`
        :type: set
        """
        for known_dfs_result in known_dfs_results:
            if node in known_dfs_result:
                return known_dfs_result

        visited_nodes = set(dfs(
            node=node,
            neighbours_mapping=neighbours_mapping,
            ignore_cycles=ignore_cycles,
        ))
        known_dfs_results.append(visited_nodes)

        return visited_nodes

    # Build connected components
    known_connected_components = []
    for instance in chronos_configs:
        cached_dfs(  # Using cached version of DFS to avoid all algorithm execution if the result it already know
            known_dfs_results=known_connected_components,
            node=instance,
            ignore_cycles=True,  # Operating on an undirected graph to simplify identification of connected components
            neighbours_mapping=adjacency_list,
        )

    connected_components = {}
    for connected_component in known_connected_components:
        for node in connected_component:
            connected_components[node] = connected_component

    return connected_components, chronos_configs


def get_related_jobs_configs(cluster, service, instance, soa_dir=DEFAULT_SOA_DIR, use_cache=True):
    """
    Extract chronos configurations of all Chronos jobs related to the (cluster, service, instance)

    :return: job-config mapping. Job identifier is a tuple (service, instance)
    """
    kwargs = {}
    if not use_cache:
        kwargs['ttl'] = -1  # Disable usage of time_cached value

    connected_components, chronos_configs = _get_related_jobs_and_configs(cluster, soa_dir=soa_dir, **kwargs)
    related_jobs = connected_components[(service, instance)]
    return {
        job: config
        for job, config in chronos_configs.items()
        if job in related_jobs
    }


def topological_sort_related_jobs(cluster, service, instance, soa_dir=DEFAULT_SOA_DIR):
    """
    Extract the topological order of all the jobs related to the specified instance.
    :return: sorted list of related jobs.
            The list is ordered such that job with index `i` could be executed in respect of its dependencies
            if all jobs with index smaller than `i` have terminated.
    """
    def _sort(nodes):
        adjacency_list = {
            node: [
                decompose_job_id(paasta_to_chronos_job_name(parent_job))
                for parent_job in chronos_configs[node].get_parents()
            ]
            for node in related_jobs
        }

        order, enter, node_status = list(), {n for n in nodes}, {}

        while enter:
            order.extend(
                dfs(
                    node=enter.pop(),
                    neighbours_mapping=adjacency_list,
                    node_status=node_status,
                    ignore_cycles=False,
                ),
            )

        return order

    connected_components, chronos_configs = _get_related_jobs_and_configs(cluster, soa_dir=soa_dir)
    related_jobs = connected_components[(service, instance)]

    return _sort(
        nodes=related_jobs,
    )
