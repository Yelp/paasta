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
from __future__ import print_function

import contextlib
import copy
import datetime
import errno
import fcntl
import glob
import hashlib
import importlib
import io
import json
import logging
import math
import os
import pwd
import re
import shlex
import signal
import sys
import tempfile
import threading
from collections import OrderedDict
from fnmatch import fnmatch
from functools import wraps
from subprocess import PIPE
from subprocess import Popen
from subprocess import STDOUT

import dateutil.tz
import requests_cache
import service_configuration_lib
import yaml
from docker import Client
from docker.utils import kwargs_from_env
from kazoo.client import KazooClient

import paasta_tools


# DO NOT CHANGE SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name and instance
SPACER = '.'
INFRA_ZK_PATH = '/nail/etc/zookeeper_discovery/infrastructure/'
PATH_TO_SYSTEM_PAASTA_CONFIG_DIR = os.environ.get('PAASTA_SYSTEM_CONFIG_DIR', '/etc/paasta/')
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
DEFAULT_DOCKERCFG_LOCATION = "file:///root/.dockercfg"
DEPLOY_PIPELINE_NON_DEPLOY_STEPS = (
    'itest',
    'security-check',
    'performance-check',
    'push-to-registry'
)
# Default values for _log
ANY_CLUSTER = 'N/A'
ANY_INSTANCE = 'N/A'
DEFAULT_LOGLEVEL = 'event'
no_escape = re.compile('\x1B\[[0-9;]*[mK]')

DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT = "http://{host:s}:{port:d}/;csv;norefresh"

DEFAULT_CPU_PERIOD = 100000
DEFAULT_CPU_BURST_PCT = 900

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

INSTANCE_TYPES = ('marathon', 'chronos', 'paasta_native')


class InvalidInstanceConfig(Exception):
    pass


class InstanceConfig(dict):

    def __init__(self, cluster, instance, service, config_dict, branch_dict):
        self.config_dict = config_dict
        self.branch_dict = branch_dict
        self.cluster = cluster
        self.instance = instance
        self.service = service
        config_interpolation_keys = ('deploy_group',)
        interpolation_facts = self.__get_interpolation_facts()
        for key in config_interpolation_keys:
            if key in self.config_dict:
                self.config_dict[key] = self.config_dict[key].format(**interpolation_facts)

    def __get_interpolation_facts(self):
        return {
            'cluster': self.cluster,
            'instance': self.instance,
            'service': self.service,
        }

    def get_cluster(self):
        return self.cluster

    def get_instance(self):
        return self.instance

    def get_service(self):
        return self.service

    def get_branch(self):
        return SPACER.join((self.get_cluster(), self.get_instance()))

    def get_deploy_group(self):
        return self.config_dict.get('deploy_group', self.get_branch())

    def get_mem(self):
        """Gets the memory required from the service's configuration.

        Defaults to 1024 (1G) if no value specified in the config.

        :returns: The amount of memory specified by the config, 1024 if not specified"""
        mem = self.config_dict.get('mem', 1024)
        return mem

    def get_mem_swap(self):
        """Gets the memory-swap value. This value is passed to the docker
        container to ensure that the total memory limit (memory + swap) is the
        same value as the 'mem' key in soa-configs. Note - this value *has* to
        be >= to the mem key, so we always round up to the closest MB.
        """
        mem = self.get_mem()
        mem_swap = int(math.ceil(mem))
        return "%sm" % mem_swap

    def get_cpus(self):
        """Gets the number of cpus required from the service's configuration.

        Defaults to .25 (1/4 of a cpu) if no value specified in the config.

        :returns: The number of cpus specified in the config, .25 if not specified"""
        cpus = self.config_dict.get('cpus', .25)
        return cpus

    def get_cpu_period(self):
        """The --cpu-period option to be passed to docker
        Comes from the cfs_period_us configuration option

        :returns: The number to be passed to the --cpu-period docker flag"""
        return self.config_dict.get('cfs_period_us', DEFAULT_CPU_PERIOD)

    def get_cpu_quota(self):
        """Gets the --cpu-quota option to be passed to docker
        Calculated from the cpu_burst_pct configuration option, which is the percent
        over its declared cpu usage that a container will be allowed to go.

        Calculation: cpus * cfs_period_us * (100 + cpu_burst_pct) / 100

        :returns: The number to be passed to the --cpu-quota docker flag"""
        cpu_burst_pct = self.config_dict.get('cpu_burst_pct', DEFAULT_CPU_BURST_PCT)
        return self.get_cpus() * self.get_cpu_period() * (100 + cpu_burst_pct) / 100

    def get_ulimit(self):
        """Get the --ulimit options to be passed to docker
        Generated from the ulimit configuration option, which is a dictionary
        of ulimit values. Each value is a dictionary itself, with the soft
        limit stored under the 'soft' key and the optional hard limit stored
        under the 'hard' key.

        Example configuration: {'nofile': {soft: 1024, hard: 2048}, 'nice': {soft: 20}}

        :returns: A generator of ulimit options to be passed as --ulimit flags"""
        for key, val in sorted(self.config_dict.get('ulimit', {}).iteritems()):
            soft = val.get('soft')
            hard = val.get('hard')
            if soft is None:
                raise InvalidInstanceConfig(
                    'soft limit missing in ulimit configuration for {0}.'.format(key)
                )
            combined_val = '%i' % soft
            if hard is not None:
                combined_val += ':%i' % hard
            yield {"key": "ulimit", "value": "{0}={1}".format(key, combined_val)}

    def get_cap_add(self):
        """Get the --cap-add options to be passed to docker
        Generated from the cap_add configuration option, which is a list of
        capabilities.

        Example configuration: {'cap_add': ['IPC_LOCK', 'SYS_PTRACE']}

        :returns: A generator of cap_add options to be passed as --cap-add flags"""
        for value in self.config_dict.get('cap_add', []):
            yield {"key": "cap-add", "value": "{0}".format(value)}

    def format_docker_parameters(self):
        """Formats extra flags for running docker.  Will be added in the format
        `["--%s=%s" % (e['key'], e['value']) for e in list]` to the `docker run` command
        Note: values must be strings

        :returns: A list of parameters to be added to docker run"""
        parameters = [{"key": "memory-swap", "value": self.get_mem_swap()},
                      {"key": "cpu-period", "value": "%s" % int(self.get_cpu_period())},
                      {"key": "cpu-quota", "value": "%s" % int(self.get_cpu_quota())}]
        parameters.extend(self.get_ulimit())
        parameters.extend(self.get_cap_add())
        return parameters

    def get_disk(self):
        """Gets the  amount of disk space required from the service's configuration.

        Defaults to 1024 (1G) if no value is specified in the config.

        :returns: The amount of disk space specified by the config, 1024 if not specified"""
        disk = self.config_dict.get('disk', 1024)
        return disk

    def get_cmd(self):
        """Get the docker cmd specified in the service's configuration.

        Defaults to null if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get('cmd', None)

    def get_env_dictionary(self):
        """A dictionary of key/value pairs that represent environment variables
        to be injected to the container environment"""
        env = {
            "PAASTA_SERVICE": self.service,
            "PAASTA_INSTANCE": self.instance,
            "PAASTA_CLUSTER": self.cluster,
            "PAASTA_DOCKER_IMAGE": self.get_docker_image(),
        }
        user_env = self.config_dict.get('env', {})
        env.update(user_env)
        return env

    def get_env(self):
        """Basic get_env that simply returns the basic env, other classes
        might need to override this getter for more implementation-specific
        env getting"""
        return self.get_env_dictionary()

    def get_args(self):
        """Get the docker args specified in the service's configuration.

        If not specified in the config and if cmd is not specified, defaults to an empty array.
        If not specified in the config but cmd is specified, defaults to null.
        If specified in the config and if cmd is also specified, throws an exception. Only one may be specified.

        :param service_config: The service instance's configuration dictionary
        :returns: An array of args specified in the config,
            ``[]`` if not specified and if cmd is not specified,
            otherwise None if not specified but cmd is specified"""
        if self.get_cmd() is None:
            return self.config_dict.get('args', [])
        else:
            args = self.config_dict.get('args', None)
            if args is None:
                return args
            else:
                # TODO validation stuff like this should be moved into a check_* like in chronos tools
                raise InvalidInstanceConfig('Instance configuration can specify cmd or args, but not both.')

    def get_monitoring(self):
        """Get monitoring overrides defined for the given instance"""
        return self.config_dict.get('monitoring', {})

    def get_deploy_blacklist(self):
        """The deploy blacklist is a list of lists, where the lists indicate
        which locations the service should not be deployed"""
        return self.config_dict.get('deploy_blacklist', [])

    def get_deploy_whitelist(self):
        """The deploy whitelist is a list of lists, where the lists indicate
        which locations are explicitly allowed.  The blacklist will supersede
        this if a host matches both the white and blacklists."""
        return self.config_dict.get('deploy_whitelist', [])

    def get_monitoring_blacklist(self):
        """The monitoring_blacklist is a list of tuples, where the tuples indicate
        which locations the user doesn't care to be monitored"""
        return self.config_dict.get('monitoring_blacklist', self.get_deploy_blacklist())

    def get_docker_image(self):
        """Get the docker image name (with tag) for a given service branch from
        a generated deployments.json file."""
        return self.branch_dict.get('docker_image', '')

    def get_desired_state(self):
        """Get the desired state (either 'start' or 'stop') for a given service
        branch from a generated deployments.json file."""
        return self.branch_dict.get('desired_state', 'start')

    def get_force_bounce(self):
        """Get the force_bounce token for a given service branch from a generated
        deployments.json file. This is a token that, when changed, indicates that
        the instance should be recreated and bounced, even if no other
        parameters have changed. This may be None or a string, generally a
        timestamp.
        """
        return self.branch_dict.get('force_bounce', None)

    def check_cpus(self):
        cpus = self.get_cpus()
        if cpus is not None:
            if not isinstance(cpus, (float, int)):
                return False, 'The specified cpus value "%s" is not a valid float or int.' % cpus
        return True, ''

    def check_mem(self):
        mem = self.get_mem()
        if mem is not None:
            if not isinstance(mem, (float, int)):
                return False, 'The specified mem value "%s" is not a valid float or int.' % mem
        return True, ''

    def check_disk(self):
        disk = self.get_disk()
        if disk is not None:
            if not isinstance(disk, (float, int)):
                return False, 'The specified disk value "%s" is not a valid float or int.' % disk
        return True, ''

    def check(self, param):
        check_methods = {
            'cpus': self.check_cpus,
            'mem': self.check_mem,
        }
        if param in check_methods:
            return check_methods[param]()
        else:
            return False, 'Your Chronos config specifies "%s", an unsupported parameter.' % param

    def validate(self):
        error_msgs = []
        for param in ['cpus', 'mem']:
            check_passed, check_msg = self.check(param)
            if not check_passed:
                error_msgs.append(check_msg)
        return error_msgs

    def get_extra_volumes(self):
        """Extra volumes are a specially formatted list of dictionaries that should
        be bind mounted in a container The format of the dictionaries should
        conform to the `Mesos container volumes spec
        <https://mesosphere.github.io/marathon/docs/native-docker.html>`_"""
        return self.config_dict.get('extra_volumes', [])

    def get_pool(self):
        """Which pool of nodes this job should run on. This can be used to mitigate noisy neighbors, by putting
        particularly noisy or noise-sensitive jobs into different pools.

        This is implemented with an attribute "pool" on each mesos slave and by adding a constraint to Marathon/Chronos
        application defined by this instance config.

        Eventually this may be implemented with Mesos roles, once a framework can register under multiple roles.

        :returns: the "pool" attribute in your config dict, or the string "default" if not specified."""
        return self.config_dict.get('pool', 'default')

    def get_pool_constraints(self):
        pool = self.get_pool()
        return [["pool", "LIKE", pool]]

    def get_constraints(self):
        return self.config_dict.get('constraints', None)

    def get_extra_constraints(self):
        return self.config_dict.get('extra_constraints', [])

    def get_net(self):
        """
        :returns: the docker networking mode the container should be started with.
        """
        return self.config_dict.get('net', 'bridge')


def validate_service_instance(service, instance, cluster, soa_dir):
    for instance_type in INSTANCE_TYPES:
        services = get_services_for_cluster(cluster=cluster, instance_type=instance_type, soa_dir=soa_dir)
        if (service, instance) in services:
            return instance_type
    else:
        print ("Error: %s doesn't look like it has been deployed to this cluster! (%s)"
               % (compose_job_id(service, instance), cluster))
        sys.exit(3)


def compose(func_one, func_two):
    def composed(*args, **kwargs):
        return func_one(func_two(*args, **kwargs))
    return composed


class PaastaColors:

    """Collection of static variables and methods to assist in coloring text."""
    # ANSI colour codes
    BLUE = '\033[34m'
    BOLD = '\033[1m'
    CYAN = '\033[36m'
    DEFAULT = '\033[0m'
    GREEN = '\033[32m'
    GREY = '\033[38;5;242m'
    MAGENTA = '\033[35m'
    RED = '\033[31m'
    YELLOW = '\033[33m'

    @staticmethod
    def bold(text):
        """Return bolded text.

        :param text: a string
        :return: text colour coded with ANSI bold
        """
        return PaastaColors.color_text(PaastaColors.BOLD, text)

    @staticmethod
    def blue(text):
        """Return text that can be printed blue.

        :param text: a string
        :return: text colour coded with ANSI blue
        """
        return PaastaColors.color_text(PaastaColors.BLUE, text)

    @staticmethod
    def green(text):
        """Return text that can be printed green.

        :param text: a string
        :return: text colour coded with ANSI green"""
        return PaastaColors.color_text(PaastaColors.GREEN, text)

    @staticmethod
    def red(text):
        """Return text that can be printed red.

        :param text: a string
        :return: text colour coded with ANSI red"""
        return PaastaColors.color_text(PaastaColors.RED, text)

    @staticmethod
    def magenta(text):
        """Return text that can be printed magenta.

        :param text: a string
        :return: text colour coded with ANSI magenta"""
        return PaastaColors.color_text(PaastaColors.MAGENTA, text)

    @staticmethod
    def color_text(color, text):
        """Return text that can be printed color.

        :param color: ANSI colour code
        :param text: a string
        :return: a string with ANSI colour encoding"""
        # any time text returns to default, we want to insert our color.
        replaced = text.replace(PaastaColors.DEFAULT, PaastaColors.DEFAULT + color)
        # then wrap the beginning and end in our color/default.
        return color + replaced + PaastaColors.DEFAULT

    @staticmethod
    def cyan(text):
        """Return text that can be printed cyan.

        :param text: a string
        :return: text colour coded with ANSI cyan"""
        return PaastaColors.color_text(PaastaColors.CYAN, text)

    @staticmethod
    def yellow(text):
        """Return text that can be printed yellow.

        :param text: a string
        :return: text colour coded with ANSI yellow"""
        return PaastaColors.color_text(PaastaColors.YELLOW, text)

    @staticmethod
    def grey(text):
        return PaastaColors.color_text(PaastaColors.GREY, text)

    @staticmethod
    def default(text):
        return PaastaColors.color_text(PaastaColors.DEFAULT, text)


LOG_COMPONENTS = OrderedDict([
    ('build', {
        'color': PaastaColors.blue,
        'help': 'Jenkins build jobs output, like the itest, promotion, security checks, etc.',
        'source_env': 'devc',
    }),
    ('deploy', {
        'color': PaastaColors.cyan,
        'help': 'Output from the paasta deploy code. (setup_marathon_job, bounces, etc)',
        'additional_source_envs': ['devc'],
    }),
    ('monitoring', {
        'color': PaastaColors.green,
        'help': 'Logs from Sensu checks for the service',
    }),
    ('marathon', {
        'color': PaastaColors.magenta,
        'help': 'Logs from Marathon for the service',
    }),
    ('chronos', {
        'color': PaastaColors.red,
        'help': 'Logs from Chronos for the service',
    }),
    ('app_output', {
        'color': compose(PaastaColors.yellow, PaastaColors.bold),
        'help': 'Stderr and stdout of the actual process spawned by Mesos. '
                'Convenience alias for both the stdout and stderr components',
    }),
    ('stdout', {
        'color': PaastaColors.yellow,
        'help': 'Stdout from the process spawned by Mesos.',
    }),
    ('stderr', {
        'color': PaastaColors.yellow,
        'help': 'Stderr from the process spawned by Mesos.',
    }),
    # I'm leaving these planned components here since they provide some hints
    # about where we want to go. See PAASTA-78.
    #
    # But I'm commenting them out so they don't delude users into believing we
    # can expose logs that we cannot actually expose. See PAASTA-927.
    #
    # ('app_request', {
    #     'color': PaastaColors.bold,
    #     'help': 'The request log for the service. Defaults to "service_NAME_requests"',
    #     'command': 'scribe_reader -e ENV -f service_example_happyhour_requests',
    # }),
    # ('app_errors', {
    #     'color': PaastaColors.red,
    #     'help': 'Application error log, defaults to "stream_service_NAME_errors"',
    #     'command': 'scribe_reader -e ENV -f stream_service_SERVICE_errors',
    # }),
    # ('lb_requests', {
    #     'color': PaastaColors.bold,
    #     'help': 'All requests from Smartstack haproxy',
    #     'command': 'NA - TODO: SRV-1130',
    # }),
    # ('lb_errors', {
    #     'color': PaastaColors.red,
    #     'help': 'Logs from Smartstack haproxy that have 400-500 error codes',
    #     'command': 'scribereader -e ENV -f stream_service_errors | grep SERVICE.instance',
    # }),
])


class NoSuchLogComponent(Exception):
    pass


def validate_log_component(component):
    if component in LOG_COMPONENTS.keys():
        return True
    else:
        raise NoSuchLogComponent


def get_git_url(service, soa_dir=DEFAULT_SOA_DIR):
    """Get the git url for a service. Assumes that the service's
    repo matches its name, and that it lives in services- i.e.
    if this is called with the string 'test', the returned
    url will be git@git.yelpcorp.com:services/test.git.

    :param service: The service name to get a URL for
    :returns: A git url to the service's repository"""
    general_config = service_configuration_lib.read_service_configuration(
        service,
        soa_dir=soa_dir,
    )
    default_location = 'git@git.yelpcorp.com:services/%s.git' % service
    return general_config.get('git_url', default_location)


class NoSuchLogLevel(Exception):
    pass


# The active log writer.
_log_writer = None
# The map of name -> LogWriter subclasses, used by configure_log.
_log_writer_classes = {}


def register_log_writer(name):
    """Returns a decorator that registers that bounce function at a given name
    so get_log_writer_classes can find it."""
    def outer(bounce_func):
        _log_writer_classes[name] = bounce_func
        return bounce_func
    return outer


def get_log_writer_class(name):
    return _log_writer_classes[name]


def list_log_writers():
    return _log_writer_classes.keys()


def configure_log():
    """We will log to the yocalhost binded scribe."""
    log_writer_config = load_system_paasta_config().get_log_writer()
    global _log_writer
    LogWriterClass = get_log_writer_class(log_writer_config['driver'])
    _log_writer = LogWriterClass(**log_writer_config.get('options', {}))


def _log(*args, **kwargs):
    if _log_writer is None:
        configure_log()
    return _log_writer.log(*args, **kwargs)


class LogWriter(object):
    def log(self, line, component, level=DEFAULT_LOGLEVEL, cluster=ANY_CLUSTER, instance=ANY_INSTANCE):
        raise NotImplementedError()


def _now():
    return datetime.datetime.utcnow().isoformat()


def remove_ansi_escape_sequences(line):
    """Removes ansi escape sequences from the given line."""
    return no_escape.sub('', line)


def format_log_line(level, cluster, service, instance, component, line, timestamp=None):
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """
    validate_log_component(component)
    if not timestamp:
        timestamp = _now()
    line = remove_ansi_escape_sequences(line)
    message = json.dumps({
        'timestamp': timestamp,
        'level': level,
        'cluster': cluster,
        'service': service,
        'instance': instance,
        'component': component,
        'message': line,
    }, sort_keys=True)
    return message


def get_log_name_for_service(service, prefix=None):
    if prefix:
        return 'stream_paasta_%s_%s' % (prefix, service)
    return 'stream_paasta_%s' % service


@register_log_writer('scribe')
class ScribeLogWriter(LogWriter):
    def __init__(self, scribe_host='169.254.255.254', scribe_port=1463, scribe_disable=False, **kwargs):
        self.clog = importlib.import_module('clog')
        self.clog.config.configure(scribe_host=scribe_host, scribe_port=scribe_port, scribe_disable=scribe_disable)

    def log(self, service, line, component, level=DEFAULT_LOGLEVEL, cluster=ANY_CLUSTER, instance=ANY_INSTANCE):
        """This expects someone (currently the paasta cli main()) to have already
        configured the log object. We'll just write things to it.
        """
        if level == 'event':
            print(line, file=sys.stdout)
        elif level == 'debug':
            print(line, file=sys.stderr)
        else:
            raise NoSuchLogLevel
        log_name = get_log_name_for_service(service)
        formatted_line = format_log_line(level, cluster, service, instance, component, line)
        self.clog.log_line(log_name, formatted_line)


@register_log_writer('null')
class NullLogWriter(LogWriter):
    """A LogWriter class that doesn't do anything. Primarily useful for integration tests where we don't care about
    logs."""

    def __init__(self, **kwargs):
        pass

    def log(self, service, line, component, level=DEFAULT_LOGLEVEL, cluster=ANY_CLUSTER, instance=ANY_INSTANCE):
        pass


@register_log_writer('file')
class FileLogWriter(LogWriter):
    def __init__(self, path_format, mode='a+', line_delimeter='\n', flock=False):
        self.path_format = path_format
        self.mode = mode
        self.flock = flock
        self.line_delimeter = line_delimeter

    @contextlib.contextmanager
    def maybe_flock(self, fd):
        if self.flock:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        else:
            yield

    def format_path(self, service, component, level, cluster, instance):
        return self.path_format.format(
            service=service,
            component=component,
            level=level,
            cluster=cluster,
            instance=instance,
        )

    def log(self, service, line, component, level=DEFAULT_LOGLEVEL, cluster=ANY_CLUSTER, instance=ANY_INSTANCE):
        path = self.format_path(service, component, level, cluster, instance)

        # We use io.FileIO here because it guarantees that write() is implemented with a single write syscall,
        # and on Linux, writes to O_APPEND files with a single write syscall are atomic.
        #
        # https://docs.python.org/2/library/io.html#io.FileIO
        # http://article.gmane.org/gmane.linux.kernel/43445

        to_write = "%s%s" % (format_log_line(level, cluster, service, instance, component, line), self.line_delimeter)

        with io.FileIO(path, mode=self.mode, closefd=True) as f:
            with self.maybe_flock(f):
                f.write(to_write)


def _timeout(process):
    """Helper function for _run. It terminates the process.
    Doesn't raise OSError, if we try to terminate a non-existing
    process as there can be a very small window between poll() and kill()
    """
    if process.poll() is None:
        try:
            # sending SIGKILL to the process
            process.kill()
        except OSError as e:
            # No such process error
            # The process could have been terminated meanwhile
            if e.errno != errno.ESRCH:
                raise


class PaastaNotConfiguredError(Exception):
    pass


class NoConfigurationForServiceError(Exception):
    pass


def get_readable_files_in_glob(glob, path):
    """
    Returns a sorted list of files that are readable in an input glob by recursively searching a path
    """
    globbed_files = []
    for root, dirs, files in os.walk(path):
        for f in files:
            fn = os.path.join(root, f)
            if os.path.isfile(fn) and os.access(fn, os.R_OK) and fnmatch(fn, glob):
                globbed_files.append(fn)
    return sorted(globbed_files)


def load_system_paasta_config(path=PATH_TO_SYSTEM_PAASTA_CONFIG_DIR):
    """
    Reads Paasta configs in specified directory in lexicographical order and deep merges
    the dictionaries (last file wins).
    """
    config = {}
    if not os.path.isdir(path):
        raise PaastaNotConfiguredError("Could not find system paasta configuration directory: %s" % path)

    if not os.access(path, os.R_OK):
        raise PaastaNotConfiguredError("Could not read from system paasta configuration directory: %s" % path)

    try:
        for config_file in get_readable_files_in_glob(glob="*.json", path=path):
            with open(config_file) as f:
                config = deep_merge_dictionaries(json.load(f), config)
    except IOError as e:
        raise PaastaNotConfiguredError("Could not load system paasta config file %s: %s" % (e.filename, e.strerror))
    return SystemPaastaConfig(config, path)


class SystemPaastaConfig(dict):

    def __init__(self, config, directory):
        self.directory = directory
        super(SystemPaastaConfig, self).__init__(config)

    def get_zk_hosts(self):
        """Get the zk_hosts defined in this hosts's cluster config file.
        Strips off the zk:// prefix, if it exists, for use with Kazoo.

        :returns: The zk_hosts specified in the paasta configuration
        """
        try:
            hosts = self['zookeeper']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find zookeeper connection string in configuration directory: %s'
                                           % self.directory)

        # how do python strings not have a method for doing this
        if hosts.startswith('zk://'):
            return hosts[len('zk://'):]
        return hosts

    def get_docker_registry(self):
        """Get the docker_registry defined in this host's cluster config file.

        :returns: The docker_registry specified in the paasta configuration
        """
        try:
            return self['docker_registry']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find docker registry in configuration directory: %s'
                                           % self.directory)

    def get_volumes(self):
        """Get the volumes defined in this host's volumes config file.

        :returns: The list of volumes specified in the paasta configuration
        """
        try:
            return self['volumes']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find volumes in configuration directory: %s' % self.directory)

    def get_cluster(self):
        """Get the cluster defined in this host's cluster config file.

        :returns: The name of the cluster defined in the paasta configuration
        """
        try:
            return self['cluster']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find cluster in configuration directory: %s' % self.directory)

    def get_dashboard_links(self):
        return self['dashboard_links']

    def get_api_endpoints(self):
        return self['api_endpoints']

    def get_fsm_template(self):
        fsm_path = os.path.dirname(sys.modules['paasta_tools.cli.fsm'].__file__)
        template_path = os.path.join(fsm_path, "template")
        return self.get('fsm_template', template_path)

    def get_log_writer(self):
        """Get the log_writer configuration out of global paasta config

        :returns: The log_writer dictionary.
        """
        try:
            return self['log_writer']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find log_writer in configuration directory: %s' % self.directory)

    def get_log_reader(self):
        """Get the log_reader configuration out of global paasta config

        :returns: the log_reader dictionary.
        """
        try:
            return self['log_reader']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find log_reader in configuration directory: %s' % self.directory)

    def get_sensu_host(self):
        """Get the host that we should send sensu events to.

        :returns: the sensu_host string, or localhost if not specified.
        """
        return self.get('sensu_host', 'localhost')

    def get_sensu_port(self):
        """Get the port that we should send sensu events to.

        :returns: the sensu_port value as an integer, or 3030 if not specified.
        """
        return int(self.get('sensu_port', 3030))

    def get_dockercfg_location(self):
        """Get the location of the dockerfile, as a URI.

        :returns: the URI specified, or file:///root/.dockercfg if not specified.
        """
        return self.get('dockercfg_location', DEFAULT_DOCKERCFG_LOCATION)

    def get_synapse_port(self):
        """Get the port that haproxy-synapse exposes its status on. Defaults to 3212.

        :returns: the haproxy-synapse status port."""
        return int(self.get('synapse_port', 3212))

    def get_default_synapse_host(self):
        """Get the default host we should interrogate for haproxy-synapse state.

        :returns: A hostname that is running haproxy-synapse."""
        return self.get('synapse_host', 'localhost')

    def get_synapse_haproxy_url_format(self):
        """Get a format string for the URL to query for haproxy-synapse state. This format string gets two keyword
        arguments, host and port. Defaults to "http://{host:s}:{port:d}/;csv;norefresh".

        :returns: A format string for constructing the URL of haproxy-synapse's status page."""
        return self.get('synapse_haproxy_url_format', DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT)

    def get_cluster_autoscaling_resources(self):
        return self.get('cluster_autoscaling_resources', {})

    def get_resource_pool_settings(self):
        return self.get('resource_pool_settings', {})

    def get_cluster_fqdn_format(self):
        """Get a format string that constructs a DNS name pointing at the paasta masters in a cluster. This format
        string gets one parameter: cluster. Defaults to 'paasta-{cluster:s}.yelp'.

        :returns: A format string for constructing the FQDN of the masters in a given cluster."""
        return self.get('cluster_fqdn_format', 'paasta-{cluster:s}.yelp')

    def get_chronos_config(self):
        """Get the chronos config

        :returns: The chronos config dictionary"""
        try:
            return self['chronos_config']
        except KeyError:
            return {}

    def get_marathon_config(self):
        """Get the marathon config

        :returns: The marathon config dictionary"""
        try:
            return self['marathon_config']
        except KeyError:
            return {}

    def get_paasta_native_config(self):
        return self.get('paasta_native', {})

    def get_mesos_cli_config(self):
        """Get the config for mesos-cli

        :returns: The mesos cli config
        """
        return self.get("mesos_config", {})


def _run(command, env=os.environ, timeout=None, log=False, stream=False, stdin=None, **kwargs):
    """Given a command, run it. Return a tuple of the return code and any
    output.

    :param timeout: If specified, the command will be terminated after timeout
        seconds.
    :param log: If True, the _log will be handled by _run. If set, it is mandatory
        to pass at least a :service: and a :component: parameter. Optionally you
        can pass :cluster:, :instance: and :loglevel: parameters for logging.
    We wanted to use plumbum instead of rolling our own thing with
    subprocess.Popen but were blocked by
    https://github.com/tomerfiliba/plumbum/issues/162 and our local BASH_FUNC
    magic.
    """
    output = []
    if log:
        service = kwargs['service']
        component = kwargs['component']
        cluster = kwargs.get('cluster', ANY_CLUSTER)
        instance = kwargs.get('instance', ANY_INSTANCE)
        loglevel = kwargs.get('loglevel', DEFAULT_LOGLEVEL)
    try:
        process = Popen(shlex.split(command), stdout=PIPE, stderr=STDOUT, stdin=stdin, env=env)
        process.name = command
        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, (process,))
            proctimer.start()
        for line in iter(process.stdout.readline, ''):
            # additional indentation is for the paasta status command only
            if stream:
                if ('paasta_serviceinit status' in command):
                    if 'instance: ' in line:
                        print('  ' + line.rstrip('\n'))
                    else:
                        print('    ' + line.rstrip('\n'))
                else:
                    print(line.rstrip('\n'))
            else:
                output.append(line.rstrip('\n'))

            if log:
                _log(
                    service=service,
                    line=line.rstrip('\n'),
                    component=component,
                    level=loglevel,
                    cluster=cluster,
                    instance=instance,
                )
        # when finished, get the exit code
        returncode = process.wait()
    except OSError as e:
        if log:
            _log(
                service=service,
                line=e.strerror.rstrip('\n'),
                component=component,
                level=loglevel,
                cluster=cluster,
                instance=instance,
            )
        output.append(e.strerror.rstrip('\n'))
        returncode = e.errno
    except (KeyboardInterrupt, SystemExit):
        # need to clean up the timing thread here
        if timeout:
            proctimer.cancel()
        raise
    else:
        # Stop the timer
        if timeout:
            proctimer.cancel()
    if returncode == -9:
        output.append("Command '%s' timed out (longer than %ss)" % (command, timeout))
    return returncode, '\n'.join(output)


def get_umask():
    """Get the current umask for this process. NOT THREAD SAFE."""
    old_umask = os.umask(0022)
    os.umask(old_umask)
    return old_umask


def get_user_agent():
    user_agent = "PaaSTA Tools %s" % paasta_tools.__version__
    if len(sys.argv) >= 1:
        return user_agent + " " + os.path.basename(sys.argv[0])
    else:
        return user_agent


@contextlib.contextmanager
def atomic_file_write(target_path):
    dirname = os.path.dirname(target_path)
    basename = os.path.basename(target_path)

    with tempfile.NamedTemporaryFile(
        dir=dirname,
        prefix=('.%s-' % basename),
        delete=False
    ) as f:
        temp_target_path = f.name
        yield f

    mode = 0666 & (~get_umask())
    os.chmod(temp_target_path, mode)
    os.rename(temp_target_path, target_path)


class InvalidJobNameError(Exception):
    pass


def compose_job_id(name, instance, git_hash=None, config_hash=None, spacer=SPACER):
    """Compose a job/app id by concatenating its name, instance, git hash, and config hash.

    :param name: The name of the service
    :param instance: The instance of the service
    :param git_hash: The git_hash portion of the job_id. If git_hash is set,
                     config_hash must also be set.
    :param config_hash: The config_hash portion of the job_id. If config_hash
                        is set, git_hash must also be set.
    :returns: <name><SPACER><instance> if no tag, or <name><SPACER><instance><SPACER><hashes>...
              if extra hash inputs are provided.

    """
    composed = '%s%s%s' % (name, spacer, instance)
    if git_hash and config_hash:
        composed = '%s%s%s%s%s' % (composed, spacer, git_hash, spacer, config_hash)
    elif git_hash or config_hash:
        raise InvalidJobNameError(
            'invalid job id because git_hash (%s) and config_hash (%s) must '
            'both be defined or neither can be defined' % (git_hash, config_hash))
    return composed


def decompose_job_id(job_id, spacer=SPACER):
    """Break a composed job id into its constituent (service name, instance,
    git hash, config hash) by splitting with ``spacer``.

    :param job_id: The composed id of the job/app
    :returns: A tuple (service name, instance, git hash, config hash) that
        comprise the job_id
    """
    decomposed = job_id.split(spacer)
    if len(decomposed) == 2:
        git_hash = None
        config_hash = None
    elif len(decomposed) == 4:
        git_hash = decomposed[2]
        config_hash = decomposed[3]
    else:
        raise InvalidJobNameError('invalid job id %s' % job_id)
    return (decomposed[0], decomposed[1], git_hash, config_hash)


def build_docker_image_name(upstream_job_name):
    """docker-paasta.yelpcorp.com:443 is the URL for the Registry where PaaSTA
    will look for your images.

    upstream_job_name is a sanitized-for-Jenkins (s,/,-,g) version of the
    service's path in git. E.g. For git.yelpcorp.com:services/foo the
    upstream_job_name is services-foo.
    """
    docker_registry_url = load_system_paasta_config().get_docker_registry()
    name = '%s/services-%s' % (docker_registry_url, upstream_job_name)
    return name


def build_docker_tag(upstream_job_name, upstream_git_commit):
    """Builds the DOCKER_TAG string

    upstream_job_name is a sanitized-for-Jenkins (s,/,-,g) version of the
    service's path in git. E.g. For git.yelpcorp.com:services/foo the
    upstream_job_name is services-foo.

    upstream_git_commit is the SHA that we're building. Usually this is the
    tip of origin/master.
    """
    tag = '%s:paasta-%s' % (
        build_docker_image_name(upstream_job_name),
        upstream_git_commit,
    )
    return tag


def check_docker_image(service, tag):
    """Checks whether the given image for :service: with :tag: exists.

    :raises: ValueError if more than one docker image with :tag: found.
    :returns: True if there is exactly one matching image found.
    """
    docker_client = get_docker_client()
    image_name = build_docker_image_name(service)
    docker_tag = build_docker_tag(service, tag)
    images = docker_client.images(name=image_name)
    result = [image for image in images if docker_tag in image['RepoTags']]
    if len(result) > 1:
        raise ValueError('More than one docker image found with tag %s\n%s' % docker_tag, result)
    return len(result) == 1


def datetime_from_utc_to_local(utc_datetime):
    return datetime_convert_timezone(utc_datetime, dateutil.tz.tzutc(), dateutil.tz.tzlocal())


def datetime_convert_timezone(datetime, from_zone, to_zone):
    datetime = datetime.replace(tzinfo=from_zone)
    converted_datetime = datetime.astimezone(to_zone)
    converted_datetime = converted_datetime.replace(tzinfo=None)
    return converted_datetime


def get_username():
    """Returns the current username in a portable way. Will use the SUDO_USER
    environment variable if present.
    http://stackoverflow.com/a/2899055
    """
    return os.environ.get('SUDO_USER', pwd.getpwuid(os.getuid())[0])


def get_default_cluster_for_service(service, soa_dir=DEFAULT_SOA_DIR):
    cluster = None
    try:
        cluster = load_system_paasta_config().get_cluster()
    except PaastaNotConfiguredError:
        clusters_deployed_to = list_clusters(service, soa_dir=soa_dir)
        if len(clusters_deployed_to) > 0:
            cluster = clusters_deployed_to[0]
        else:
            raise NoConfigurationForServiceError("No cluster configuration found for service %s" % service)
    return cluster


def get_soa_cluster_deploy_files(service=None, soa_dir=DEFAULT_SOA_DIR, instance_type=None):
    if service is None:
        service = '*'
    service_path = os.path.join(soa_dir, service)

    if instance_type in INSTANCE_TYPES:
        instance_types = instance_type
    else:
        instance_types = '|'.join(INSTANCE_TYPES)

    search_re = r'/.*/(' + instance_types + r')-([0-9a-z-_]*)\.yaml$'

    for yaml_file in glob.glob('%s/*.yaml' % service_path):
        cluster_re_match = re.search(search_re, yaml_file)
        if cluster_re_match is not None:
            cluster = cluster_re_match.group(2)
            yield (cluster, yaml_file)


def list_clusters(service=None, soa_dir=DEFAULT_SOA_DIR, instance_type=None):
    """Returns a sorted list of clusters a service is configured to deploy to,
    or all clusters if ``service`` is not specified.

    Includes every cluster that has a ``marathon-*.yaml`` or ``chronos-*.yaml`` file associated with it.

    :param service: The service name. If unspecified, clusters running any service will be included.
    :returns: A sorted list of cluster names
    """
    clusters = set()
    for cluster, _ in get_soa_cluster_deploy_files(
        service=service,
        soa_dir=soa_dir,
        instance_type=instance_type,
    ):
        clusters.add(cluster)
    return sorted(clusters)


def list_all_instances_for_service(service, clusters=None, instance_type=None, soa_dir=DEFAULT_SOA_DIR):
    instances = set()
    if not clusters:
        clusters = list_clusters(service, soa_dir=soa_dir)
    for cluster in clusters:
        for service_instance in get_service_instance_list(service, cluster, instance_type, soa_dir=soa_dir):
            instances.add(service_instance[1])
    return instances


def get_service_instance_list(service, cluster=None, instance_type=None, soa_dir=DEFAULT_SOA_DIR):
    """Enumerate the instances defined for a service as a list of tuples.

    :param service: The service name
    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'marathon', 'chronos', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, instance) for each instance defined for the service name
    """
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    if instance_type in INSTANCE_TYPES:
        instance_types = [instance_type]
    else:
        instance_types = INSTANCE_TYPES

    instance_list = []
    for srv_instance_type in instance_types:
        conf_file = "%s-%s" % (srv_instance_type, cluster)
        log.info("Enumerating all instances for config file: %s/*/%s.yaml" % (soa_dir, conf_file))
        instances = service_configuration_lib.read_extra_service_information(
            service,
            conf_file,
            soa_dir=soa_dir
        )
        for instance in instances:
            instance_list.append((service, instance))

    log.debug("Enumerated the following instances: %s", instance_list)
    return instance_list


def get_services_for_cluster(cluster=None, instance_type=None, soa_dir=DEFAULT_SOA_DIR):
    """Retrieve all services and instances defined to run in a cluster.

    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'marathon', 'chronos', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, instance)
    """
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    rootdir = os.path.abspath(soa_dir)
    log.info("Retrieving all service instance names from %s for cluster %s", rootdir, cluster)
    instance_list = []
    for srv_dir in os.listdir(rootdir):
        instance_list.extend(get_service_instance_list(srv_dir, cluster, instance_type, soa_dir))
    return instance_list


def parse_yaml_file(yaml_file):
    return yaml.load(open(yaml_file))


def get_docker_host():
    return os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')


def get_docker_client():
    client_opts = kwargs_from_env(assert_hostname=False)
    if 'base_url' in client_opts:
        return Client(**client_opts)
    else:
        return Client(base_url=get_docker_host(), **client_opts)


def get_running_mesos_docker_containers():
    client = get_docker_client()
    running_containers = client.containers()
    return [container for container in running_containers if "mesos-" in container["Names"][0]]


class TimeoutError(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


class Timeout:
    # From http://stackoverflow.com/questions/2281850/timeout-function-if-it-takes-too-long-to-finish

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def print_with_indent(line, indent=2):
    """Print a line with a given indent level"""
    print(" " * indent + line)


class NoDeploymentsAvailable(Exception):
    pass


def load_deployments_json(service, soa_dir=DEFAULT_SOA_DIR):
    deployment_file = os.path.join(soa_dir, service, 'deployments.json')
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            return DeploymentsJson(json.load(f)['v1'])
    else:
        raise NoDeploymentsAvailable


class DeploymentsJson(dict):

    def get_branch_dict(self, service, branch):
        full_branch = '%s:%s' % (service, branch)
        return self.get(full_branch, {})


def get_paasta_branch_from_deploy_group(identifier):
    return 'paasta-%s' % (identifier)


def get_paasta_branch(cluster, instance):
    return get_paasta_branch_from_deploy_group('%s.%s' % (cluster, instance))


def parse_timestamp(tstamp):
    return datetime.datetime.strptime(tstamp, '%Y%m%dT%H%M%S')


def format_timestamp(dt=None):
    if dt is None:
        dt = datetime.datetime.utcnow()
    return dt.strftime('%Y%m%dT%H%M%S')


def get_paasta_tag_from_deploy_group(identifier, desired_state):
    timestamp = format_timestamp(datetime.datetime.utcnow())
    return 'paasta-%s-%s-%s' % (identifier, timestamp, desired_state)


def get_paasta_tag(cluster, instance, desired_state):
    timestamp = format_timestamp(datetime.datetime.utcnow())
    return 'paasta-%s.%s-%s-%s' % (cluster, instance, timestamp, desired_state)


def format_tag(tag):
    return 'refs/tags/%s' % tag


class NoDockerImageError(Exception):
    pass


def get_docker_url(registry_uri, docker_image):
    """Compose the docker url.
    :param registry_uri: The URI of the docker registry
    :param docker_image: The docker image name, with tag if desired
    :returns: '<registry_uri>/<docker_image>'
    """
    if not docker_image:
        raise NoDockerImageError('Docker url not available because there is no docker_image')
    docker_url = '%s/%s' % (registry_uri, docker_image)
    return docker_url


def get_config_hash(config, force_bounce=None):
    """Create an MD5 hash of the configuration dictionary to be sent to
    Marathon. Or anything really, so long as str(config) works. Returns
    the first 8 characters so things are not really long.

    :param config: The configuration to hash
    :param force_bounce: a timestamp (in the form of a string) that is appended before hashing
                         that can be used to force a hash change
    :returns: A MD5 hash of str(config)
    """
    hasher = hashlib.md5()
    hasher.update(json.dumps(config, sort_keys=True) + (force_bounce or ''))
    return "config%s" % hasher.hexdigest()[:8]


def get_code_sha_from_dockerurl(docker_url):
    """We encode the sha of the code that built a docker image *in* the docker
    url. This function takes that url as input and outputs the partial sha
    """
    parts = docker_url.split('-')
    return "git%s" % parts[-1][:8]


def is_under_replicated(num_available, expected_count, crit_threshold):
    """Calculates if something is under replicated

    :param num_available: How many things are up
    :param expected_count: How many things you think should be up
    :param crit_threshold: Int from 0-100
    :returns: Tuple of (bool, ratio)
    """
    if expected_count == 0:
        ratio = 100
    else:
        ratio = (num_available / float(expected_count)) * 100

    if ratio < int(crit_threshold):
        return (True, ratio)
    else:
        return (False, ratio)


def deploy_blacklist_to_constraints(deploy_blacklist):
    """Converts a blacklist of locations into marathon appropriate constraints
    https://mesosphere.github.io/marathon/docs/constraints.html#unlike-operator

    :param blacklist: List of lists of locations to blacklist
    :returns: List of lists of constraints
    """
    constraints = []
    for blacklisted_location in deploy_blacklist:
        constraints.append([blacklisted_location[0], "UNLIKE", blacklisted_location[1]])

    return constraints


def deploy_whitelist_to_constraints(deploy_whitelist):
    """Converts a whitelist of locations into marathon appropriate constraints
    https://mesosphere.github.io/marathon/docs/constraints.html#like-operator

    :param deploy_whitelist: List of lists of locations to whitelist
    :returns: List of lists of constraints
    """
    if len(deploy_whitelist) > 0:
        (region_type, regions) = deploy_whitelist
        regionstr = '|'.join(regions)

        return [[region_type, 'LIKE', regionstr]]
    return []


def terminal_len(text):
    """Return the number of characters that text will take up on a terminal. """
    return len(remove_ansi_escape_sequences(text))


def format_table(rows, min_spacing=2):
    """Formats a table for use on the command line.

    :param rows: List of rows, each of which can either be a tuple of strings containing the row's values, or a string
                 to be inserted verbatim. Each row (except literal strings) should be the same number of elements as
                 all the others.
    :returns: A string containing rows formatted as a table.
    """

    list_rows = [r for r in rows if not isinstance(r, basestring)]

    # If all of the rows are strings, we have nothing to do, so short-circuit.
    if not list_rows:
        return rows

    widths = []
    for i in xrange(len(list_rows[0])):
        widths.append(max(terminal_len(r[i]) for r in list_rows))

    expanded_rows = []
    for row in rows:
        if row not in list_rows:
            expanded_rows.append([row])
        else:
            expanded_row = []
            for i, cell in enumerate(row):
                if i == len(row) - 1:
                    padding = ''
                else:
                    padding = ' ' * (widths[i] - terminal_len(cell))
                expanded_row.append(cell + padding)
            expanded_rows.append(expanded_row)

    return [(' ' * min_spacing).join(r) for r in expanded_rows]


def deep_merge_dictionaries(overrides, defaults):
    """
    Merges two dictionaries.
    """
    result = copy.deepcopy(defaults)
    stack = [(overrides, result)]
    while stack:
        source_dict, result_dict = stack.pop()
        for key, value in source_dict.items():
            child = result_dict.setdefault(key, {})
            if isinstance(value, dict) and isinstance(child, dict):
                stack.append((value, child))
            else:
                result_dict[key] = value
    return result


class ZookeeperPool(object):
    """
    A context manager that shares the same KazooClient with its children. The first nested contest manager
    creates and deletes the client and shares it with any of its children. This allows to place a context
    manager over a large number of zookeeper calls without opening and closing a connection each time.
    GIL makes this 'safe'.
    """
    counter = 0
    zk = None

    @classmethod
    def __enter__(cls):
        if cls.zk is None:
            cls.zk = KazooClient(hosts=load_system_paasta_config().get_zk_hosts(), read_only=True)
            cls.zk.start()
        cls.counter = cls.counter + 1
        return cls.zk

    @classmethod
    def __exit__(cls, *args, **kwargs):
        cls.counter = cls.counter - 1
        if cls.counter == 0:
            cls.zk.stop()
            cls.zk.close()
            cls.zk = None


def calculate_tail_lines(verbose_level):
    if verbose_level == 1:
        return 0
    else:
        return 10 ** (verbose_level - 1)


def is_deploy_step(step):
    """
    Returns true if the given step deploys to an instancename
    Returns false if the step is a predefined step-type, e.g. itest or command-*
    """
    return not ((step in DEPLOY_PIPELINE_NON_DEPLOY_STEPS) or (step.startswith('command-')))


def use_requests_cache(cache_name, backend='memory', **kwargs):
    def wrap(fun):
        def fun_with_cache(*args, **kwargs):
            requests_cache.install_cache(cache_name, backend=backend, **kwargs)
            result = fun(*args, **kwargs)
            requests_cache.uninstall_cache()
            return result
        return fun_with_cache
    return wrap


def long_job_id_to_short_job_id(long_job_id):
    service, instance, _, __ = decompose_job_id(long_job_id)
    return compose_job_id(service, instance)
