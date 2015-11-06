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

from __future__ import print_function
import contextlib
import datetime
import errno
import glob
import hashlib
import logging
import os
import pwd
import re
import shlex
import signal
import sys
import tempfile
import threading
from functools import wraps
from subprocess import PIPE
from subprocess import Popen
from subprocess import STDOUT

import clog
import dateutil.tz
import docker
import json
import yaml

import service_configuration_lib

# DO NOT CHANGE SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name and instance
SPACER = '.'
INFRA_ZK_PATH = '/nail/etc/zookeeper_discovery/infrastructure/'
PATH_TO_SYSTEM_PAASTA_CONFIG_DIR = '/etc/paasta/'
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
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

log = logging.getLogger('__main__')


class InvalidInstanceConfig(Exception):
    pass


class InstanceConfig(dict):

    def __init__(self, config_dict, branch_dict):
        self.config_dict = config_dict
        self.branch_dict = branch_dict

    def get_mem(self):
        """Gets the memory required from the service's configuration.

        Defaults to 1024 (1G) if no value specified in the config.

        :returns: The amount of memory specified by the config, 1024 if not specified"""
        mem = self.config_dict.get('mem')
        return mem if mem else 1024

    def get_cpus(self):
        """Gets the number of cpus required from the service's configuration.

        Defaults to .25 (1/4 of a cpu) if no value specified in the config.

        :returns: The number of cpus specified in the config, .25 if not specified"""
        cpus = self.config_dict.get('cpus')
        return cpus if cpus else .25

    def get_cmd(self):
        """Get the docker cmd specified in the service's configuration.

        Defaults to null if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get('cmd', None)

    def get_env(self):
        """A dictionary of key/value pairs that represent environment variables
        to be injected to the container environment"""
        return self.config_dict.get('env', {})

    def get_unformatted_env(self):
        """A dictionary of key/value pairs that represent environment variables
        to be injected to the container environment.

        This method always returns the raw ``env`` dictionary, and is not formatted in
        any framework-specific way."""
        return self.config_dict.get('env', {})

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

    def get_desired_state_human(self):
        desired_state = self.get_desired_state()
        if desired_state == 'start':
            return PaastaColors.bold('Started')
        elif desired_state == 'stop':
            return PaastaColors.red('Stopped')
        else:
            return PaastaColors.red('Unknown (desired_state: %s)' % desired_state)

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
            if not isinstance(cpus, float) and not isinstance(cpus, int):
                return False, 'The specified cpus value "%s" is not a valid float.' % cpus
        return True, ''

    def check_mem(self):
        mem = self.get_mem()
        if mem is not None:
            if not isinstance(mem, float) and not isinstance(mem, int):
                return False, 'The specified mem value "%s" is not a valid float.' % mem
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


def validate_service_instance(service, instance, cluster, soa_dir):
    marathon_services = get_services_for_cluster(cluster=cluster, instance_type='marathon', soa_dir=soa_dir)
    chronos_services = get_services_for_cluster(cluster=cluster, instance_type='chronos', soa_dir=soa_dir)
    if (service, instance) in marathon_services:
        return 'marathon'
    elif (service, instance) in chronos_services:
        return 'chronos'
    else:
        print ("Error: %s doesn't look like it has been deployed to this cluster! (%s)"
               % (compose_job_id(service, instance), cluster))
        sys.exit(3)


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


LOG_COMPONENTS = {
    'build': {
        'color': PaastaColors.blue,
        'help': 'Jenkins build jobs output, like the itest, promotion, security checks, etc.',
        'command': 'NA - TODO: tee jenkins build steps into scribe PAASTA-201',
        'source_env': 'devc',
    },
    'deploy': {
        'color': PaastaColors.cyan,
        'help': 'Output from the paasta deploy code. (setup_marathon_job, bounces, etc)',
        'command': 'NA - TODO: tee deploy logs into scribe PAASTA-201',
    },
    'monitoring': {
        'color': PaastaColors.green,
        'help': 'Logs from Sensu checks for the service',
        'command': 'NA - TODO log mesos healthcheck and sensu stuff.',
    },
    'marathon': {
        'color': PaastaColors.magenta,
        'help': 'Logs from Marathon for the service',
        'command': 'NA - TODO log marathon stuff.',
    },
    'chronos': {
        'color': PaastaColors.magenta,
        'help': 'Logs from Chronos for the service',
        'command': 'NA - TODO log chronos stuff.',
    },
    # I'm leaving these planned components here since they provide some hints
    # about where we want to go. See PAASTA-78.
    #
    # But I'm commenting them out so they don't delude users into believing we
    # can expose logs that we cannot actually expose. See PAASTA-927.
    #
    # 'app_output': {
    #     'color': PaastaColors.bold,
    #     'help': 'Stderr and stdout of the actual process spawned by Mesos',
    #     'command': 'NA - PAASTA-78',
    # },
    # 'app_request': {
    #     'color': PaastaColors.bold,
    #     'help': 'The request log for the service. Defaults to "service_NAME_requests"',
    #     'command': 'scribe_reader -e ENV -f service_example_happyhour_requests',
    # },
    # 'app_errors': {
    #     'color': PaastaColors.red,
    #     'help': 'Application error log, defaults to "stream_service_NAME_errors"',
    #     'command': 'scribe_reader -e ENV -f stream_service_SERVICE_errors',
    # },
    # 'lb_requests': {
    #     'color': PaastaColors.bold,
    #     'help': 'All requests from Smartstack haproxy',
    #     'command': 'NA - TODO: SRV-1130',
    # },
    # 'lb_errors': {
    #     'color': PaastaColors.red,
    #     'help': 'Logs from Smartstack haproxy that have 400-500 error codes',
    #     'command': 'scribereader -e ENV -f stream_service_errors | grep SERVICE.instance',
    # },
}


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


def configure_log():
    """We will log to the yocalhost binded scribe."""
    clog.config.configure(scribe_host='169.254.255.254', scribe_port=1463, scribe_disable=False)


def _now():
    return datetime.datetime.utcnow().isoformat()


def remove_ansi_escape_sequences(line):
    """Removes ansi escape sequences from the given line."""
    return no_escape.sub('', line)


def format_log_line(level, cluster, instance, component, line, timestamp=None):
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
        'instance': instance,
        'component': component,
        'message': line,
    }, sort_keys=True)
    return message


def get_log_name_for_service(service):
    return 'stream_paasta_%s' % service


def _log(service, line, component, level=DEFAULT_LOGLEVEL, cluster=ANY_CLUSTER, instance=ANY_INSTANCE):
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
    formatted_line = format_log_line(level, cluster, instance, component, line)
    clog.log_line(log_name, formatted_line)


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


def get_readable_files_in_glob(input_glob):
    """
    Returns lexicographically-sorted list of files that are readable in an input glob
    """
    files = []
    for f in sorted(glob.glob(input_glob)):
        if os.path.isfile(f) and os.access(f, os.R_OK):
            files.append(f)
    return files


def load_system_paasta_config(path=PATH_TO_SYSTEM_PAASTA_CONFIG_DIR):
    """
    Reads Paasta configs in specified directory in lexicographical order and merges duplicated keys (last file wins)
    """
    config = {}
    if not os.path.isdir(path):
        raise PaastaNotConfiguredError("Could not find system paasta configuration directory: %s" % path)

    if not os.access(path, os.R_OK):
        raise PaastaNotConfiguredError("Could not read from system paasta configuration directory: %s" % path)

    try:
        for config_file in get_readable_files_in_glob("%s/*.json" % path):
            with open(os.path.join(path, config_file)) as f:
                config.update(json.load(f))
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

    def get_scribe_map(self):
        """Get the scribe_map out of the paasta config

        :returns: The scribe_map dictionary
        """
        try:
            return self['scribe_map']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find scribe_map in configuration directory: %s' % self.directory)


def _run(command, env=os.environ, timeout=None, log=False, **kwargs):
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
        process = Popen(shlex.split(command), stdout=PIPE, stderr=STDOUT, env=env)
        process.name = command
        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, (process,))
            proctimer.start()
        for line in iter(process.stdout.readline, ''):
            if log:
                _log(
                    service=service,
                    line=line.rstrip('\n'),
                    component=component,
                    level=loglevel,
                    cluster=cluster,
                    instance=instance,
                )
            output.append(line.rstrip('\n'))
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
    name = 'docker-paasta.yelpcorp.com:443/services-%s' % upstream_job_name
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
    docker_client = docker.Client(timeout=60)
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
    """Returns the current username in a portable way
    http://stackoverflow.com/a/2899055
    """
    return pwd.getpwuid(os.getuid())[0]


def get_default_cluster_for_service(service):
    cluster = None
    try:
        cluster = load_system_paasta_config().get_cluster()
    except PaastaNotConfiguredError:
        clusters_deployed_to = list_clusters(service)
        if len(clusters_deployed_to) > 0:
            cluster = clusters_deployed_to[0]
        else:
            raise NoConfigurationForServiceError("No cluster configuration found for service %s" % service)
    return cluster


def list_clusters(service=None, soa_dir=DEFAULT_SOA_DIR):
    """Returns a sorted list of clusters a service is configured to deploy to,
    or all clusters if ``service`` is not specified.

    Includes every cluster that has a ``marathon-*.yaml`` or ``chronos-*.yaml`` file associated with it.

    :param service: The service name. If unspecified, clusters running any service will be included.
    :returns: A sorted list of cluster names
    """
    clusters = set()
    if service is None:
        service = '*'
    srv_path = os.path.join(soa_dir, service)

    for yaml_file in glob.glob('%s/*.yaml' % srv_path):
        cluster_re_match = re.search('/.*/(marathon|chronos)-([0-9a-z-]*).yaml$', yaml_file)
        if cluster_re_match is not None:
            clusters.add(cluster_re_match.group(2))
    return sorted(clusters)


def list_all_instances_for_service(service, instance_type=None):
    instances = set()
    for cluster in list_clusters(service):
        for service_instance in get_service_instance_list(service, cluster, instance_type):
            instances.add(service_instance[1])
    return instances


def get_service_instance_list(name, cluster=None, instance_type=None, soa_dir=DEFAULT_SOA_DIR):
    """Enumerate the instances defined for a service as a list of tuples.

    :param name: The service name
    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'marathon', 'chronos', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, instance) for each instance defined for the service name
    """
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    if instance_type == 'marathon' or instance_type == 'chronos':
        instance_types = [instance_type]
    else:
        instance_types = ['marathon', 'chronos']

    instance_list = []
    for srv_instance_type in instance_types:
        conf_file = "%s-%s" % (srv_instance_type, cluster)
        log.info("Enumerating all instances for config file: %s/*/%s.yaml" % (soa_dir, conf_file))
        instances = service_configuration_lib.read_extra_service_information(
            name,
            conf_file,
            soa_dir=soa_dir
        )
        for instance in instances:
            instance_list.append((name, instance))

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


def get_default_branch(cluster, instance):
    return 'paasta-%s.%s' % (cluster, instance)


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
    hasher.update(str(config) + (force_bounce or ''))
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

    if ratio < crit_threshold:
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
