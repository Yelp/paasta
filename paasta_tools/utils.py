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


INFRA_ZK_PATH = '/nail/etc/zookeeper_discovery/infrastructure/'
PATH_TO_SYSTEM_PAASTA_CONFIG_DIR = '/etc/paasta/'
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


class InstanceConfig(dict):

    def __init__(self, config_dict, branch_dict):
        self.config_dict = config_dict
        self.branch_dict = branch_dict

    def get_mem(self):
        """Gets the memory required from the service's configuration.

        Defaults to 1000 (1G) if no value specified in the config.

        :returns: The amount of memory specified by the config, 1000 if not specified"""
        mem = self.config_dict.get('mem')
        return mem if mem else 1000

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

    def get_monitoring(self):
        """Get monitoring overrides defined for the given instance"""
        return self.config_dict.get('monitoring', {})

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
        'source_env': 'env1',
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


def get_git_url(service):
    """Get the git url for a service. Assumes that the service's
    repo matches its name, and that it lives in services- i.e.
    if this is called with the string 'test', the returned
    url will be git@git.yelpcorp.com:services/test.git.

    :param service: The service name to get a URL for
    :returns: A git url to the service's repository"""
    return 'git@git.yelpcorp.com:services/%s.git' % service


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


def get_log_name_for_service(service_name):
    return 'stream_paasta_%s' % service_name


def _log(service_name, line, component, level=DEFAULT_LOGLEVEL, cluster=ANY_CLUSTER, instance=ANY_INSTANCE):
    """This expects someone (currently the paasta cli main()) to have already
    configured the log object. We'll just write things to it.
    """
    if level == 'event':
        print(line, file=sys.stdout)
    elif level == 'debug':
        print(line, file=sys.stderr)
    else:
        raise NoSuchLogLevel
    log_name = get_log_name_for_service(service_name)
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


class PaastaNotConfigured(Exception):
    pass


class NoMarathonClusterFoundException(Exception):
    pass


def get_files_in_dir(directory):
    """
    Returns lexically-sorted list of files that are readable in a given directory
    """
    files = []
    for f in sorted(os.listdir(directory)):
        path = os.path.join(directory, f)
        if os.path.isfile(path) and os.access(path, os.R_OK):
            files.append(path)
    return files


def load_system_paasta_config(path=PATH_TO_SYSTEM_PAASTA_CONFIG_DIR):
    """
    Reads Paasta configs in specified directory in lexographical order and merges duplicated keys (last file wins)
    """
    config = {}
    if not os.path.isdir(path):
        raise PaastaNotConfigured("Could not find system paasta configuration directory: %s" % path)

    if not os.access(path, os.R_OK):
        raise PaastaNotConfigured("Could not read from system paasta configuration directory: %s" % path)

    try:
        for config_file in get_files_in_dir(path):
            with open(os.path.join(path, config_file)) as f:
                config.update(json.load(f))
    except IOError as e:
        raise PaastaNotConfigured("Could not load system paasta config file %s: %s" % (e.filename, e.strerror))
    return SystemPaastaConfig(config, path)


class SystemPaastaConfig(dict):

    log = logging.getLogger('__main__')

    def __init__(self, config, directory):
        self.directory = directory
        super(SystemPaastaConfig, self).__init__(config)

    def get_zk_hosts(self):
        """Get the zk_hosts defined in this hosts's marathon config file.
        Strips off the zk:// prefix, if it exists, for use with Kazoo.

        :returns: The zk_hosts specified in the marathon configuration"""
        try:
            hosts = self['zookeeper']
        except KeyError:
            raise PaastaNotConfigured(
                'Could not find zookeeper connection string in configuration directory: %s' % self.directory)

        # how do python strings not have a method for doing this
        if hosts.startswith('zk://'):
            return hosts[len('zk://'):]
        return hosts

    def get_docker_registry(self):
        """Get the docker_registry defined in this host's paasta config file.

        :returns: The docker_registry specified in the marathon configuration"""
        try:
            return self['docker_registry']
        except KeyError:
            raise PaastaNotConfigured('Could not find docker registry in configuration directory: %s' % self.directory)

    def get_volumes(self):
        """Get the volumes defined in this host's volumes config file.

        :returns: list of volumes"""
        try:
            return self['volumes']
        except KeyError:
            raise PaastaNotConfigured('Could not find volumes in configuration directory: %s' % self.directory)

    def get_cluster(self):
        """Get the cluster defined in this host's paasta config file.

        :returns: The name of the cluster defined in the marathon configuration"""
        try:
            return self['cluster']
        except KeyError:
            raise NoMarathonClusterFoundException(
                'Could not find cluster in configuration directory: %s' % self.directory)


def _run(command, env=os.environ, timeout=None, log=False, **kwargs):
    """Given a command, run it. Return a tuple of the return code and any
    output.

    :param timeout: If specified, the command will be terminated after timeout
        seconds.
    :param log: If True, the _log will be handled by _run. If set, it is mandatory
        to pass at least a :service_name: and a :component: parameter. Optionally you
        can pass :cluster:, :instance: and :loglevel: parameters for logging.
    We wanted to use plumbum instead of rolling our own thing with
    subprocess.Popen but were blocked by
    https://github.com/tomerfiliba/plumbum/issues/162 and our local BASH_FUNC
    magic.
    """
    output = []
    if log:
        service_name = kwargs['service_name']
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
                    service_name=service_name,
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
                service_name=service_name,
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


def check_docker_image(service_name, tag):
    """Checks whether the given image for :service_name: with :tag: exists.
    Returns True if there is exactly one matching image found.
    Raises ValueError if more than one docker image with :tag: found.
    """
    docker_client = docker.Client(timeout=60)
    image_name = build_docker_image_name(service_name)
    docker_tag = build_docker_tag(service_name, tag)
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


def list_all_clusters(soadir=service_configuration_lib.DEFAULT_SOA_DIR):
    """Returns a set of all clusters. Includes every cluster that has
    a marathon or chronos file associated with it.
    """
    clusters = set()
    for yaml_file in glob.glob('%s/*/*.yaml' % soadir):
        cluster_re_match = re.search('/.*/(marathon|chronos)-([0-9a-z-]*).yaml$', yaml_file)
        if cluster_re_match is not None:
            clusters.add(cluster_re_match.group(2))
    return clusters


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
    """ Print a line with a given indent level """
    print(" " * indent + line)


class NoDeploymentsAvailable(Exception):
    pass


def load_deployments_json(service_name, soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    deployment_file = os.path.join(soa_dir, service_name, 'deployments.json')
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            return DeploymentsJson(json.load(f)['v1'])
    else:
        raise NoDeploymentsAvailable


class DeploymentsJson(dict):

    def get_branch_dict(self, service_name, branch):
        full_branch = '%s:%s' % (service_name, branch)
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
    :returns: A MD5 hash of str(config)"""
    hasher = hashlib.md5()
    hasher.update(str(config) + (force_bounce or ''))
    return "config%s" % hasher.hexdigest()[:8]


def get_code_sha_from_dockerurl(docker_url):
    """We encode the sha of the code that built a docker image *in* the docker
    url. This function takes that url as input and outputs the partial sha"""
    parts = docker_url.split('-')
    return "git%s" % parts[-1][:8]
