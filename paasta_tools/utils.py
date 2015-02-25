from __future__ import print_function
import contextlib
import datetime
import errno
import json
import shlex
import sys
import tempfile
import threading
import os

from subprocess import Popen
from subprocess import PIPE
from subprocess import STDOUT

import clog


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


class PaastaColors:
    """Collection of static variables and methods to assist in coloring text."""
    # ANSI colour codes
    BLUE = '\033[34m'
    BOLD = '\033[1m'
    CYAN = '\033[36m'
    DEFAULT = '\033[0m'
    GREEN = '\033[32m'
    GREY = '\033[1m\033[30m'
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
        """Return text that can be printed cyan.

        :param text: a string
        :return: text colour coded with ANSI blue
        """
        return PaastaColors.color_text(PaastaColors.BLUE, text)

    @staticmethod
    def green(text):
        """Return text that can be printed cyan.

        :param text: a string
        :return: text colour coded with ANSI green"""
        return PaastaColors.color_text(PaastaColors.GREEN, text)

    @staticmethod
    def red(text):
        """Return text that can be printed cyan.

        :param text: a string
        :return: text colour coded with ANSI red"""
        return PaastaColors.color_text(PaastaColors.RED, text)

    @staticmethod
    def color_text(color, text):
        """Return text that can be printed color.

        :param color: ANSI colour code
        :param text: a string
        :return: a string with ANSI colour encoding"""
        return color + text + PaastaColors.DEFAULT

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
    'app_output': {
        'color': PaastaColors.bold,
        'help': 'Stderr and stdout of the actual process spawned by Mesos',
        'command': 'NA - PAASTA-78',
    },
    'app_request': {
        'color': PaastaColors.bold,
        'help': 'The request log for the service. Defaults to "service_NAME_requests"',
        'command': 'scribe_reader -e ENV -f service_example_happyhour_requests',
    },
    'app_errors': {
        'color': PaastaColors.red,
        'help': 'Application error log, defaults to "stream_service_NAME_errors"',
        'command': 'scribe_reader -e ENV -f stream_service_SERVICE_errors',
    },
    'lb_requests': {
        'color': PaastaColors.bold,
        'help': 'All requests from Smartstack haproxy',
        'command': 'NA - TODO: SRV-1130',
    },
    'lb_errors': {
        'color': PaastaColors.red,
        'help': 'Logs from Smartstack haproxy that have 400-500 error codes',
        'command': 'scribereader -e ENV -f stream_service_errors | grep SERVICE.instance',
    },
    'monitoring': {
        'color': PaastaColors.green,
        'help': 'Logs from Sensu checks for the service',
        'command': 'NA - TODO log mesos healthcheck and sensu stuff.',
    },
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
    clog.config.configure(scribe_host='169.254.255.254', scribe_port=1463)


def _now():
    return datetime.datetime.utcnow().isoformat()


def format_log_line(level, cluster, instance, component, line):
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """
    validate_log_component(component)
    now = _now()
    message = json.dumps({
        'timestamp': now,
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
    line = format_log_line(level, cluster, instance, component, line)
    if level == 'event':
        print(line, file=sys.stdout)
    elif level == 'debug':
        print(line, file=sys.stderr)
    else:
        raise NoSuchLogLevel
    line = str(line)
    log_name = get_log_name_for_service(service_name)
    clog.log_line(log_name, line)


def _timeout(process):
    """Helper function for _run. It terminates the process.
    Doesn't raise OSError, if we try to terminate a non-existing
    process as there can be a very small window between poll() and kill()
    """
    if process.poll() is None:
        try:
            # sending SIGKILL to the process
            process.kill()
            print('ERROR: Timeout running command: %s' % process.name)
        except OSError as e:
            # No such process error
            # The process could have been terminated meanwhile
            if e.errno != errno.ESRCH:
                raise


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
                    line=line,
                    component=component,
                    level=loglevel,
                    cluster=cluster,
                    instance=instance,
                )
            output.append(line)
        # when finished, get the exit code
        returncode = process.wait()
    except OSError as e:
        output.append(e.strerror)
        returncode = e.errno
    # Stop the timer
    if timeout:
        proctimer.cancel()
    return returncode, ''.join(output)


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
