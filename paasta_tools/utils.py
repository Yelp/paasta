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


def _log(service_name, line, component, level='event', cluster='N/A', instance='N/A'):
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
            print 'ERROR: Timeout running command: %s', process.name
        except OSError as e:
            # No such process error
            # The process could have been terminated meanwhile
            if e.errno != errno.ESRCH:
                raise


def _run(command, env=os.environ, timeout=None):
    """Given a command, run it. Return a tuple of the return code and any
    output.

    :param timeout: If specified, the command will be terminated after timout
        seconds.
    We wanted to use plumbum instead of rolling our own thing with
    subprocess.Popen but were blocked by
    https://github.com/tomerfiliba/plumbum/issues/162 and our local BASH_FUNC
    magic.
    """
    try:
        process = Popen(shlex.split(command), stdout=PIPE, stderr=STDOUT, env=env)
        process.name = command
        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, (process,))
            proctimer.start()
        # execute it, the output goes to the stdout
        output, _ = process.communicate()
        # when finished, get the exit code
        returncode = process.returncode
    except OSError as e:
        output = e.strerror
        returncode = e.errno
    # Stop the timer
    if timeout:
        proctimer.cancel()
    return returncode, output


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
