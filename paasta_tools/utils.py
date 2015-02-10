import contextlib
import datetime
import json
import shlex
import tempfile
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


def configure_log():
    """We will log to the yocalhost binded scribe."""
    clog.config.configure(scribe_host='169.254.255.254', scribe_port=1463)


def _now():
    return datetime.datetime.utcnow().isoformat()


def format_log_line(cluster, instance, component, line):
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """
    now = _now()
    message = json.dumps({
        'timestamp': now,
        'cluster': cluster,
        'instance': instance,
        'component': component,
        'message': line,
    }, sort_keys=True)
    return message


def get_log_name_for_service(service_name):
    return 'stream_paasta_%s' % service_name


def _log(service_name, line, component, cluster='N/A', instance='N/A'):
    """This expects someone (currently the paasta cli main()) to have already
    configured the log object. We'll just write things to it.
    """
    line = format_log_line(cluster, instance, component, line)
    line = str(line)
    log_name = get_log_name_for_service(service_name)
    clog.log_line(log_name, line)


def _run(command):
    """Given a command, run it. Return a tuple of the return code and any
    output.

    We wanted to use plumbum instead of rolling our own thing with
    subprocess.Popen but were blocked by
    https://github.com/tomerfiliba/plumbum/issues/162 and our local BASH_FUNC
    magic.
    """
    try:
        process = Popen(shlex.split(command), stdout=PIPE, stderr=STDOUT)
        # execute it, the output goes to the stdout
        output, _ = process.communicate()
        # when finished, get the exit code
        returncode = process.returncode
    except OSError as e:
        output = e.strerror
        returncode = e.errno
    return returncode, output


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
    os.rename(temp_target_path, target_path)
