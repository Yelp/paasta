import shlex
import datetime
import logging
from subprocess import Popen
from subprocess import PIPE
from subprocess import STDOUT

import clog
import staticconf


DEPLOY_PIPELINE_NON_DEPLOY_STEPS = (
    'itest',
    'security-check',
    'performance-check',
    'push-to-registry'
)

LOGLEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}


def get_git_url(service):
    """Get the git url for a service. Assumes that the service's
    repo matches its name, and that it lives in services- i.e.
    if this is called with the string 'test', the returned
    url will be git@git.yelpcorp.com:services/test.git.

    :param service: The service name to get a URL for
    :returns: A git url to the service's repository"""
    return 'git@git.yelpcorp.com:services/%s.git' % service


def configure_log():
    clog_config_path = "/nail/srv/configs/clog.yaml"
    staticconf.YamlConfiguration(clog_config_path, namespace='clog')


def _now():
    return str(datetime.datetime.now())


def format_log_line(cluster, instance, component, level, line):
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """
    now = _now()
    return {
        'timestamp': now,
        'cluster': cluster,
        'instance': instance,
        'component': component,
        'level': level,
        'message': line,
    }


def get_log_name_for_service(service_name):
    return 'stream_paasta_%s' % service_name


def get_loglevel(level):
    try:
        loglevel = LOGLEVELS[level]
    except KeyError:
        loglevel = 'INFO'
    return loglevel


def _log(service_name, line, cluster='UNKNOWN', instance='UNKNOWN', component='UNKNOWN', level='INFO'):
    """This expects someone (currently the paasta cli main()) to have already
    configured the log object. We'll just write things to it.
    """
    loglevel = get_loglevel(level)
    line = format_log_line(cluster, instance, component, loglevel, line)
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
