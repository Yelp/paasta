import json
import errno
import os
import signal
import sys
import time
from functools import wraps

import requests
from fig.cli import command


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


def print_container_logs(service_name):
    print get_fig_service(service_name).get_container().logs()


def get_fig_service(service_name):
    """Returns a fig object for the service"""
    cmd = command.Command()
    project = cmd.get_project(cmd.get_config_path())
    return project.get_service(service_name)


def get_service_connection_string(service_name):
    """Given a container name this function returns
    the host and ephemeral port that you need to use to connect to. For example
    if you are spinning up a 'web' container that inside listens on 80, this
    function would return 0.0.0.0:23493 or whatever ephemeral forwarded port
    it has from fig"""
    service_port = get_service_internal_port(service_name)
    return get_fig_service(service_name).get_container().get_local_port(service_port)


def get_service_internal_port(service_name):
    """Gets the exposed port for service_name from fig.yml. If there are
    multiple ports. It returns the first one."""
    return get_fig_service(service_name).options['ports'][0]


@timeout(30, error_message='Marathon service is not available. Cancelling integration tests')
def wait_for_marathon():
    """Waits for marathon to start. Maximum 30 seconds"""
    marathon_service = get_service_connection_string('marathon')
    while True:
        print 'Connecting marathon on %s' % marathon_service
        try:
            response = requests.get('http://%s/ping' % marathon_service, timeout=5)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ):
            time.sleep(5)
            continue
        if response.status_code == 200:
            print "Marathon is up and running!"
            break


def setup_mesos_cli_config(config_file, cluster):
    """Creates a mesos-cli.json config file for mesos.cli module.
    Sets up the environment dictionary to point to that file"""
    zookeeper_service = get_service_connection_string('zookeeper')
    mesos_cli_config = {
        "profile": "default",
        "default": {
            "master": "zk://%s/mesos-%s" % (zookeeper_service, cluster),
            "log_file": "None",
        }
    }
    print 'Generating mesos.cli config file: %s' % config_file
    with open(config_file, 'w') as fp:
        json.dump(mesos_cli_config, fp)
    os.environ['MESOS_CLI_CONFIG'] = config_file


def cleanup_file(path_to_file):
    """Removes the given file"""
    print "Removing generated file: %s" % path_to_file
    os.remove(path_to_file)
