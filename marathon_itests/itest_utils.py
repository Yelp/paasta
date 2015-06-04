import threading
import sys
import time

import requests
from fig.cli import command


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


def no_marathon():
    """Helper function for wait_for_marathon timeout"""
    print 'Failed to connect to marathon, canceling integration tests'
    sys.exit(1)


def wait_for_marathon():
    """Waits for marathon to start. Maximum 30 seconds"""
    marathon_service = get_service_connection_string('marathon')
    reqtimer = threading.Timer(30, no_marathon)
    reqtimer.start()
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
            reqtimer.cancel()
            print "Marathon is up and running!"
            break

