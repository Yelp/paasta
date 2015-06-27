import json
import os
import threading
import sys
import time

import requests


def get_service_connection_string(service_name):
    """Given a container name this function returns
    the host and ephemeral port that you need to use to connect to. For example
    if you are spinning up a 'web' container that inside listens on 80, this
    function would return 0.0.0.0:23493 or whatever ephemeral forwarded port
    it has from fig"""
    service_name = service_name.upper()
    raw_host_port = os.environ['%s_PORT' % service_name]
    # Remove leading tcp:// or similar
    host_port = raw_host_port.split("://")[1]
    return host_port


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
