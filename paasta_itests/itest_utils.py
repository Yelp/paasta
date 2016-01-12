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

import json
import os
import time

import requests
from compose.cli import command

from paasta_tools.utils import timeout


def get_docker_compose_id_from_name(container_name):
    cmd = command.Command()
    project = cmd.get_project(cmd.get_config_path())
    containers = project.containers(service_names=container_name)
    if containers is []:
        raise Exception("Could not find a container with that name")
    else:
        return containers[0].id


def get_service_connection_string(service):
    """Given a container name this function returns
    the host and ephemeral port that you need to use to connect to. For example
    if you are spinning up a 'web' container that inside listens on 80, this
    function would return 0.0.0.0:23493 or whatever ephemeral forwarded port
    it has from docker-compose"""
    service = service.upper()
    raw_host_port = os.environ['%s_PORT' % service]
    # Remove leading tcp:// or similar
    host_port = raw_host_port.split("://")[1]
    return host_port


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
