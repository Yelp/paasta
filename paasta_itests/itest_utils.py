# Copyright 2015-2016 Yelp Inc.
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

from paasta_tools import mesos_tools


def get_service_connection_string(service):
    """Given a container name this function returns
    the host and ephemeral port that you need to use to connect to. For example
    if you are spinning up a 'web' container that inside listens on 80, this
    function would return 0.0.0.0:23493 or whatever ephemeral forwarded port
    it has from docker-compose"""
    service = service.upper()
    raw_host_port = os.environ["%s_PORT" % service]
    # Remove leading tcp:// or similar
    host_port = raw_host_port.split("://")[1]
    return host_port


def setup_mesos_cli_config(config_file, cluster):
    """Creates a mesos-cli.json config file for mesos.cli module.
    Sets up the environment dictionary to point to that file"""
    zookeeper_service = get_service_connection_string("zookeeper")
    mesos_cli_config = {
        "profile": "default",
        "default": {
            "master": f"zk://{zookeeper_service}/mesos-{cluster}",
            "log_file": "None",
            "response_timeout": 5,
        },
    }
    print("Generating mesos.cli config file: %s" % config_file)
    with open(config_file, "w") as fp:
        json.dump(mesos_cli_config, fp)
    os.environ["MESOS_CLI_CONFIG"] = config_file


def cleanup_file(path_to_file):
    """Removes the given file"""
    print("Removing generated file: %s" % path_to_file)
    os.remove(path_to_file)


def clear_mesos_tools_cache():
    try:
        del mesos_tools.master.CURRENT._cache
        print("cleared mesos_tools.master.CURRENT._cache")
    except AttributeError:
        pass
