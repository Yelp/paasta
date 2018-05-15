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
import time

import mock
import requests
import requests_cache
import service_configuration_lib
from marathon import NotFoundError

from paasta_tools import marathon_tools
from paasta_tools import mesos_tools
from paasta_tools.marathon_tools import app_has_tasks
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.text_utils import paasta_print
from paasta_tools.utils import timeout


def update_context_marathon_config(context):
    whitelist_keys = {
        'id', 'backoff_factor', 'backoff_seconds', 'max_instances', 'mem', 'cpus', 'instances',
        'marathon_shard', 'previous_marathon_shards',
    }
    with mock.patch.object(
        MarathonServiceConfig, 'get_min_instances', autospec=True, return_value=1,
    ), mock.patch.object(
        MarathonServiceConfig, 'get_max_instances', autospec=True,
    ) as mock_get_max_instances:
        mock_get_max_instances.return_value = context.max_instances if 'max_instances' in context else None
        service_configuration_lib._yaml_cache = {}
        context.job_config = marathon_tools.load_marathon_service_config_no_cache(
            service=context.service,
            instance=context.instance,
            cluster=context.system_paasta_config.get_cluster(),
            soa_dir=context.soa_dir,
        )
        context.current_client = context.marathon_clients.get_current_client_for_service(context.job_config)
        context.marathon_complete_config = {
            key: value for key, value in context.job_config.format_marathon_app_dict().items() if key in whitelist_keys
        }
    context.marathon_complete_config.update({
        'cmd': '/bin/sleep 1m',
        'constraints': None,
        'container': {
            'type': 'DOCKER',
            'docker': {
                'network': 'BRIDGE',
                'image': 'busybox',
            },
        },
    })
    if 'max_instances' not in context:
        context.marathon_complete_config['instances'] = context.instances


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
        paasta_print('Connecting marathon on %s' % marathon_service)
        try:
            response = requests.get('http://%s/ping' % marathon_service, timeout=5)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ):
            time.sleep(5)
            continue
        if response.status_code == 200:
            paasta_print("Marathon is up and running!")
            break


@timeout(30)
def wait_for_app_to_launch_tasks(client, app_id, expected_tasks, exact_matches_only=False):
    """ Wait for an app to have num_tasks tasks launched. If the app isn't found, then this will swallow the exception
    and retry. Times out after 30 seconds.

    :param client: The marathon client
    :param app_id: The app id to which the tasks belong
    :param expected_tasks: The number of tasks to wait for
    :param exact_matches_only: a boolean indicating whether we require exactly expected_tasks to be running
    """
    found = False
    with requests_cache.disabled():
        while not found:
            try:
                found = app_has_tasks(client, app_id, expected_tasks, exact_matches_only)
            except NotFoundError:
                pass
            if found:
                time.sleep(3)  # Give it a bit more time to actually launch
                return
            else:
                paasta_print("waiting for app %s to have %d tasks. retrying" % (app_id, expected_tasks))
                time.sleep(0.5)


def setup_mesos_cli_config(config_file, cluster):
    """Creates a mesos-cli.json config file for mesos.cli module.
    Sets up the environment dictionary to point to that file"""
    zookeeper_service = get_service_connection_string('zookeeper')
    mesos_cli_config = {
        "profile": "default",
        "default": {
            "master": "zk://%s/mesos-%s" % (zookeeper_service, cluster),
            "log_file": "None",
            "response_timeout": 5,
        },
    }
    paasta_print('Generating mesos.cli config file: %s' % config_file)
    with open(config_file, 'w') as fp:
        json.dump(mesos_cli_config, fp)
    os.environ['MESOS_CLI_CONFIG'] = config_file


def cleanup_file(path_to_file):
    """Removes the given file"""
    paasta_print("Removing generated file: %s" % path_to_file)
    os.remove(path_to_file)


def clear_mesos_tools_cache():
    try:
        del mesos_tools.master.CURRENT._cache
        paasta_print("cleared mesos_tools.master.CURRENT._cache")
    except AttributeError:
        pass
