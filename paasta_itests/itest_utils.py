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
import contextlib
import json
import os
import time

import mock
import requests
import requests_cache
from marathon import NotFoundError

import paasta_tools.mesos.master
from paasta_tools import marathon_tools
from paasta_tools.marathon_tools import app_has_tasks
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import timeout


@contextlib.contextmanager
def patch_mesos_cli_master_config():
    mesos_config = {
        "master": "%s" % get_service_connection_string('mesosmaster'),
        "scheme": "http",
        "response_timeout": 5,
    }

    with mock.patch.object(paasta_tools.mesos.master, 'CFG', mesos_config):
        yield


def update_context_marathon_config(context):
    whitelist_keys = set(['id', 'backoff_factor', 'backoff_seconds', 'max_instances', 'mem', 'cpus', 'instances'])
    with contextlib.nested(
        # This seems to be necessary because mesos reads the config file at
        # import which is sometimes before the tests get a chance to write the
        # config file
        patch_mesos_cli_master_config(),
        mock.patch.object(SystemPaastaConfig, 'get_zk_hosts', autospec=True, return_value=context.zk_hosts),
        mock.patch.object(MarathonServiceConfig, 'get_min_instances', autospec=True, return_value=1),
        mock.patch.object(MarathonServiceConfig, 'get_max_instances', autospec=True),
    ) as (
        _,
        _,
        _,
        mock_get_max_instances,
    ):
        mock_get_max_instances.return_value = context.max_instances if 'max_instances' in context else None
        context.marathon_complete_config = {key: value for key, value in marathon_tools.create_complete_config(
            context.service,
            context.instance,
            soa_dir=context.soa_dir,
        ).items() if key in whitelist_keys}
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


@timeout()
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
                return
            else:
                print "waiting for app %s to have %d tasks. retrying" % (app_id, expected_tasks)
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
