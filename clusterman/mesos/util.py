# Copyright 2019 Yelp Inc.
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
import re
from typing import Any
from typing import Mapping
from typing import Sequence

import colorlog
import requests
from mypy_extensions import TypedDict

from clusterman.exceptions import PoolConnectionError
from clusterman.util import ClustermanResources

logger = colorlog.getLogger(__name__)


class MesosAgentDict(TypedDict):
    attributes: Mapping[str, str]
    id: str
    pid: str
    resources: Mapping[str, Any]
    used_resources: Mapping[str, Any]


class MesosAgents(TypedDict):
    slaves: Sequence[MesosAgentDict]


class MesosTaskDict(TypedDict):
    id: str
    framework_id: str
    slave_id: str
    state: str


class MesosFrameworkDict(TypedDict):
    active: str
    id: str
    name: str
    registered_time: str
    tasks: Sequence[MesosTaskDict]
    unregistered_time: str
    used_resources: Mapping[str, Any]


class MesosFrameworks(TypedDict):
    completed_frameworks: Sequence[MesosFrameworkDict]
    frameworks: Sequence[MesosFrameworkDict]


def agent_pid_to_ip(agent_pid: str) -> str:
    """Convert the agent PID from Mesos into an IP address

    :param: agent pid (this is in the format 'slave(1)@10.40.31.172:5051')
    :returns: ip address
    """
    m = re.match(r'.+?@([\d\.]+):\d+', agent_pid)
    assert m
    return m.group(1)


def allocated_agent_resources(agent_dict: MesosAgentDict) -> ClustermanResources:
    used_resources = agent_dict.get('used_resources', {})
    return ClustermanResources(
        cpus=used_resources.get('cpus', 0),
        mem=used_resources.get('mem', 0),
        disk=used_resources.get('disk', 0),
        gpus=used_resources.get('gpus', 0),
    )


def mesos_post(url: str, endpoint: str) -> requests.Response:
    master_url = url if endpoint == 'redirect' else mesos_post(url, 'redirect').url + '/'
    request_url = master_url + endpoint
    response = None
    try:
        response = requests.post(
            request_url,
            headers={'user-agent': 'clusterman'},
        )
        response.raise_for_status()
    except Exception as e:  # there's no one exception class to check for problems with the request :(
        log_message = (
            f'Mesos is unreachable:\n\n'
            f'{str(e)}\n'
            f'Querying Mesos URL: {request_url}\n'
        )
        if response is not None:
            log_message += (
                f'Response Code: {response.status_code}\n'
                f'Response Text: {response.text}\n'
            )
        logger.critical(log_message)
        raise PoolConnectionError(f'Mesos master unreachable: check the logs for details') from e

    return response


def total_agent_resources(agent: MesosAgentDict) -> ClustermanResources:
    resources = agent.get('resources', {})
    return ClustermanResources(
        cpus=resources.get('cpus', 0),
        mem=resources.get('mem', 0),
        disk=resources.get('disk', 0),
        gpus=resources.get('gpus', 0),
    )
