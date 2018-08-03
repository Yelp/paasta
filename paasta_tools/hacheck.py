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
import re
from typing import Dict

import aiohttp
from mypy_extensions import TypedDict

from paasta_tools.utils import get_user_agent

HACHECK_CONN_TIMEOUT = 30
HACHECK_READ_TIMEOUT = 10

SpoolInfo = TypedDict(
    'SpoolInfo',
    {
        'service': str,
        'state': str,
        'since': float,
        'until': float,
        'reason': str,
    },
    total=False,
)


async def post_spool(url: str, status: str, data: Dict['str', 'str']) -> None:
    async with aiohttp.ClientSession(
        conn_timeout=HACHECK_CONN_TIMEOUT,
        read_timeout=HACHECK_READ_TIMEOUT,
    ) as session:
        async with session.post(
            url,
            data=data,
            headers={'User-Agent': get_user_agent()},
        ) as resp:
            resp.raise_for_status()


async def get_spool(spool_url: str) -> SpoolInfo:
    """Query hacheck for the state of a task, and parse the result into a dictionary."""
    if spool_url is None:
        return None

    # TODO: aiohttp says not to create a session per request. Fix this.
    async with aiohttp.ClientSession(
        conn_timeout=HACHECK_CONN_TIMEOUT,
        read_timeout=HACHECK_READ_TIMEOUT,
    ) as session:
        async with session.get(
            spool_url,
            headers={'User-Agent': get_user_agent()},
        ) as response:
            if response.status == 200:
                return {
                    'state': 'up',
                }

            regex = ''.join([
                "^",
                r"Service (?P<service>.+)",
                r" in (?P<state>.+) state",
                r"(?: since (?P<since>[0-9.]+))?",
                r"(?: until (?P<until>[0-9.]+))?",
                r"(?:: (?P<reason>.*))?",
                "$",
            ])

            response_text = await response.text()
            match = re.match(regex, response_text)
            groupdict = match.groupdict()
            info: SpoolInfo = {}
            info['service'] = groupdict['service']
            info['state'] = groupdict['state']
            if 'since' in groupdict:
                info['since'] = float(groupdict['since'] or 0)
            if 'until' in groupdict:
                info['until'] = float(groupdict['until'] or 0)
            if 'reason' in groupdict:
                info['reason'] = groupdict['reason']
            return info
