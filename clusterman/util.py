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
import logging
import os
import pprint
import time
from datetime import datetime
from enum import Enum
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import TypeVar

import arrow
import colorlog
import parsedatetime
import staticconf
from colorama import Fore
from colorama import Style
from staticconf.config import DEFAULT as DEFAULT_NAMESPACE

from clusterman.aws.client import dynamodb
from clusterman.config import get_cluster_config_directory
from clusterman.config import LOG_STREAM_NAME
from clusterman.config import POOL_NAMESPACE


logger = colorlog.getLogger(__name__)
CLUSTERMAN_STATE_TABLE = 'clusterman_cluster_state'
AUTOSCALER_PAUSED = 'autoscaler_paused'


# These should stay in sync with
# https://github.com/Yelp/pysensu-yelp/blob/master/pysensu_yelp/__init__.py#L122
class Status(Enum):
    OK = 0
    WARNING = 1
    CRITICAL = 2
    UNKNOWN = 3


class All:
    pass


class ClustermanResources(NamedTuple):
    cpus: float = 0
    mem: float = 0
    disk: float = 0
    gpus: float = 0


def setup_logging(log_level_str: str = 'info') -> None:
    EVENT_LOG_LEVEL = 25
    logging.addLevelName(EVENT_LOG_LEVEL, 'EVENT')

    def event(self, message, *args, **kwargs):
        if self.isEnabledFor(EVENT_LOG_LEVEL):
            self._log(EVENT_LOG_LEVEL, message, args, **kwargs)
    # we're adding a new function to Logger so ignore the type here to make mypy happy
    logging.Logger.event = event  # type: ignore

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    logger = colorlog.getLogger()
    logger.addHandler(handler)

    log_level = getattr(logging, log_level_str.upper())
    logging.getLogger().setLevel(log_level)
    logging.getLogger('botocore').setLevel(max(logging.INFO, log_level))
    logging.getLogger('boto3').setLevel(max(logging.INFO, log_level))


def get_autoscaler_scribe_stream(cluster, pool, scheduler):
    scribe_stream = f'{LOG_STREAM_NAME}_{cluster}'
    if pool != 'default':
        scribe_stream += f'_{pool}'
    if scheduler == 'kubernetes':
        scribe_stream += '_k8s'
    return scribe_stream


def log_to_scribe(scribe_stream, message):
    try:
        import clog
        clog.log_line(scribe_stream, message)
    except ModuleNotFoundError:
        logger.warn('clog not found, are you running on a Yelp host?')


_T = TypeVar('_T')


def any_of(*choices) -> Callable[[_T], bool]:
    return lambda x: x in choices


def ask_for_confirmation(prompt='Are you sure? ', default=True):
    """ Display a prompt asking for confirmation from the user before continuing; accepts any form of "yes"/"no"

    :param prompt: the prompt to display before accepting input
    :param default: the default value if CR pressed with no input
    :returns: True if "yes", False if "no"
    """
    yes, no = ('Y', 'n') if default else ('y', 'N')
    prompt += f'[{yes}/{no}] '

    while True:
        ans = input(prompt).lower().strip()
        if not ans:
            return default
        elif not ('yes'.startswith(ans) or 'no'.startswith(ans)):
            print('Please enter yes or no.')
            continue
        else:
            return 'yes'.startswith(ans)


def ask_for_choice(prompt, choices):
    enumerated_choices = [f'- {choice}: {num}\n' for num, choice in enumerate(choices)]
    full_prompt = prompt + '\n' + ''.join(enumerated_choices)

    current_prompt = full_prompt
    while True:
        ans = input(current_prompt).strip()
        if (not ans.isnumeric()) or int(ans) > len(choices):
            print('Please enter one of the numbers corresponding to a choice above.')
            current_prompt = prompt + ' '
            continue
        else:
            return choices[int(ans)]


def color_conditions(
    input_obj: _T,
    prefix: Optional[str] = None,
    postfix: Optional[str] = None,
    **kwargs: Callable[[_T], bool],
) -> str:
    prefix = prefix or ''
    postfix = postfix or ''
    color_str = ''
    for color, condition in kwargs.items():
        if condition(input_obj):
            color_str = getattr(Fore, color.upper())
            break
    return color_str + prefix + str(input_obj) + postfix + Style.RESET_ALL


def parse_time_string(time_str, tz='US/Pacific'):
    """ Convert a date or time string into an arrow object in UTC

    :param time_str: the string to convert
    :param tz: what timezone to interpret the time_str as *if no tz is specified*
    :returns: an arrow object representing the time_str in UTC
    :raises ValueError: if the time_str could not be parsed
    """

    # parsedatetime doesn't handle ISO-8601 time strings (YYYY-MM-DDThh:mm:ss+zz) so
    # try to parse it with arrow first and then use parsedatetime as a fallback (grumble)
    t = None
    try:
        t = arrow.get(time_str)
        # If the input string didn't specify a timezone, fill in the default
        if len(time_str.split('+')) == 1:
            t = t.replace(tzinfo=tz)
    except arrow.parser.ParserError:
        cal = parsedatetime.Calendar()
        parse_result = cal.parse(time_str)
        if parse_result[1] == 0:
            raise ValueError('Could not understand time {time}'.format(time=time_str))
        t = arrow.get(parse_result[0]).replace(tzinfo=tz)
    return t.to('utc')


def parse_time_interval_seconds(time_str):
    """ Convert a given time interval (e.g. '5m') into the number of seconds in that interval

    :param time_str: the string to parse
    :returns: the number of seconds in the interval
    :raises ValueError: if the string could not be parsed
    """
    cal = parsedatetime.Calendar()
    parse_result = cal.parseDT(time_str, sourceTime=datetime.min)
    if parse_result[1] == 0:
        raise ValueError('Could not understand time {time}'.format(time=time_str))
    return (parse_result[0] - datetime.min).total_seconds()


def _get_sensu():
    try:
        import pysensu_yelp
        return pysensu_yelp
    except ImportError:
        return None


def sensu_checkin(
    *,
    check_name: str,
    output: str,
    source: str,
    status: Status = Status.OK,
    app: Optional[str] = None,
    pool: Optional[str] = None,
    scheduler: Optional[str] = None,
    noop: bool = False,
    page: bool = True,
    **kwargs: Any,
) -> None:
    # This function feels like a massive hack, let's revisit and see if we can make it better (CLUSTERMAN-304)
    #
    # TODO (CLUSTERMAN-126) right now there's only one app per pool so use the global pool namespace
    # We assume the "pool" name and the "app" name are the same
    #
    # Use 'no-namespace' instead of None so we don't skip the per-cluster override
    pool_namespace = POOL_NAMESPACE.format(pool=app, scheduler=scheduler) if app else 'no-namespace'

    # read the sensu configuration from srv-configs; signals are not required to define this, so in the case
    # that they do not define anything, we fall back to the clusterman config.  The clusterman config can override
    # alerts on a per-cluster basis, so first check there; if nothing is defined there, fall back to the default,
    # which is required to be defined, so we know that someone is going to get the notification
    #
    sensu_config = dict(staticconf.read_list('sensu_config', default=[{}], namespace=pool_namespace).pop())
    if not sensu_config:
        sensu_config = dict(staticconf.read_list(f'clusters.{source}.sensu_config', default=[{}]).pop())
    if not sensu_config:
        sensu_config = dict(staticconf.read_list('sensu_config').pop())

    # If we've turned off paging in the config, we don't want this function to ever page
    config_page = sensu_config.pop('page', None)
    page = False if config_page is False else page

    # So we know where alerts are coming from precisely
    output += ''.join([
        '\n\nThis check came from:\n',
        f'- Cluster/region: {source}\n',
        f'- Pool: {pool}.{scheduler}\n' if pool else '',
        f'- App: {app}\n' if app else '',
    ])

    sensu_config.update({
        'name': check_name,
        'output': output,
        'source': source,
        'status': status.value,
        'page': page,
    })
    # values passed in to this function override config file values (is this really correct??)
    sensu_config.update(kwargs)

    pysensu_yelp = _get_sensu()
    if noop or not pysensu_yelp:
        logger.info((
            'Would have sent this event to Sensu:\n'
            f'{pprint.pformat(sensu_config)}'
        ))
        return

    # team and runbook are required entries in srv-configs, so we know this will go to the "right" place
    pysensu_yelp.send_event(**sensu_config)


def splay_event_time(frequency: int, key: str, timestamp: float = None) -> float:
    """ Return the length of time until the next event should trigger based on the given frequency;
    randomly splay out the 'initial' start time based on some key, to prevent events with the same
    frequency from all triggering at once

    :param frequency: how often the event should occur (in seconds)
    :param key: a string to hash to get the splay time
    :param timestamp: what time it is "now" (uses time.time() if None)
    :returns: the number of seconds until the next event should happen
    """
    timestamp = timestamp or time.time()
    random_wait_time = hash(key) % frequency
    return frequency - (timestamp % frequency) + random_wait_time


def read_int_or_inf(reader, param):
    return float('inf') if reader.read_string(param, default=0) == 'inf' else reader.read_int(param, default=0)


def get_pool_name_list(cluster_name: str, scheduler: str) -> List[str]:
    cluster_config_directory = get_cluster_config_directory(cluster_name)
    return [
        os.path.splitext(f)[0] for f in os.listdir(cluster_config_directory)
        if f[0] != '.' and f.endswith(scheduler)  # skip dotfiles and only read scheduler files
    ]


def get_cluster_name_list(config_namespace: str = DEFAULT_NAMESPACE) -> List[str]:
    namespace = staticconf.config.get_namespace(config_namespace)
    return namespace.get_config_dict().get('clusters', {}).keys()


def get_cluster_dimensions(cluster: str, pool: str, scheduler: Optional[str]) -> Dict[str, str]:
    return {
        'cluster': cluster,
        'pool': (f'{pool}.{scheduler}' if scheduler else pool),
    }


def autoscaling_is_paused(cluster: str, pool: str, scheduler: str, timestamp: arrow.Arrow) -> bool:
    response = dynamodb.get_item(
        TableName=CLUSTERMAN_STATE_TABLE,
        Key={
            'state': {'S': AUTOSCALER_PAUSED},
            'entity': {'S': f'{cluster}.{pool}.{scheduler}'},
        },
        ConsistentRead=True,
    )
    if 'Item' not in response:
        return False

    if (
        'expiration_timestamp' in response['Item'] and
        timestamp.timestamp > int(response['Item']['expiration_timestamp']['N'])
    ):
        return False

    return True
