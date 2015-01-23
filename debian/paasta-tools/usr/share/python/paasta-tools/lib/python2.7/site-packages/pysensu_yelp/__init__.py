#!/usr/bin/env python

import json
import socket
import re
from ordereddict import OrderedDict

SENSU_ON_LOCALHOST = ('localhost', 3030)

# Status codes for sensu checks
# Code using this module can write pysensu_yelp.Status.OK, etc
# for easy status codes
Status = type('Enum', (), {
    'OK':       0,
    'WARNING':  1,
    'CRITICAL': 2,
    'UNKNOWN':  3
})

# Copied from: http://thomassileo.com/blog/2013/03/31/how-to-convert-seconds-to-human-readable-interval-back-and-forth-with-python/
interval_dict = OrderedDict([("Y", 365*86400),  # 1 year
                             ("M", 30*86400),   # 1 month
                             ("W", 7*86400),    # 1 week
                             ("D", 86400),      # 1 day
                             ("h", 3600),       # 1 hour
                             ("m", 60),         # 1 minute
                             ("s", 1)])         # 1 second

def human_to_seconds(string):
    """Convert internal string like 1M, 1Y3M, 3W to seconds.

    :type string: str
    :param string: Interval string like 1M, 1W, 1M3W4h2s...
        (s => seconds, m => minutes, h => hours, D => days, W => weeks, M => months, Y => Years).

    :rtype: int
    :return: The conversion in seconds of string.
    """
    interval_exc = "Bad interval format for {0}".format(string)

    interval_regex = re.compile("^(?P<value>[0-9]+)(?P<unit>[{0}])".format("".join(interval_dict.keys())))
    seconds = 0

    while string:
        match = interval_regex.match(string)
        if match:
            value, unit = int(match.group("value")), match.group("unit")
            if unit in interval_dict:
                seconds += value * interval_dict[unit]
                string = string[match.end():]
            else:
                raise Exception("'{0}' unit not present in {1}".format(
                    unit, interval_dict.keys()))
        else:
            raise Exception(interval_exc)
    return seconds

def send_event(name, runbook, status, output, team, page=False, tip=None, notification_email=None,
               check_every='5m', realert_every=1, alert_after='0s', dependencies=[],
               irc_channels=None, ticket=False, project=None, source=None):
    """Send a new event with the given information. Requires a name, runbook, status code,
    and event output, but the other keys are kwargs and have defaults."""
    if not (name and team):
        raise ValueError("Name and team must be present")
    if not runbook:
        runbook = 'Please set a runbook!'
    result_dict = {
        'name': name,
        'status': status,
        'output': output,
        'handler': 'default',
        'team': team,
        'runbook': runbook,
        'tip': tip,
        'notification_email': notification_email,
        'interval': human_to_seconds(check_every),
        'page': page,
        'realert_every': int(realert_every),
        'dependencies': dependencies,
        'alert_after': human_to_seconds(alert_after),
        'ticket': ticket,
        'project': project,
        'source': source,
    }
    if irc_channels:
        result_dict['irc_channels'] = irc_channels

    json_hash = json.dumps(result_dict)
    sock = socket.socket()
    try:
        sock.connect(SENSU_ON_LOCALHOST)
        sock.sendall(json_hash + '\n')
    finally:
        sock.close()
