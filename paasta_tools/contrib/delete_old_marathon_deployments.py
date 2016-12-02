#!/usr/bin/env python
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
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import datetime
import logging

import dateutil.parser
from dateutil import tz
from pytimeparse import timeparse

from paasta_tools import marathon_tools


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--age', dest='age', type=timedelta_type, default='1h',
                        help="Max age of a Marathon deployment before it is stopped."
                        "Any pytimeparse unit is supported")
    parser.add_argument('-n', '--dry-run', action="store_true",
                        help="Don't actually stop any Marathon deployments")
    parser.add_argument('-v', '--verbose', action='store_true')
    options = parser.parse_args()
    return options


def timedelta_type(value):
    """Return the :class:`datetime.datetime.DateTime` for a time in the past.
    :param value: a string containing a time format supported by :mod:`pytimeparse`
    """
    if value is None:
        return None
    return datetime_seconds_ago(timeparse.timeparse(value))


def datetime_seconds_ago(seconds):
    return now() - datetime.timedelta(seconds=seconds)


def now():
    return datetime.datetime.now(tz.tzutc())


def delete_deployment_if_too_old(client, deployment, max_date, dry_run):
    started_at = dateutil.parser.parse(deployment.version)
    age = now() - started_at
    if started_at < max_date:
        if dry_run is True:
            log.warning("Would delete %s for %s as it is %s old" % (deployment.id, deployment.affected_apps[0], age))
        else:
            log.warning("Deleting %s for %s as it is %s old" % (deployment.id, deployment.affected_apps[0], age))
            client.delete_deployment(deployment_id=deployment.id, force=False)
    else:
        if dry_run is True:
            log.warning("NOT deleting %s for %s as it is %s old" % (deployment.id, deployment.affected_apps[0], age))


def main():
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    config = marathon_tools.load_marathon_config()
    client = marathon_tools.get_marathon_client(config.get_url(), config.get_username(), config.get_password())

    for deployment in client.list_deployments():
        delete_deployment_if_too_old(
            client=client,
            deployment=deployment,
            max_date=args.age,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
