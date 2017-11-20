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
import time
from datetime import datetime

from paasta_tools.cli.utils import delete_service_autoscale_pause_time
from paasta_tools.cli.utils import get_service_autoscale_pause_time
from paasta_tools.cli.utils import update_service_autoscale_pause_time
from paasta_tools.utils import paasta_print

MAX_PAUSE_DURATION = 320


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'pause_service_autoscaler',
        help="Pause the service autoscaler for an entire cluster",
        description=(
            "'paasta pause_service_autoscaler is used to pause the paasta service autoscaler "
            "for an entire paasta cluster. "
        ),
    )
    status_parser.add_argument(
        '-c', '--cluster',
        dest="cluster",
        help='which cluster to pause autoscaling in. ie. norcal-prod',
    )
    status_parser.add_argument(
        '-d', '--pause-duration',
        default='120',
        dest="duration",
        help="How long to pause the autoscaler for, defaults to 120 minutes",
    )
    status_parser.add_argument(
        '-f', '--force',
        help='Force pause for longer than max duration',
        action='store_true',
        dest='force',
        default=False,
    )
    status_parser.add_argument(
        '-i', '--info',
        help='Print when the autoscaler is paused until',
        action='store_true',
        dest='info',
        default=False,
    )
    status_parser.add_argument(
        '-r', '--resume',
        help='Resume autoscaling (unpause) in a cluster',
        action='store_true',
        dest='resume',
        default=False,
    )

    status_parser.set_defaults(command=paasta_pause_service_autoscaler)


def paasta_pause_service_autoscaler(args):
    """With a given cluster and duration, pauses the paasta service autoscaler
       in that cluster for duration minutes"""
    if int(args.duration) > MAX_PAUSE_DURATION:
        if not args.force:
            paasta_print('Specified duration: {d} longer than max: {m}'.format(
                d=args.duration,
                m=MAX_PAUSE_DURATION,
            ))
            paasta_print('If you are really sure, run again with --force')
            return 3

    minutes = args.duration
    retval = 0

    if args.info:
        retval = get_service_autoscale_pause_time(args.cluster)
    elif args.resume:
        retval = delete_service_autoscale_pause_time(args.cluster)
    else:
        retval = update_service_autoscale_pause_time(args.cluster, minutes)

    if retval == 1:
        paasta_print('Could not connect to paasta api. Maybe you misspelled the cluster?')
        return 1
    elif retval == 2:
        paasta_print('Could not connect to zookeeper server')
        return 2

    elif args.info:
        if retval < time.time():
            paasta_print('Service autoscaler is not paused')
            return 0
        else:
            paused_readable = datetime.fromtimestamp(retval).strftime('%H:%M:%S %Y-%m-%d')
            paasta_print('Service autoscaler is paused until {}'.format(paused_readable))
            return 0

    return 0
