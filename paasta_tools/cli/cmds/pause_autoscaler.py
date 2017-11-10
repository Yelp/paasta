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
from paasta_tools.cli.utils import execute_pause_service_autoscaler_on_remote_master
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print

MAX_PAUSE_DURATION = 120


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'pause_autoscaler',
        help="Pause the servicce autoscaler for an entire cluster",
        description=(
            "'paasta pause_autoscaler is used to pause the paasta service autoscaler "
            "for an entire paasta cluster. Use it sparingly. "
        ),
    )
    status_parser.add_argument(
        '-c', '--cluster',
        dest="cluster",
        help='which cluster to pause autoscaling in. ie. norcal-prod',
    )
    status_parser.add_argument(
        '-d', '--pause-duration',
        default=30,
        dest="duration",
        type=int,
        help="How long to pause the autoscaler for, defaults to 30 minutes",
    )
    status_parser.add_argumet(
        '-f', '--force',
        help='Force pause for longer than max duration',
        action='store_true',
        dest='force',
        default=False,
    )
    status_parser.set_defaults(command=paasta_pause_autoscaler)


def paasta_pause_autoscaler(args):
    """With a given cluster and duration, pauses the paasta service autoscaler
       in that cluster for duration minutes"""
    if args.duration > MAX_PAUSE_DURATION:
        if not args.force:
            paasta_print('Specified duration: {d} longer than max: {m}'.format(
                d=str(args.duration),
                m=MAX_PAUSE_DURATION,
            ))
            paasta_print('If you are really sure, run again with --force')
            return 2

    return_code, output = execute_pause_service_autoscaler_on_remote_master(
        cluster=args.cluster,
        system_paasta_config=load_system_paasta_config(),
        pause_duration=args.duration,
    )

    paasta_print("Cluster: %s" % args.cluster)
    paasta_print(output)
    paasta_print()

    return return_code
