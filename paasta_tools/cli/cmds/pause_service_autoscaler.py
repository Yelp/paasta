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
from paasta_tools.autoscaling.pause_service_autoscaler import (
    delete_service_autoscale_pause_time,
)
from paasta_tools.autoscaling.pause_service_autoscaler import (
    get_service_autoscale_pause_time,
)
from paasta_tools.autoscaling.pause_service_autoscaler import (
    update_service_autoscale_pause_time,
)
from paasta_tools.utils import _log_audit

MAX_PAUSE_DURATION = 320


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        "pause_service_autoscaler",
        help="Pause the service autoscaler for an entire cluster",
        description=(
            "'paasta pause_service_autoscaler is used to pause the paasta service autoscaler "
            "for an entire paasta cluster. "
        ),
    )
    status_parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        help="which cluster to pause autoscaling in. ie. pnw-prod",
    )
    status_parser.add_argument(
        "-d",
        "--pause-duration",
        default=120,
        dest="duration",
        type=int,
        help="How long to pause the autoscaler for, defaults to %(default)s minutes",
    )
    status_parser.add_argument(
        "-f",
        "--force",
        help="Force pause for longer than max duration",
        action="store_true",
        dest="force",
        default=False,
    )
    status_parser.add_argument(
        "-i",
        "--info",
        help="Print when the autoscaler is paused until",
        action="store_true",
        dest="info",
        default=False,
    )
    status_parser.add_argument(
        "-r",
        "--resume",
        help="Resume autoscaling (unpause) in a cluster",
        action="store_true",
        dest="resume",
        default=False,
    )

    status_parser.set_defaults(command=paasta_pause_service_autoscaler)


def paasta_pause_service_autoscaler(args):
    """With a given cluster and duration, pauses the paasta service autoscaler
    in that cluster for duration minutes"""
    if args.duration > MAX_PAUSE_DURATION:
        if not args.force:
            print(
                "Specified duration: {d} longer than max: {m}".format(
                    d=args.duration, m=MAX_PAUSE_DURATION
                )
            )
            print("If you are really sure, run again with --force")
            return 3

    if args.info:
        return_code = get_service_autoscale_pause_time(args.cluster)
    elif args.resume:
        return_code = delete_service_autoscale_pause_time(args.cluster)
        _log_audit(action="resume-service-autoscaler", cluster=args.cluster)
    else:
        minutes = args.duration
        return_code = update_service_autoscale_pause_time(args.cluster, minutes)
        _log_audit(
            action="pause-service-autoscaler",
            action_details={"duration": minutes},
            cluster=args.cluster,
        )

    return return_code
