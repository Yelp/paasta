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
import logging
import time
from datetime import datetime
from typing import Optional

import pytz
from pytimeparse.timeparse import timeparse

import paasta_tools.paastaapi.models as paastamodels
from paasta_tools.api import client
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_configs_for_service
from paasta_tools.cli.utils import get_paasta_oapi_api_clustername
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.utils import _log_audit
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import PaastaColors


log = logging.getLogger(__name__)


def add_subparser(subparsers):
    autoscale_parser = subparsers.add_parser(
        "autoscale",
        help="Manually scale a service up and down manually, bypassing the normal autoscaler",
    )

    autoscale_parser.add_argument(
        "-s",
        "--service",
        help="Service that you want to autoscale. Like 'example_service'.",
    ).completer = lazy_choices_completer(list_services)
    autoscale_parser.add_argument(
        "-i",
        "--instance",
        help="Instance of the service that you want to autoscale. Like 'main' or 'canary'.",
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    autoscale_parser.add_argument(
        "-c",
        "--cluster",
        help="The PaaSTA cluster that has the service instance you want to autoscale. Like 'pnw-prod'.",
        required=True,
    ).completer = lazy_choices_completer(list_clusters)
    autoscale_parser.add_argument(
        "--set", help="Set the number to scale to. Must be an Int.", type=int
    )

    # Temporary override options
    override_group = autoscale_parser.add_argument_group("Temporary Override Options")
    override_group.add_argument(
        "--set-min",
        help="Set the minimum number of replicas (must be >= 1). Requires --for parameter.",
        type=lambda x: int(x)
        if int(x) >= 1
        else autoscale_parser.error("Minimum instances must be >= 1"),
        default=None,
    )
    override_group.add_argument(
        "--set-max",
        help="Set the maximum number of replicas (must be >= 1). Requires --for parameter.",
        type=lambda x: int(x)
        if int(x) >= 1
        else autoscale_parser.error("Maximum instances must be >= 1"),
        default=None,
    )
    override_group.add_argument(
        "--for",
        dest="duration",
        help="Duration for the temporary override (e.g. '3h', '30m'). Required when using --set-min and/or --set-max.",
        default=None,
    )

    autoscale_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    autoscale_parser.set_defaults(command=paasta_autoscale)


def parse_duration_to_seconds(duration: str) -> Optional[int]:
    """Parse a duration string like '3h' or '30m' into seconds.

    Args:
        duration_str: A string representing a duration (e.g., "3h", "30m", "1d")

    Returns:
        The duration in seconds, or None if parsing failed
    """
    if not duration:
        return None

    seconds = timeparse(duration)
    return seconds


def paasta_autoscale(args):
    log.setLevel(logging.DEBUG)
    service = figure_out_service_name(args)

    if (args.set_min is not None or args.set_max is not None) and not args.duration:
        print(
            PaastaColors.yellow(
                "WARNING: --set-min/--set-max usage requires --for parameter to specify duration - defaulting to 30m"
            )
        )
        args.duration = "30m"

    if args.duration is not None and args.set_min is None and args.set_max is None:
        print(
            PaastaColors.red("Error: --for requires --set-min or --set-max parameter")
        )
        return 1

    if args.set is not None and args.set_min is not None and args.set_max is not None:
        print(
            PaastaColors.red(
                "Error: Cannot use both --set and --set-min or --set-max at the same time"
            )
        )
        return 1

    instance_config = next(
        get_instance_configs_for_service(
            service=service,
            soa_dir=args.soa_dir,
            clusters=[args.cluster],
            instances=[args.instance],
        ),
        None,
    )
    if not instance_config:
        print(
            "Could not find config files for this service instance in soaconfigs. Maybe you misspelled an argument?"
        )
        return 1

    api = client.get_paasta_oapi_client(
        cluster=get_paasta_oapi_api_clustername(
            cluster=args.cluster,
            is_eks=(instance_config.__class__ == EksDeploymentConfig),
        ),
        http_res=True,
    )
    if not api:
        print("Could not connect to paasta api. Maybe you misspelled the cluster?")
        return 1

    # TODO: we should probably also make sure we set a couple other defaults - currently, it's possible for
    # status/res/etc to be unbound in some code paths
    err_reason = None
    try:
        # get current autoscaler count
        if args.set is None and args.set_min is None and args.set_max is None:
            log.debug("Getting the current autoscaler count...")
            res, status, _ = api.autoscaler.get_autoscaler_count(
                service=service, instance=args.instance, _return_http_data_only=False
            )

        # set desired instances
        elif args.set is not None:
            log.debug(f"Setting desired instances to {args.set}.")
            msg = paastamodels.AutoscalerCountMsg(desired_instances=int(args.set))
            res, status, _ = api.autoscaler.update_autoscaler_count(
                service=service,
                instance=args.instance,
                autoscaler_count_msg=msg,
                _return_http_data_only=False,
            )

            _log_audit(
                action="manual-scale",
                action_details=str(msg),
                service=service,
                instance=args.instance,
                cluster=args.cluster,
            )

        # set lower bound
        elif args.set_min is not None or args.set_max is not None:
            duration_seconds = parse_duration_to_seconds(args.duration)
            if not duration_seconds:
                print(
                    PaastaColors.red(
                        f"Error: Invalid duration format '{args.duration}'. "
                        f"Please use a format like '3h' or '30m'."
                    )
                )
                return 1
            # NOTE: this is explicitly using time.time() since we're doing everything using epoch time
            # for simplicity
            expiration_time = time.time() + duration_seconds

            log.debug(
                f"Sending the following overrides for duration {args.duration}: min_instances: {args.set_min}, max_instances: {args.set_max}."
            )
            msg = paastamodels.AutoscalingOverride(
                min_instances=args.set_min,
                max_instances=args.set_max,
                expire_after=expiration_time,
            )

            res, status, _ = api.autoscaler.set_autoscaling_override(
                service=service,
                instance=args.instance,
                autoscaling_override=msg,
                _return_http_data_only=False,
            )
            _log_audit(
                action="manual-autoscale-override",
                action_details=str(msg),
                service=service,
                instance=args.instance,
                cluster=args.cluster,
            )
    except api.api_error as exc:
        status = exc.status
        err_reason = exc.body

    if not 200 <= status <= 299:
        print(
            PaastaColors.red(
                f"ERROR: '{args.instance}' is not configured to autoscale OR you set impossible {{min, max}}_instances, "
                f"and `paasta autoscale` could not update it. "
                f"If you want to be able to boost this service, please configure autoscaling for the service "
                f"in its config file by setting min and max instances appropriately. Example: \n"
                f"{args.instance}:\n"
                f"     min_instances: 5\n"
                f"     max_instances: 50\n"
                f"{err_reason}"
            )
        )
        return 0

    log.debug(f"Res: {res} Http: {status}")
    if not args.set_min and not args.set_max:
        print(f"Desired instances: {res.desired_instances}")
    else:
        if args.set_min:
            print(
                f"Temporary override set for {args.service}.{args.instance} with minimum instances: {args.set_min}"
            )
        if args.set_max:
            print(
                f"Temporary override set for {args.service}.{args.instance} with maximum instances: {args.set_max}"
            )
        # folks using this might be in different timezones, so let's convert the expiration time to a few common ones
        # to make it extra clear when the override will expire
        epoch_time = datetime.fromtimestamp(res.expire_after)
        eastern_time = epoch_time.astimezone(pytz.timezone("US/Eastern"))
        pacific_time = epoch_time.astimezone(pytz.timezone("US/Pacific"))
        london_time = epoch_time.astimezone(pytz.timezone("Europe/London"))

        time_format = "%Y-%m-%d %H:%M:%S %Z%z"
        print(f"The {args.duration} override will expire at:")
        print(f"Eastern Time: {eastern_time.strftime(time_format)}")
        print(f"Pacific Time: {pacific_time.strftime(time_format)}")
        print(f"London Time:  {london_time.strftime(time_format)}")

    return 0
