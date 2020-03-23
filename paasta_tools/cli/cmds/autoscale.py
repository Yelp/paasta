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

from bravado.exception import HTTPNotFound

from paasta_tools.api import client
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.utils import _log_audit
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors


log = logging.getLogger(__name__)


def add_subparser(subparsers):
    autoscale_parser = subparsers.add_parser(
        "autoscale",
        help="Manually scale a service up and down manually, bypassing the normal autoscaler",
    )

    autoscale_parser.add_argument(
        "-s", "--service", help="Service that you want to stop. Like 'example_service'."
    ).completer = lazy_choices_completer(list_services)
    autoscale_parser.add_argument(
        "-i",
        "--instance",
        help="Instance of the service that you want to stop. Like 'main' or 'canary'.",
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    autoscale_parser.add_argument(
        "-c",
        "--cluster",
        help="The PaaSTA cluster that has the service instance you want to stop. Like 'norcal-prod'.",
        required=True,
    ).completer = lazy_choices_completer(list_clusters)
    autoscale_parser.add_argument(
        "--set", help="Set the number to scale to. Must be an Int.", type=int
    )
    autoscale_parser.set_defaults(command=paasta_autoscale)


def paasta_autoscale(args):
    log.setLevel(logging.DEBUG)
    service = figure_out_service_name(args)
    api = client.get_paasta_api_client(cluster=args.cluster, http_res=True)
    if not api:
        paasta_print(
            "Could not connect to paasta api. Maybe you misspelled the cluster?"
        )
        return 1
    # try:
    if args.set is None:
        log.debug("Getting the current autoscaler count...")
        res, http = api.autoscaler.get_autoscaler_count(
            service=service, instance=args.instance
        ).result()
    else:
        log.debug(f"Setting desired instances to {args.set}.")
        body = {"desired_instances": int(args.set)}
        res, http = api.autoscaler.update_autoscaler_count(
            service=service, instance=args.instance, json_body=body
        ).result()
        _log_audit(
            action="manual-scale",
            action_details=body,
            service=service,
            instance=args.instance,
            cluster=args.cluster,
        )
    # except HTTPNotFound:
    #     paasta_print(
    #         PaastaColors.red(
    #             f"ERROR: '{args.instance}' is not configured to autoscale, "
    #             f"so paasta autoscale could not scale it up on demand. "
    #             f"If you want to be able to boost this service, please configure autoscaling for the service "
    #             f"in its config file by setting min and max instances. Example: \n"
    #             f"{args.instance}:\n"
    #             f"     min_instances: 5\n"
    #             f"     max_instances: 50"
    #         )
    #     )
    #     return 0

    log.debug(f"Res: {res} Http: {http}")
    print(res["desired_instances"])
    return 0
