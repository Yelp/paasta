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
from typing import List
from typing import Tuple

from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cli.cmds.status import get_envoy_status_human
from paasta_tools.cli.cmds.status import get_smartstack_status_human
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import verify_instances
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SystemPaastaConfig


def add_subparser(subparsers) -> None:
    mesh_status_parser = subparsers.add_parser(
        "mesh-status",
        help="Display the mesh status of a PaaSTA service.",
        description=(
            "'paasta mesh-status' queries the PaaSTA API in order to report "
            "on the health of a PaaSTA service in the mesh."
        ),
    )
    mesh_status_parser.add_argument(
        "-s",
        "--service",
        type=str,
        help="The name of the service you wish to inspect",
        required=True,
    ).completer = lazy_choices_completer(list_services)
    mesh_status_parser.add_argument(
        "-i",
        "--instance",
        type=str,
        help="The name of the instance of the service you wish to inspect",
        required=True,
    )  # No completer because we need to know service first and we can't until some other stuff has happened
    mesh_status_parser.add_argument(
        "-c",
        "--cluster",
        type=str,
        help="The name of the cluster in which the instance runs",
        required=True,
    ).completer = lazy_choices_completer(list_clusters)
    mesh_status_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    mesh_status_parser.set_defaults(command=paasta_mesh_status)


def paasta_mesh_status_on_api_endpoint(
    cluster: str,
    service: str,
    instance: str,
    system_paasta_config: SystemPaastaConfig,
) -> Tuple[int, List[str]]:
    client = get_paasta_oapi_client(cluster, system_paasta_config)
    if not client:
        print("ERROR: Cannot get a paasta-api client")
        exit(1)

    try:
        mesh_status = client.service.mesh_instance(
            service=service,
            instance=instance,
            include_smartstack=False,
        )
    except client.api_error as exc:
        # 405 (method not allowed) is returned for instances that are not configured
        # for the mesh, or for which getting mesh status is not supported
        return (
            exc.status,
            [PaastaColors.red(exc.body if exc.status == 405 else exc.reason)],
        )
    except (client.connection_error, client.timeout_error) as exc:
        return (
            1,
            [PaastaColors.red(f"Could not connect to API: {exc.__class__.__name__}")],
        )
    except Exception as e:
        output = [PaastaColors.red(f"Exception when talking to the API:")]
        output.extend(str(e).split("\n"))
        return 1, output

    output = []
    if mesh_status.smartstack is not None:
        smartstack_status_human = get_smartstack_status_human(
            mesh_status.smartstack.registration,
            mesh_status.smartstack.expected_backends_per_location,
            mesh_status.smartstack.locations,
        )
        output.extend(smartstack_status_human)
    if mesh_status.envoy is not None:
        envoy_status_human = get_envoy_status_human(
            mesh_status.envoy.registration,
            mesh_status.envoy.expected_backends_per_location,
            mesh_status.envoy.locations,
        )
        output.extend(envoy_status_human)

    return 0, output


def paasta_mesh_status(args) -> int:
    system_paasta_config = load_system_paasta_config()

    # validate args, funcs have their own error output
    service = figure_out_service_name(args, args.soa_dir)
    if verify_instances(args.instance, service, [args.cluster]):
        return 1

    return_code, mesh_output = paasta_mesh_status_on_api_endpoint(
        cluster=args.cluster,
        service=service,
        instance=args.instance,
        system_paasta_config=system_paasta_config,
    )

    output = [
        f"service: {service}",
        f"cluster: {args.cluster}",
        f"instance: {PaastaColors.cyan(args.instance)}",
    ]
    output.extend(["  " + line for line in mesh_output])
    print("\n".join(output))

    return return_code
