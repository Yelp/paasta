#!/usr/bin/env python
# Copyright 2015-2020 Yelp Inc.
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
import datetime
import json
import sys
import time
import traceback
from typing import List
from typing import Union

from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_table
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors


def add_subparser(
    subparsers,
) -> None:
    list_deploy_queue_parser = subparsers.add_parser(
        "list-deploy-queue",
        help="Display the deploy queue for a PaaSTA cluster",
        description="",  # TODO
    )

    list_deploy_queue_parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        help="The cluster for which to display the deploy queue",
    ).completer = lazy_choices_completer(list_clusters)
    list_deploy_queue_parser.add_argument(
        "--json",
        dest="json",
        action="store_true",
        default=False,
        help="Output the raw API response JSON",
    )
    list_deploy_queue_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )

    list_deploy_queue_parser.set_defaults(command=list_deploy_queue)


def list_deploy_queue(args) -> int:
    cluster = args.cluster
    all_clusters = list_clusters(soa_dir=args.soa_dir)
    if cluster not in all_clusters:
        print(
            f"{cluster} does not appear to be a valid cluster. Run `paasta "
            "list-clusters` to see available options."
        )
        return 1

    system_paasta_config = load_system_paasta_config()
    client = get_paasta_oapi_client(cluster, system_paasta_config, http_res=True)
    if not client:
        print("Cannot get a paasta API client")
        return 1

    try:
        deploy_queues = client.default.deploy_queue()
    except client.api_error as exc:
        print(PaastaColors.red(exc.reason))
        return exc.status
    except (client.connection_error, client.timeout_error) as exc:
        print(PaastaColors.red(f"Could not connect to API: {exc.__class__.__name__}"))
        return 1
    except Exception as exc:
        tb = sys.exc_info()[2]
        print(PaastaColors.red(f"Exception when talking to the API: {exc}"))
        print("".join(traceback.format_tb(tb)))
        return 1

    if args.json:
        json.dump(deploy_queues.to_dict(), sys.stdout)
    else:
        formatted_deploy_queues = format_deploy_queues(deploy_queues, cluster)
        print(formatted_deploy_queues)

    return 0


def format_deploy_queues(deploy_queues, cluster: str) -> str:
    lines = [
        f"Deploy Queue for Cluster {cluster}",
        "  Available Service Instances:",
    ]
    available_instances_table = create_queue_entries_table(
        deploy_queues.available_service_instances
    )
    lines.extend([f"    {line}" for line in available_instances_table])

    lines.append("  Unavailable Service Instances:")
    unavailable_instances_table = create_queue_entries_table(
        deploy_queues.unavailable_service_instances
    )
    lines.extend([f"    {line}" for line in unavailable_instances_table])

    return "\n".join(lines)


def create_queue_entries_table(service_instances) -> List[str]:
    if len(service_instances) == 0:
        return [PaastaColors.grey("Empty")]

    table_header = [
        "Service Instance",
        "Bounce by",
        "Wait until",
        "Enqueue time",
        "Bounce Start Time",
        "Processed Count",
        "Failures",
        "Watcher",
    ]
    rows = [table_header]
    for service_instance in service_instances:
        now = time.time()
        bounce_by = format_timestamp(service_instance.bounce_by)
        if service_instance.bounce_by < now:
            bounce_by = PaastaColors.red(bounce_by)

        failures = str(service_instance.failures)
        if service_instance.failures > 10:
            failures = PaastaColors.red(failures)

        processed_count = str(service_instance.processed_count)
        if service_instance.processed_count > 50:
            processed_count = PaastaColors.red(processed_count)

        rows.append(
            [
                f"{service_instance.service}.{service_instance.instance}",
                bounce_by,
                format_timestamp(service_instance.wait_until),
                format_timestamp(service_instance.enqueue_time),
                format_timestamp(service_instance.bounce_start_time),
                processed_count,
                failures,
                service_instance.watcher,
            ]
        )

    return format_table(rows)


def format_timestamp(timestamp: Union[int, float]) -> str:
    datetime_obj = datetime.datetime.fromtimestamp(timestamp)
    return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S")
