#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
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
import argparse
import os
import sys

from paasta_tools.envoy_tools import are_namespaces_up_in_eds
from paasta_tools.envoy_tools import are_services_up_in_pod as is_envoy_ready
from paasta_tools.smartstack_tools import (
    are_services_up_on_ip_port as is_smartstack_ready,
)
from paasta_tools.utils import load_system_paasta_config


system_paasta_config = load_system_paasta_config()

synapse_port = system_paasta_config.get_synapse_port()
synapse_host = "169.254.255.254"
synapse_haproxy_url_format = system_paasta_config.get_synapse_haproxy_url_format()

envoy_host = os.environ["PAASTA_HOST"]
envoy_admin_port = system_paasta_config.get_envoy_admin_port()
envoy_admin_endpoint_format = system_paasta_config.get_envoy_admin_endpoint_format()
envoy_eds_path = "/nail/etc/envoy/endpoints"
pod_ip = os.environ["PAASTA_POD_IP"]

###############################################################
#
# This file is used in the hacheck sidecar, make sure to update `check_proxy_up.sh`
# when changing this file
#
###############################################################


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--enable-smartstack",
        dest="smartstack_readiness_check_enabled",
        action="store_true",
        help="Check smartstack readiness",
    )

    parser.add_argument(
        "--enable-envoy",
        action="store_true",
        dest="envoy_readiness_check_enabled",
        help="Check envoy readiness",
    )

    parser.add_argument(
        "--envoy-check-mode",
        choices=["admin-port", "eds-dir"],
        default="admin-port",
        help="Query Envoy backends through the admin interface (default) or the EDS directory",
    )

    parser.add_argument(
        "pod_port",
        help="Pod Port",
        type=int,
    )

    parser.add_argument(
        "services",
        nargs="+",
        help="List of service.instance names",
    )

    return parser


def main() -> None:
    args = get_parser().parse_args()

    if args.smartstack_readiness_check_enabled:
        smartstack_ready = is_smartstack_ready(
            synapse_host=synapse_host,
            synapse_port=synapse_port,
            synapse_haproxy_url_format=synapse_haproxy_url_format,
            services=args.services,
            host_ip=pod_ip,
            host_port=args.pod_port,
        )
    else:
        smartstack_ready = True

    if args.envoy_readiness_check_enabled:
        if args.envoy_check_mode == "admin-port":
            envoy_ready = is_envoy_ready(
                envoy_host=envoy_host,
                envoy_admin_port=envoy_admin_port,
                envoy_admin_endpoint_format=envoy_admin_endpoint_format,
                registrations=args.services,
                pod_ip=pod_ip,
                pod_port=args.pod_port,
            )
        elif args.envoy_check_mode == "eds-dir":
            envoy_ready = are_namespaces_up_in_eds(
                envoy_eds_path=envoy_eds_path,
                namespaces=args.services,
                pod_ip=pod_ip,
                pod_port=args.pod_port,
            )

    else:
        envoy_ready = True

    if smartstack_ready and envoy_ready:
        sys.exit(0)
    else:
        if not smartstack_ready:
            print(
                f"Could not find backend {pod_ip}:{args.pod_port} for service {args.services} "
                f"on Haproxy at {synapse_host}:{synapse_port}"
            )
        if not envoy_ready:
            print(
                f"Could not find backend {pod_ip}:{args.pod_port} for service {args.services} "
                f"on Envoy at {envoy_host}"
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
