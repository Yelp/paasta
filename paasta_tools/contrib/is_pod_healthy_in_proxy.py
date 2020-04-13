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
import socket
import sys

from paasta_tools.envoy_tools import are_services_up_in_pod as is_envoy_ready
from paasta_tools.envoy_tools import service_is_in_envoy
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

host_ip = socket.gethostbyname(envoy_host)
pod_ip = os.environ["PAASTA_POD_IP"]


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
        "pod_port", help="Pod Port", type=int,
    )

    parser.add_argument(
        "services", nargs="+", help="List of service.instance names",
    )

    return parser


def main() -> None:
    args = get_parser().parse_args()

    # All proxied services are in smartstack
    smartstack_services = args.services

    # Some proxied services are also in envoy
    envoy_services = [
        service for service in args.services if service_is_in_envoy(service)
    ]

    if args.smartstack_readiness_check_enabled:
        smartstack_ready = is_smartstack_ready(
            synapse_host=synapse_host,
            synapse_port=synapse_port,
            synapse_haproxy_url_format=synapse_haproxy_url_format,
            services=smartstack_services,
            host_ip=pod_ip,
            host_port=args.pod_port,
        )
    else:
        smartstack_ready = True

    if args.envoy_readiness_check_enabled:
        envoy_ready = is_envoy_ready(
            envoy_host=envoy_host,
            envoy_admin_port=envoy_admin_port,
            registrations=envoy_services,
            host_ip=host_ip,
            pod_ip=pod_ip,
            pod_port=args.pod_port,
        )
    else:
        envoy_ready = True

    if smartstack_ready and envoy_ready:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
