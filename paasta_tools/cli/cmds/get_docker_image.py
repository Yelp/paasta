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
import sys

from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import PaastaColors
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.deployment_utils import load_v2_deployments_json
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_service_docker_registry
from paasta_tools.utils import list_services


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "get-docker-image",
        help="Gets the docker image URL for the deployment of a service",
    )
    list_parser.add_argument(
        "-s",
        "--service",
        help="Name of the service which you want to get the docker image for.",
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        "-i",
        "-l",
        "--deploy-group",
        help='Name of the deploy group, like "prod".',
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        "-d",
        "--soa-dir",
        help="A directory from which soa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )

    list_parser.set_defaults(command=paasta_get_docker_image)


def paasta_get_docker_image(args):
    service = args.service
    deploy_group = args.deploy_group
    soa_dir = args.soa_dir
    validate_service_name(service, soa_dir)

    deployments = load_v2_deployments_json(service=service, soa_dir=soa_dir)
    docker_image = deployments.get_docker_image_for_deploy_group(deploy_group)

    if not docker_image:
        print(
            PaastaColors.red(
                f"There is no {service} docker_image for {deploy_group}. Has it been deployed yet?"
            ),
            file=sys.stderr,
        )
        return 1
    else:
        registry_uri = get_service_docker_registry(service=service, soa_dir=soa_dir)
        docker_url = f"{registry_uri}/{docker_image}"
        print(docker_url)
        return 0
