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
import argparse
import datetime
import re
import sys
from typing import Optional

from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import PaastaColors
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeploymentsJsonV2Dict
from paasta_tools.utils import format_timestamp
from paasta_tools.utils import get_pipeline_deploy_group_configs
from paasta_tools.utils import list_services
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import parse_timestamp


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "get-image-version",
        help="Returns the value to be used for an image version, which will be used in automated redeploys of the same service SHA. If no deploy groups are configured for automated redeploys, will return no output.",
    )
    parser.add_argument(
        "-f",
        "--force",
        help="Force a brand new image_version, regardless if the latest build was recent",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--max-age",
        help="max age in seconds (default %(default)s)",
        type=int,
        default=2592000,  # TODO: Get from paasta system config
    )
    arg_service = parser.add_argument(
        "-s",
        "--service",
        help="Name of the service which you want to get the image version for.",
        required=True,
    )
    arg_service.completer = lazy_choices_completer(list_services)  # type: ignore
    parser.add_argument(
        "-c",
        "--commit",
        help="Commit to be used with generated image version",
        required=True,
    )
    parser.add_argument(
        "-y",
        "--soa-dir",
        help="A directory from which soa-configs should be read",
        default=DEFAULT_SOA_DIR,
    )
    parser.set_defaults(command=paasta_get_image_version)


def check_enable_automated_redeploys(service: str, soa_dir: str) -> bool:
    # TODO: Handle global flag
    deploy_steps = get_pipeline_deploy_group_configs(service, soa_dir)
    return any([step.get("enable_automated_redeploys", False) for step in deploy_steps])


def extract_timestamp(image_version: str) -> Optional[datetime.datetime]:
    ts_str = re.match(r"^(?P<ts_str>[0-9]{8}T[0-9]{6})", image_version).group()
    return parse_timestamp(ts_str)


def should_generate_new_image_version(old: str, new: str, max_age: int) -> bool:
    # TODO: Handle additional criteria
    # Extract dates & compare
    try:
        age_diff = (extract_timestamp(new) - extract_timestamp(old)).total_seconds()

        if age_diff < max_age:
            return False
        print(
            PaastaColors.yellow(
                f"Old image version was {age_diff}s old, generating new one"
            ),
            file=sys.stderr,
        )
    except Exception as e:
        print(
            PaastaColors.red(
                f"Error: hit an exception {e} checking image version, will create new version {new}"
            ),
            file=sys.stderr,
        )
    return True


def get_latest_image_version(deployments: DeploymentsJsonV2Dict, commit: str) -> str:
    image_version = None
    # Image versions start with sortable timestamp
    # We only care about deployments for this sha; otherwise we will generate a new image_version
    sorted_image_versions = sorted(
        [
            deployment.get("image_version")
            for deployment in deployments["deployments"].values()
            if deployment.get("image_version") and deployment.get("git_sha") == commit
        ],
        reverse=True,
    )
    if sorted_image_versions:
        image_version = sorted_image_versions[0]
    return image_version


def paasta_get_image_version(args: argparse.Namespace) -> int:
    service = args.service
    soa_dir = args.soa_dir

    validate_service_name(service, soa_dir)

    # Check if any deploy groups have set enable_automated_redeploys
    if not check_enable_automated_redeploys(service, soa_dir):
        print(
            PaastaColors.red(
                f"Automated redeploys not enabled for {service}, returning no image_version"
            ),
            file=sys.stderr,
        )
        return 0

    current_timestamp = datetime.datetime.now()

    # TODO: Handle additional identifiers, flavor, etc
    new_image_version = format_timestamp(current_timestamp)

    if args.force:
        # Force a new version immediately
        print(new_image_version)
        return 0

    # get latest image_version of any deploy group from deployments.json
    deployments = load_v2_deployments_json(service, soa_dir)
    latest_image_version = get_latest_image_version(
        deployments.config_dict, commit=args.commit
    )

    if should_generate_new_image_version(
        old=latest_image_version, new=new_image_version, max_age=args.max_age
    ):
        print(new_image_version)
    else:
        print(latest_image_version)

    return 0
