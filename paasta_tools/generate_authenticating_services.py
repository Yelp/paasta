#!/usr/bin/env python3
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
"""
A simple script to enumerate all services participating in authenticated
communications, and list them in a YAML/JSON file.
"""
import argparse
import glob
import logging
import os
from typing import Dict
from typing import List
from typing import Set

import yaml

from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import write_json_configuration_file
from paasta_tools.utils import write_yaml_configuration_file


AUTHORIZATION_CONFIG_FILE = "authorization.yaml"


def list_services_in_authz_config(config_path: str) -> Set[str]:
    auth_config: dict = {}
    try:
        with open(config_path) as f:
            auth_config = yaml.safe_load(f)
    except Exception as e:
        logging.warning(f"Issue loading {config_path}: {e}")
    return {
        service
        for rule in auth_config.get("authorization", {}).get("rules", [])
        for service in rule.get("identity_groups", {}).get("services", [])
    }


def enumerate_authenticating_services() -> Dict[str, List[str]]:
    result = set()
    config_path_pattern = os.path.join(DEFAULT_SOA_DIR, "*", AUTHORIZATION_CONFIG_FILE)
    for authz_config in glob.glob(config_path_pattern):
        result.update(list_services_in_authz_config(authz_config))
    result.update(load_system_paasta_config().get_always_authenticating_services())
    return {"services": sorted(result)}


def parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-o",
        "--output",
        help="Output filepath",
        dest="output_filename",
        required=True,
    )
    parser.add_argument(
        "-f",
        "--format",
        help="Output format. Defaults to %(default)s",
        dest="output_format",
        choices=["yaml", "json"],
        default="yaml",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if args.output_format == "yaml":
        writer = write_yaml_configuration_file
    elif args.output_format == "json":
        writer = write_json_configuration_file
    else:
        raise NotImplementedError(f"Unknown format: {args.output_format}")
    configuration = enumerate_authenticating_services()
    writer(filename=args.output_filename, configuration=configuration)


if __name__ == "__main__":
    main()
