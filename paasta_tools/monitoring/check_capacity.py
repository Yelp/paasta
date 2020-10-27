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
import json
import sys
from collections import defaultdict

from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.utils import load_system_paasta_config


def parse_capacity_check_options():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "type", choices=["cpus", "mem", "disk"], type=str, help="The resource to check."
    )
    parser.add_argument(
        "--warn",
        "-w",
        dest="warn",
        type=float,
        default=80,
        help="Level to emit a warning status.",
    )
    parser.add_argument(
        "--crit",
        "-c",
        dest="crit",
        type=float,
        default=90,
        help="Level to emit a critical status.",
    )
    parser.add_argument(
        "--overrides",
        dest="overrides",
        type=str,
        help="json file of per-attribute overrides.\n"
        "In the format [{groupings: {attribute: value, ...}, warn: {cpus: num, disk: num, mem: num}, "
        "crit: {cpus: num, disk: num, mem: num}}, ...]",
    )
    parser.add_argument(
        "--cluster",
        dest="cluster",
        type=str,
        help="Cluster to check. Defaults to looking for the current cluster.",
    )
    parser.add_argument(
        "--attributes",
        dest="attributes",
        type=str,
        default="pool",
        help="Comma separated list of attributes to check.\n"
        "Checks combinations of attributes",
    )
    options = parser.parse_args()

    return options


def calc_percent_usage(resource_item, value_to_check):
    values = resource_item[value_to_check]
    if values["total"] == 0:
        return 0
    return 100 * (values["used"] / values["total"])


def error_message(failures, level, cluster, value_to_check):
    result = f"{level} cluster {cluster} {value_to_check} usage:\n"
    results = []
    for f in failures:
        attrs = ", ".join(["{}: {}".format(e["attr"], e["value"]) for e in f["attrs"]])
        results.append(
            "    {} is at {:.2f} percent {}, maximum {:.2f} percent".format(
                attrs, f["current"], value_to_check, f["maximum"]
            )
        )

    result += "\n".join(results)

    return result


def get_check_from_overrides(overrides, default_check, groupings):
    """Get the overrides dict from overrides with the same groupings as groupings,
    or return the default"""
    checks = [o for o in overrides if o["groupings"] == groupings]
    if len(checks) == 0:
        return default_check
    elif len(checks) == 1:
        return checks[0]
    else:
        group_string = ", ".join([f"{k}: {v}" for k, v in groupings.items()])
        print("UNKNOWN Multiple overrides specified for %s" % group_string)
        sys.exit(3)


def read_overrides(override_file):
    if override_file:
        with open(override_file, "r") as f:
            return json.loads(f.read())
    else:
        return {}


def run_capacity_check():
    options = parse_capacity_check_options()
    system_paasta_config = load_system_paasta_config()
    cluster = (
        options.cluster
        if options.cluster is not None
        else system_paasta_config.get_cluster()
    )
    value_to_check = options.type

    client = get_paasta_oapi_client(cluster=cluster)
    if client is None:
        print("UNKNOWN Failed to load paasta api client")
        sys.exit(3)

    overrides = read_overrides(options.overrides)

    attributes = options.attributes.split(",")

    try:
        resource_use = client.resources.resources(groupings=attributes)
    except client.api_error as e:
        print(f"UNKNOWN received exception from paasta api:\n\t%s{e}")
        sys.exit(3)

    default_check = {
        "warn": {"cpus": options.warn, "mem": options.warn, "disk": options.warn},
        "crit": {"cpus": options.crit, "mem": options.crit, "disk": options.crit},
    }

    failures = defaultdict(list)
    for usage_value in resource_use.value:
        check = get_check_from_overrides(
            overrides, default_check, usage_value["groupings"]
        )
        usage_percent = calc_percent_usage(usage_value, value_to_check)
        for c in ["crit", "warn"]:
            if usage_percent > check[c][value_to_check]:
                failures[c].append(
                    {
                        "attrs": [
                            {"attr": a, "value": v}
                            for a, v in usage_value["groupings"].items()
                        ],
                        "maximum": check[c][value_to_check],
                        "current": usage_percent,
                    }
                )
                break

    return_value = [0]
    if len(failures["crit"]) > 0:
        result = error_message(failures["crit"], "CRITICAL", cluster, value_to_check)
        print(result)
        return_value.append(2)
    if len(failures["warn"]) > 0:
        result = error_message(failures["warn"], "WARNING", cluster, value_to_check)
        print(result)
        return_value.append(1)

    if max(return_value) == 0:
        print(f"OK cluster {cluster} is below critical capacity in {value_to_check}")

    sys.exit(max(return_value))


if __name__ == "__main__":
    run_capacity_check()
