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
"""Usage: ./synapse_srv_namespaces_fact.py

A simple script to enumerate all namespaces as a sorted comma separated
string to stdout, with each entry in the form of full_name:proxy_port.

If a proxy_port isn't defined for a namespace, that namespace is skipped.

Example output: mumble.canary:5019,mumble.main:111,zookeeper.hab:4921

This is nice to use as a facter fact for Synapse stuff!
"""
import sys

from paasta_tools import long_running_service_tools


def main():
    strings = []
    for full_name, config in long_running_service_tools.get_all_namespaces():
        if "proxy_port" in config:
            strings.append("{}:{}".format(full_name, config["proxy_port"]))
    strings = sorted(strings)
    print("synapse_srv_namespaces=" + ",".join(strings))
    sys.exit(0)


if __name__ == "__main__":
    main()
