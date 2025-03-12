#!/usr/bin/env python
# Copyright 2015-2017 Yelp Inc.
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

from paasta_tools.utils import PaastaColors


def add_subparser(subparsers: argparse._SubParsersAction):
    subparsers.add_parser(
        "remote-run",
        help="Schedule adhoc service sandbox on PaaSTA cluster",
        description=(
            "`paasta remote-run` is useful for running adhoc commands in "
            "context of a service's Docker image."
        ),
    )


def paasta_remote_run(args: argparse.Namespace):
    print(PaastaColors.red("Error: functionality under construction"))
    return 1
