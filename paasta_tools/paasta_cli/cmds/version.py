#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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

"""Contains methods used to return the current version of the PaaSTA client"""
import pkg_resources


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'version',
        description="Print the current version of the PaaSTA client",
        help="Print the current version of the PaaSTA client")
    list_parser.set_defaults(command=paasta_version)


def paasta_version(args):
    """Print the current version of the PaaSTA client.
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    print pkg_resources.require("paasta-tools")[0].version
