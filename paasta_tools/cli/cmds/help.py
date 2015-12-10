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

"""
Contains methods used by the paasta cli tool to get more help
"""


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'help',
        description="Outputs more help about paasta in general",
        help="Outputs help about paasta. Run just `paasta help`")
    check_parser.set_defaults(command=paasta_help)


def paasta_help(args):
    print """"
Running The paasta cli
---------------
For every `paasta` command, each has individual help. For example:
  * paasta list -h
  * paasta check -h
  * etc

TIP: Did you know you can tab complete almost *everything* with the paasta command?


Working With PaaSTA as User
--------------------------
Most of the user-facing documentation for paasta can be found at
    http://y/paasta

Check this out if you are just getting started with PaaSTA.


Working With PaaSTA as an Operator
---------------------------------
Operators who have to deal with the nuts and bolts of the subsystems can find
that documentation at
    http://y/rb-mesos


Developing PaaSTA Itself
------------------------
Want to help make paasta better? Great!

The code is in:
    git@git.yelpcorp.com:paasta_tools

It is almost all python and all under test.
"""
