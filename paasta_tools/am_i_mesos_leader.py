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
"""
Usage: ./am_i_mesos_leader.py

Check if this host is the current mesos-master leader.
This is done by simply calling mesos_tools.is_mesos_leader.
Exits 0 if this is the leader, and 1 if it isn't.
"""
from sys import exit

from paasta_tools.mesos_tools import is_mesos_leader
from paasta_tools.utils import paasta_print


def main():
    if is_mesos_leader():
        paasta_print(True)
        exit(0)
    else:
        paasta_print(False)
        exit(1)


if __name__ == "__main__":
    main()
