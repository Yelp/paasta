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

from paasta_tools.metrics.metastatus_lib import assert_quorum_size
from paasta_tools.utils import paasta_print


def check_mesos_quorum():
    result = assert_quorum_size()
    if result.healthy:
        paasta_print("OK: " + result.message)
        sys.exit(0)
    else:
        paasta_print(result.message)
        sys.exit(2)


if __name__ == '__main__':
    check_mesos_quorum()
