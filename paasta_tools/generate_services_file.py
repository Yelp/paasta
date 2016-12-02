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
A simple script to enumerate all smartstack namespaces and output
a /etc/services compatible file
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import sys

import service_configuration_lib

from paasta_tools.marathon_tools import get_all_namespaces_for_service
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import paasta_print


def get_service_lines_for_service(service):
    lines = []
    config = service_configuration_lib.read_service_configuration(service)
    port = config.get('port', None)
    description = config.get('description', "No description")

    if port is not None:
        lines.append("%s\t%d/tcp\t# %s" % (service, port, description))

    for namespace, config in get_all_namespaces_for_service(service, full_name=False):
        proxy_port = config.get('proxy_port', None)
        if proxy_port is not None:
            lines.append("%s\t%d/tcp\t# %s" % (compose_job_id(service, namespace), proxy_port, description))
    return lines


def main():
    strings = []
    for service in sorted(os.listdir(DEFAULT_SOA_DIR)):
        strings.extend(get_service_lines_for_service(service))
    paasta_print("\n".join(strings))
    sys.exit(0)


if __name__ == "__main__":
    main()
