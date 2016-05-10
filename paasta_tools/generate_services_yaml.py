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

import yaml

from paasta_tools.marathon_tools import get_all_namespaces
from paasta_tools.utils import atomic_file_write

YOCALHOST = '169.254.255.254'


def generate_configuration():
    service_data = get_all_namespaces()

    config = {}
    for (name, data) in service_data:
        proxy_port = data.get('proxy_port')
        if proxy_port is None:
            continue
        config[name] = {
            'host': YOCALHOST,
            'port': int(proxy_port),
        }

    return config


def main():
    if len(sys.argv) != 2:
        print >>sys.stderr, "Usage: %s <output_path>"
        sys.exit(1)

    output_path = sys.argv[1]
    configuration = generate_configuration()

    with atomic_file_write(output_path) as fp:
        yaml.dump(configuration,
                  fp,
                  indent=2,
                  explicit_start=True,
                  default_flow_style=False)


if __name__ == '__main__':
    main()
