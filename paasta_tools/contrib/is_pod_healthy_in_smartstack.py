#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
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
import os
import sys

from paasta_tools.smartstack_tools import are_services_up_on_ip_port
from paasta_tools.utils import load_system_paasta_config

system_paasta_config = load_system_paasta_config()
synapse_port = system_paasta_config.get_synapse_port()
synapse_host = '169.254.255.254'
synapse_haproxy_url_format = system_paasta_config.get_synapse_haproxy_url_format()
host_ip = os.environ['PAASTA_POD_IP']
services = sys.argv[1:]

if are_services_up_on_ip_port(
    synapse_host=synapse_host,
    synapse_port=synapse_port,
    synapse_haproxy_url_format=synapse_haproxy_url_format,
    services=services,
    host_ip=host_ip,
    host_port=8888,
):
    sys.exit(0)
else:
    sys.exit(1)
