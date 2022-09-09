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
synapse_host = "169.254.255.254"
synapse_haproxy_url_format = system_paasta_config.get_synapse_haproxy_url_format()
host_ip = os.environ["PAASTA_POD_IP"]
port = sys.argv[1]
services = sys.argv[2:]

###############################################################
#
# This file is used in the hacheck sidecar, make sure to update `check_smartstack_up.sh`
# when changing this file
#
###############################################################

if are_services_up_on_ip_port(
    synapse_host=synapse_host,
    synapse_port=synapse_port,
    synapse_haproxy_url_format=synapse_haproxy_url_format,
    services=services,
    host_ip=host_ip,
    host_port=int(port),
):
    sys.exit(0)
else:
    print(
        f"Could not find backend {host_ip}:{port} for service {services} "
        f"on Synapse at {synapse_host}:{synapse_port}"
    )
    sys.exit(1)
