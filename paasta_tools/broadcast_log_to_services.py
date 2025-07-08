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

from paasta_tools.kubernetes_tools import get_all_kubernetes_services_running_here
from paasta_tools.mesos_tools import MesosSlaveConnectionError
from paasta_tools.tron_tools import tron_jobs_running_here
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config


def broadcast_log_all_services_running_here(line: str, soa_dir=DEFAULT_SOA_DIR) -> None:
    """Log a line of text to paasta logs of all services running on this host.

    :param line: text to log
    """
    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    services = get_all_services_running_here(cluster, soa_dir)
    for service, instance, _ in services:
        _log(
            line=line,
            service=service,
            instance=instance,
            component="monitoring",
            cluster=cluster,
        )


def get_all_services_running_here(cluster, soa_dir):
    try:
        tron_services = tron_jobs_running_here()
    except MesosSlaveConnectionError:
        tron_services = []

    try:
        kubernetes_services = get_all_kubernetes_services_running_here()
    except Exception:
        kubernetes_services = []

    return tron_services + kubernetes_services


def main() -> None:
    broadcast_log_all_services_running_here(sys.stdin.read().strip())


if __name__ == "__main__":
    main()
