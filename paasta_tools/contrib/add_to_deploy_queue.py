#!/usr/bin/env python
# Copyright 2015-2020 Yelp Inc.
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
import sys
import time

from kazoo.client import KazooClient
from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.deployd.queue import ZKDelayDeadlineQueue
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import validate_service_instance


def parse_args(default_bounce_by_delay_secs):
    parser = argparse.ArgumentParser(
        description="Add a service instance to the deploy queue",
    )
    parser.add_argument(
        "--bounce-by-delay-secs",
        help="Number of seconds to wait before considering this entry late. Default: %(default)s",
        dest="bounce_by_delay_secs",
        type=float,
        default=default_bounce_by_delay_secs,
    )
    parser.add_argument(
        "service_instance",
        help="The service.instance to add to the deploy queue",
        type=str,
    )
    return parser.parse_args()


def main():
    system_paasta_config = load_system_paasta_config()
    args = parse_args(system_paasta_config.get_deployd_startup_bounce_deadline())

    service, instance = args.service_instance.split(".", 1)
    try:
        validate_service_instance(
            service,
            instance,
            cluster=system_paasta_config.get_cluster(),
            soa_dir=DEFAULT_SOA_DIR,
        )
    except NoConfigurationForServiceError as e:
        print(PaastaColors.red(str(e)))
        sys.exit(1)

    service_instance = ServiceInstance(
        service=service,
        instance=instance,
        bounce_by=time.time() + args.bounce_by_delay_secs,
        wait_until=time.time(),
        watcher="manually_added",
        failures=0,
        enqueue_time=time.time(),
        bounce_start_time=time.time(),
    )

    zk_client = KazooClient(hosts=system_paasta_config.get_zk_hosts())
    zk_client.start()
    queue = ZKDelayDeadlineQueue(client=zk_client)

    queue.put(service_instance)


if __name__ == "__main__":
    sys.exit(main())
