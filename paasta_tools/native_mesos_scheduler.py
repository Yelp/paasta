#!/usr/bin/env python
# Without this, the import of mesos.interface breaks because paasta_tools.mesos exists
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import sys
import time
from time import sleep

from paasta_tools.frameworks.native_scheduler import create_driver
from paasta_tools.frameworks.native_scheduler import get_paasta_native_jobs_for_cluster
from paasta_tools.frameworks.native_scheduler import NativeScheduler
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config


def parse_args(argv):
    parser = argparse.ArgumentParser(description='Runs native paasta mesos scheduler.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR", default=DEFAULT_SOA_DIR)
    parser.add_argument('--stay-alive-seconds', dest="stay_alive_seconds", type=int, default=300)
    parser.add_argument('--periodic-interval', dest="periodic_interval", type=int, default=30)
    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    drivers = []
    schedulers = []
    for service, instance in get_paasta_native_jobs_for_cluster(cluster=cluster, soa_dir=args.soa_dir):
        scheduler = NativeScheduler(
            service_name=service,
            instance_name=instance,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            soa_dir=args.soa_dir,
        )
        schedulers.append(scheduler)

        driver = create_driver(
            framework_name="paasta %s" % compose_job_id(service, instance),
            scheduler=scheduler,
            system_paasta_config=system_paasta_config
        )
        driver.start()
        drivers.append(driver)

    end_time = time.time() + args.stay_alive_seconds
    while time.time() < end_time:
        sleep(args.periodic_interval)
        for scheduler, driver in zip(schedulers, drivers):
            scheduler.periodic(driver)

    return schedulers


if __name__ == '__main__':
    main(sys.argv[1:])
