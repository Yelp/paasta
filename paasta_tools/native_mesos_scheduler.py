#!/usr/bin/env python
import argparse
import sys
import time
from time import sleep
from typing import Optional
from typing import Sequence
from typing import Tuple

from paasta_tools import mesos_tools
from paasta_tools.frameworks.native_scheduler import create_driver
from paasta_tools.frameworks.native_scheduler import get_paasta_native_jobs_for_cluster
from paasta_tools.frameworks.native_scheduler import load_paasta_native_job_config
from paasta_tools.frameworks.native_scheduler import NativeScheduler
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaNotConfiguredError


def parse_args(argv):
    parser = argparse.ArgumentParser(description='Runs native paasta mesos scheduler.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR", default=DEFAULT_SOA_DIR)
    parser.add_argument('--stay-alive-seconds', dest="stay_alive_seconds", type=int, default=300)
    parser.add_argument('--periodic-interval', dest="periodic_interval", type=int, default=30)
    parser.add_argument('--staging-timeout', dest="staging_timeout", type=float, default=60)
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
            staging_timeout=args.staging_timeout,
            system_paasta_config=system_paasta_config,
            soa_dir=args.soa_dir,
        )
        schedulers.append(scheduler)

        driver = create_driver(
            framework_name="paasta_native %s" % compose_job_id(service, instance),
            scheduler=scheduler,
            system_paasta_config=system_paasta_config,
        )
        driver.start()
        drivers.append(driver)

    end_time = time.time() + args.stay_alive_seconds
    while time.time() < end_time:
        sleep(args.periodic_interval)
        for scheduler, driver in zip(schedulers, drivers):
            scheduler.periodic(driver)

    return schedulers


def get_app_id_and_task_uuid_from_executor_id(executor_id):
    """Parse the paasta_native executor ID and return the (app id, task uuid)"""
    return executor_id.rsplit('.', 1)


def parse_service_instance_from_executor_id(task_id):
    app_id, task_uuid = get_app_id_and_task_uuid_from_executor_id(task_id)
    (srv_name, srv_instance, _, __) = decompose_job_id(app_id)
    return srv_name, srv_instance


def paasta_native_services_running_here(hostname=None, framework_id=None):
    """See what paasta_native services are being run by a mesos-slave on this host.
    :returns: A list of triples of (service, instance, port)

    :param hostname: query the mesos slave on this hostname.
    :param framework_id: If specified, return info only for tasks belonging to this framework id.
    """

    def framework_filter(fw):
        return fw['name'].startswith('paasta_native ') and (framework_id is None or fw['id'] == framework_id)

    return mesos_tools.mesos_services_running_here(
        framework_filter=framework_filter,
        parse_service_instance_from_executor_id=parse_service_instance_from_executor_id,
        hostname=hostname,
    )


def get_paasta_native_services_running_here_for_nerve(
    cluster: Optional[str],
    soa_dir: str,
    hostname: Optional[str] = None,
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    if not cluster:
        try:
            system_paasta_config = load_system_paasta_config()
            cluster = system_paasta_config.get_cluster()
        # In the cases where there is *no* cluster or in the case
        # where there isn't a Paasta configuration file at *all*, then
        # there must be no native services running here, so we catch
        # these custom exceptions and return [].
        except (PaastaNotConfiguredError):
            return []
        if not system_paasta_config.get_register_native_services():
            return []
    # When a cluster is defined in mesos, let's iterate through paasta_native services
    paasta_native_services = paasta_native_services_running_here(hostname=hostname)
    nerve_list = []
    for name, instance, port in paasta_native_services:
        try:
            job_config = load_paasta_native_job_config(
                service=name,
                instance=instance,
                cluster=cluster,
                load_deployments=False,
                soa_dir=soa_dir,
            )
            for registration in job_config.get_registrations():
                reg_service, reg_namespace, _, __ = decompose_job_id(registration)
                nerve_dict = load_service_namespace_config(
                    service=reg_service, namespace=reg_namespace, soa_dir=soa_dir,
                )
                if not nerve_dict.is_in_smartstack():
                    continue
                nerve_dict['port'] = port
                nerve_list.append((registration, nerve_dict))
        except KeyError:
            continue  # SOA configs got deleted for this app, it'll get cleaned up
    return nerve_list


if __name__ == '__main__':
    main(sys.argv[1:])
