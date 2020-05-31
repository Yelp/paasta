#!/usr/bin/env python
import argparse
import sys
import time

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import marathon_tools
from paasta_tools.setup_marathon_job import do_bounce
from paasta_tools.setup_marathon_job import get_tasks_by_state
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import load_system_paasta_config


def parse_args():
    parser = argparse.ArgumentParser(
        description="""This script attempts to gracefully drain and kill a marathon app.
        It is intended for use in emergencies when the regular bounce script can't proceed,
        and needs to kill a specific app to get going."""
    )
    parser.add_argument("appname", help="the app that will be drained")
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=marathon_tools.DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    return parser.parse_args()


def kill_marathon_app(full_appid, cluster, client, soa_dir):
    service, instance, _, __ = (
        s.replace("--", "_") for s in decompose_job_id(full_appid)
    )
    service_instance_config = marathon_tools.load_marathon_service_config(
        service=service, instance=instance, cluster=cluster, soa_dir=soa_dir
    )
    complete_config = service_instance_config.format_marathon_app_dict()
    registrations = service_instance_config.get_registrations()
    service_namespace_config = marathon_tools.load_service_namespace_config(
        service=service, namespace=registrations[0]
    )
    drain_method = drain_lib.get_drain_method(
        service_instance_config.get_drain_method(service_namespace_config),
        service=service,
        instance=instance,
        registrations=registrations,
        drain_method_params=service_instance_config.get_drain_method_params(
            service_namespace_config
        ),
    )

    bounce_func = bounce_lib.get_bounce_method_func("down")

    while marathon_tools.is_app_id_running(app_id=full_appid, client=client):
        app_to_kill = client.get_app(full_appid)
        (
            old_app_live_happy_tasks,
            old_app_live_unhappy_tasks,
            old_app_draining_tasks,
            old_app_at_risk_tasks,
        ) = get_tasks_by_state(
            other_apps=[app_to_kill],
            drain_method=drain_method,
            service=service,
            nerve_ns=registrations[0],
            bounce_health_params=service_instance_config.get_bounce_health_params(
                service_namespace_config
            ),
        )
        do_bounce(
            bounce_func=bounce_func,
            drain_method=drain_method,
            config=complete_config,
            new_app_running="",
            happy_new_tasks=[],
            old_app_live_happy_tasks=old_app_live_happy_tasks,
            old_app_live_unhappy_tasks=old_app_live_unhappy_tasks,
            old_app_draining_tasks=old_app_draining_tasks,
            old_app_at_risk_tasks=old_app_at_risk_tasks,
            serviceinstance=f"{service}.{instance}",
            bounce_method="down",
            service=service,
            cluster=cluster,
            instance=instance,
            marathon_jobid=full_appid,
            client=client,
            soa_dir=soa_dir,
        )

        print("Sleeping for 10 seconds to give the tasks time to drain")
        time.sleep(10)

    print(f"Successfully killed {full_appid}")


def main():
    exit_code = 1
    args = parse_args()
    full_appid = args.appname.lstrip("/")

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    clients = marathon_tools.get_list_of_marathon_clients(
        system_paasta_config=system_paasta_config
    )

    for client in clients:
        if marathon_tools.is_app_id_running(app_id=full_appid, client=client):
            kill_marathon_app(
                full_appid=full_appid,
                cluster=cluster,
                client=client,
                soa_dir=args.soa_dir,
            )
            exit_code = 0

    if exit_code:
        print(f"Couldn't find an app named {full_appid}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
