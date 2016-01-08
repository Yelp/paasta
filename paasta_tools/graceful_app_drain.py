#!/usr/bin/env python

import argparse
import sys

import service_configuration_lib
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools import marathon_tools
from paasta_tools import drain_lib
from paasta_tools import bounce_lib
from paasta_tools.utils import decompose_job_id
from paasta_tools.setup_marathon_job import do_bounce, get_old_live_draining_tasks
from paasta_tools.utils import load_system_paasta_config
from marathon.exceptions import MarathonHttpError


def parse_args():
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument(
        'appname',
        help="the app that will be drained",
    )#.completer = lazy_choices_completer()
    parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=service_configuration_lib.DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    full_appid = args.appname
    soa_dir = args.soa_dir
    marathon_config = marathon_tools.load_marathon_config()
    client = marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password(),
    )

    if not is_app_id_running(app_id=full_appid, client=client):
        print("Couldn't find an app named {0}".format(full_appid))
        quit()

    service, instance, _, __ = (s.replace('--', '_') for s in decompose_job_id(full_appid))
    short_appid = marathon_tools.format_job_id(service, instance)
    complete_config = marathon_tools.create_complete_config(service, instance, marathon_config)
    cluster = load_system_paasta_config().get_cluster()
    service_instance_config = marathon_tools.load_marathon_service_config(
        service,
        instance,
        cluster,
        soa_dir=soa_dir,
    )
    nerve_ns = service_instance_config.get_nerve_namespace()
    service_namespace_config = marathon_tools.load_service_namespace_config(service=service, namespace=nerve_ns)
    drain_method = drain_lib.get_drain_method(
        service_instance_config.get_drain_method(service_namespace_config),
        service=service,
        instance=instance,
        nerve_ns=nerve_ns,
        drain_method_params=service_instance_config.get_drain_method_params(service_namespace_config),
    )

    bounce_func = bounce_lib.get_bounce_method_func('down')

    while is_app_id_running(app_id=full_appid, client=client):
        app_to_kill = client.get_app(full_appid)
        try:
            old_app_live_tasks, old_app_draining_tasks = get_old_live_draining_tasks([app_to_kill], drain_method)
            with bounce_lib.bounce_lock_zookeeper(short_appid):
                do_bounce(
                    bounce_func=bounce_func,
                    drain_method=drain_method,
                    config=complete_config,
                    new_app_running='',
                    happy_new_tasks=[],
                    old_app_live_tasks=old_app_live_tasks,
                    old_app_draining_tasks=old_app_draining_tasks,
                    serviceinstance="{0}.{1}".format(service, instance),
                    bounce_method='down',
                    service=service,
                    cluster=cluster,
                    instance=instance,
                    marathon_jobid=full_appid,
                    client=client,
                    soa_dir=soa_dir,
                )
        except MarathonHttpError:
            break

    print("Sucessfully killed {0}".format(full_appid))

if __name__ == '__main__':
    main()
