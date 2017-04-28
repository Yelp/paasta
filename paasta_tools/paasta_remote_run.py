#!/usr/bin/env python
# Copyright 2015-2017 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import json
import signal
import sys
import threading
from datetime import datetime

from paasta_tools.cli.cmds.remote_run import add_remote_run_args
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.frameworks.adhoc_scheduler import AdhocScheduler
from paasta_tools.frameworks.native_scheduler import create_driver
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import validate_service_instance


def parse_args(argv):
    parser = argparse.ArgumentParser(description='')
    add_remote_run_args(parser)
    parser.add_argument(
        '-X', '--constraints-json',
        help=('Mesos constraints JSON'),
        required=False,
        default=None,
    )
    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        paasta_print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This indicates"
                "PaaSTA is not configured locally on this host, and remote-run may not behave"
                "the same way it would behave on a server configured for PaaSTA."
            ),
            sep='\n',
        )
        system_paasta_config = SystemPaastaConfig({"volumes": []}, '/etc/paasta')

    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    cluster = args.cluster or system_paasta_config.get_local_run_config().get('default_cluster', None)

    if not cluster:
        paasta_print(
            PaastaColors.red(
                "PaaSTA on this machine has not been configured with a default cluster."
                "Please pass one using '-c'."),
            sep='\n',
            file=sys.stderr,
        )
        return 1

    soa_dir = args.yelpsoa_config_root
    dry_run = args.dry_run
    instance = args.instance
    command = args.cmd

    if instance is None:
        instance_type = 'adhoc'
        instance = 'remote'
    else:
        instance_type = validate_service_instance(service, instance, cluster, soa_dir)

    overrides_dict = {}

    constraints_json = args.constraints_json
    if constraints_json:
        try:
            constraints = json.loads(constraints_json)
        except Exception as e:
            paasta_print("Error while parsing constraints: %s", e)

        if constraints:
            overrides_dict['constraints'] = constraints

    if command:
        overrides_dict['cmd'] = command

    paasta_print('Scheduling a task on Mesos')
    scheduler = AdhocScheduler(
        service_name=service,
        instance_name=instance,
        instance_type=instance_type,
        cluster=cluster,
        system_paasta_config=system_paasta_config,
        soa_dir=soa_dir,
        reconcile_backoff=0,
        dry_run=dry_run,
        staging_timeout=args.staging_timeout,
        service_config_overrides=overrides_dict,
    )
    driver = create_driver(
        framework_name="paasta-remote %s %s" % (
            compose_job_id(service, instance),
            datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
        ),
        scheduler=scheduler,
        system_paasta_config=system_paasta_config
    )

    def handle_interrupt(_signum, _frame):
        paasta_print(PaastaColors.red("Signal received, shutting down scheduler."))
        scheduler.shutdown(driver)
        driver.stop()
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    # driver.run makes the thread uninterruptible
    t = threading.Thread(target=driver.run)
    t.start()
    t.join(float("inf"))


if __name__ == '__main__':
    main(sys.argv[1:])
