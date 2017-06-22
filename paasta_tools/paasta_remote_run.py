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
import os
import random
import re
import signal
import string
import sys
from datetime import datetime

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.sync import Sync

from paasta_tools import mesos_tools
from paasta_tools.cli.cmds.remote_run import add_common_args_to_parser
from paasta_tools.cli.cmds.remote_run import add_start_args_to_parser
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.frameworks.native_service_config import load_paasta_native_job_config
from paasta_tools.mesos_tools import get_all_frameworks
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_docker_url
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import validate_service_instance

MESOS_TASK_SPACER = '.'


def parse_args(argv):
    parser = argparse.ArgumentParser(description='')
    subs = parser.add_subparsers(dest='action', help='Subcommands of paasta_remote_run')

    start_parser = subs.add_parser('start', help='Start task')
    add_start_args_to_parser(start_parser)
    add_common_args_to_parser(start_parser)
    start_parser.add_argument(
        '-X', '--constraints-json',
        help=('Mesos constraints JSON'),
        required=False,
        default=None,
    )

    stop_parser = subs.add_parser('stop', help='Stop task')
    add_common_args_to_parser(stop_parser)
    stop_parser.add_argument(
        '-F', '--framework-id',
        help=('ID of framework to stop'),
        required=False,
        default=None,
    )

    list_parser = subs.add_parser('list', help='List tasks')
    add_common_args_to_parser(list_parser)

    return parser.parse_args(argv)


def extract_args(args):
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
        os._exit(1)

    soa_dir = args.yelpsoa_config_root
    instance = args.instance
    if instance is None:
        instance_type = 'adhoc'
        instance = 'remote'
    else:
        instance_type = validate_service_instance(service, instance, cluster, soa_dir)

    return (system_paasta_config, service, cluster, soa_dir, instance, instance_type)


def paasta_to_task_config_kwargs(
        service,
        instance,
        cluster,
        system_paasta_config,
        instance_type='paasta_native',
        soa_dir=DEFAULT_SOA_DIR,
        config_overrides=None):
    native_job_config = load_paasta_native_job_config(
        service,
        instance,
        cluster,
        soa_dir=soa_dir,
        instance_type=instance_type,
        config_overrides=config_overrides
    )

    image = get_docker_url(
        system_paasta_config.get_docker_registry(),
        native_job_config.get_docker_image()
    )
    docker_parameters = [
        {'key': param['key'], 'value': param['value']}
        for param in native_job_config.format_docker_parameters()
    ]
    # network = native_job_config.get_mesos_network_mode()

    docker_volumes = native_job_config.get_volumes(
        system_volumes=system_paasta_config.get_volumes()
    )
    volumes = [
        {
            'container_path': volume['containerPath'],
            'host_path': volume['hostPath'],
            'mode': volume['mode'].upper(),
        }
        for volume in docker_volumes
    ]
    cmd = native_job_config.get_cmd()[0]
    uris = system_paasta_config.get_dockercfg_location()
    cpus = native_job_config.get_cpus()
    mem = native_job_config.get_mem()
    disk = native_job_config.get_disk(10)

    kwargs = {
        'image': str(image),
        'cmd': cmd,
        'cpus': cpus,
        'mem': float(mem),
        'disk': float(disk),
        'volumes': volumes,
        # 'ports': None,
        # 'cap_add'
        # 'ulimit'
        'uris': [uris],
        'docker_parameters': docker_parameters
    }

    config_hash = get_config_hash(
        kwargs,
        force_bounce=native_job_config.get_force_bounce(),
    )

    kwargs['name'] = str(compose_job_id(
        service,
        # TODO: where do we stick "remote-run"?
        instance,
        git_hash=get_code_sha_from_dockerurl(image),
        config_hash=config_hash,
        spacer=MESOS_TASK_SPACER,
    ))

    return kwargs


def remote_run_start(args):
    system_paasta_config, service, cluster, soa_dir, instance, instance_type = extract_args(args)
    overrides_dict = {}

    constraints_json = args.constraints_json
    if constraints_json:
        try:
            constraints = json.loads(constraints_json)
        except Exception as e:
            paasta_print("Error while parsing constraints: %s", e)

        if constraints:
            overrides_dict['constraints'] = constraints

    if args.cmd:
        overrides_dict['cmd'] = args.cmd

    if args.instances:
        overrides_dict['instances'] = args.instances

    run_id = args.run_id
    if run_id is None:
        run_id = ''.join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        paasta_print("Assigned random run-id: %s" % run_id)

    if args.detach:
        paasta_print("Running in background")
        if os.fork() > 0:
            return
        os.setsid()
        if os.fork() > 0:
            return
        sys.stdout = open('/dev/null', 'w')
        sys.stderr = open('/dev/null', 'w')

    paasta_print('Scheduling a task on Mesos')

    mesos_address = '{}:{}'.format(
        mesos_tools.get_mesos_leader(), mesos_tools.MESOS_MASTER_PORT
    )

    paasta_config = system_paasta_config.get_paasta_native_config()
    # TODO: implement DryRunExecutor?
    executor = MesosExecutor(
        role='*',
        principal=paasta_config['principal'],
        secret=paasta_config['secret'],
        mesos_address=mesos_address,
        framework_name="paasta-remote %s %s %s" % (
            compose_job_id(service, instance),
            datetime.utcnow().strftime('%Y%m%d%H%M%S%f'),
            run_id
        ),
        framework_staging_timeout=args.staging_timeout
    )
    runner = Sync(executor)

    def handle_interrupt(_signum, _frame):
        paasta_print(PaastaColors.red("Signal received, shutting down scheduler."))
        runner.stop()
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    task_config = MesosExecutor.TASK_CONFIG_INTERFACE(
        **paasta_to_task_config_kwargs(
            service,
            instance,
            cluster,
            system_paasta_config,
            instance_type=instance_type,
            soa_dir=soa_dir,
            config_overrides=overrides_dict,
        )
    )
    runner.run(task_config)
    runner.stop()


def remote_run_stop(args):
    _, service, cluster, _, instance, _ = extract_args(args)
    if args.framework_id is None and args.run_id is None:
        paasta_print(PaastaColors.red("Must provide either run id or framework id to stop."))
        os._exit(1)

    frameworks = [
        f
        for f in get_all_frameworks(active_only=True)
        if re.search('^paasta-remote %s.%s' % (service, instance), f.name)
    ]
    framework_id = args.framework_id
    if framework_id is None:
        if re.match('\s', args.run_id):
            paasta_print(PaastaColors.red("Run id must not contain whitespace."))
            os._exit(1)

        found = [f for f in frameworks if re.search(' %s$' % args.run_id, f.name) is not None]
        if len(found) > 0:
            framework_id = found[0].id
        else:
            paasta_print(PaastaColors.red("Framework with run id %s not found." % args.run_id))
            os._exit(1)
    else:
        found = [f for f in frameworks if f.id == framework_id]
        if len(found) == 0:
            paasta_print(
                PaastaColors.red(
                    "Framework id %s does not match any %s.%s remote-run. Check status to find the correct id." %
                    (framework_id, service, instance)))
            os._exit(1)

    paasta_print("Tearing down framework %s." % framework_id)
    mesos_master = get_mesos_master()
    teardown = mesos_master.teardown(framework_id)
    if teardown.status_code == 200:
        paasta_print(PaastaColors.green("OK"))
    else:
        paasta_print(teardown.text)


def remote_run_list(args):
    _, service, cluster, _, instance, _ = extract_args(args)
    frameworks = get_all_frameworks(active_only=True)
    prefix = "paasta-remote %s.%s" % (service, instance)
    filtered = [f for f in frameworks if f.name.startswith(prefix)]
    filtered.sort(key=lambda x: x.name)
    for f in filtered:
        launch_time, run_id = re.match('paasta-remote [^\s]+ (\w+) (\w+)', f.name).groups()
        paasta_print("Launch time: %s, run id: %s, framework id: %s" %
                     (launch_time, run_id, f.id))
    if len(filtered) > 0:
        paasta_print(
            "Use `paasta remote-run stop -s %s -c %s -i %s [-R <run id> | -F <framework id>]` to stop." %
            (service, cluster, instance))
    else:
        paasta_print("Nothing found.")


def main(argv):
    args = parse_args(argv)
    actions = {
        'start': remote_run_start,
        'stop': remote_run_stop,
        'list': remote_run_list
    }
    actions[args.action](args)


if __name__ == '__main__':
    main(sys.argv[1:])
