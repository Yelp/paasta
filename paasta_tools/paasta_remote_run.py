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
import argparse
import json
import logging
import os
import random
import re
import signal
import string
import sys
import traceback
from datetime import datetime

from boto3.session import Session
from pyrsistent import InvariantException
from pyrsistent import PTypeError
from task_processing.metrics import create_counter
from task_processing.metrics import get_metric
from task_processing.plugins.persistence.dynamodb_persistence import DynamoDBPersister
from task_processing.runners.sync import Sync
from task_processing.task_processor import TaskProcessor

from paasta_tools import mesos_tools
from paasta_tools.cli.cmds.remote_run import add_common_args_to_parser
from paasta_tools.cli.cmds.remote_run import add_start_args_to_parser
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.frameworks.native_service_config import load_paasta_native_job_config
from paasta_tools.mesos_tools import get_all_frameworks
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import validate_service_instance

MESOS_TASK_SPACER = '.'


def emit_counter_metric(counter_name, service, instance):
    create_counter(counter_name, {'service': service, 'instance': instance})
    get_metric(counter_name).count(1)


def parse_args(argv):
    parser = argparse.ArgumentParser(description='')
    subs = parser.add_subparsers(
        dest='action',
        help='Subcommands of paasta_remote_run',
    )

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
                "the same way it would behave on a server configured for PaaSTA.",
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
                "Please pass one using '-c'.",
            ),
            sep='\n',
            file=sys.stderr,
        )
        emit_counter_metric('paasta.remote_run.' + args.action + '.failed', service, 'UNKNOWN')
        sys.exit(1)

    soa_dir = args.yelpsoa_config_root
    instance = args.instance
    if instance is None:
        instance_type = 'adhoc'
        instance = 'remote'
    else:
        try:
            instance_type = validate_service_instance(
                service, instance, cluster, soa_dir,
            )
        except NoConfigurationForServiceError as e:
            paasta_print(e)
            emit_counter_metric('paasta.remote_run.' + args.action + '.failed', service, instance)
            sys.exit(1)

        if instance_type != 'adhoc':
            paasta_print(
                PaastaColors.red(
                    (
                        "Please use instance declared in adhoc.yaml for use "
                        "with remote-run, {} is declared as {}"
                    ).format(instance, instance_type),
                ),
            )
            emit_counter_metric('paasta.remote_run.' + args.action + '.failed', service, instance)
            sys.exit(1)

    return (
        system_paasta_config,
        service,
        cluster,
        soa_dir,
        instance,
        instance_type,
    )


def paasta_to_task_config_kwargs(
    service,
    instance,
    native_job_config,
    offer_timeout,
    system_paasta_config,
    config_overrides=None,
    docker_image=None,
):

    if docker_image is None:
        docker_image = native_job_config.get_docker_url()
    docker_parameters = [
        {'key': param['key'], 'value': param['value']}
        for param in native_job_config.format_docker_parameters()
    ]
    # network = native_job_config.get_mesos_network_mode()

    docker_volumes = native_job_config.get_volumes(
        system_volumes=system_paasta_config.get_volumes(),
    )
    volumes = [
        {
            'container_path': volume['containerPath'],
            'host_path': volume['hostPath'],
            'mode': volume['mode'].upper(),
        }
        for volume in docker_volumes
    ]
    cmd = native_job_config.get_cmd()
    uris = system_paasta_config.get_dockercfg_location()
    cpus = native_job_config.get_cpus()
    mem = native_job_config.get_mem()
    disk = native_job_config.get_disk(10)
    gpus = native_job_config.get_gpus()

    kwargs = {
        'image': str(docker_image),
        'cpus': cpus,
        'mem': float(mem),
        'disk': float(disk),
        'volumes': volumes,
        # 'ports': None,
        # 'cap_add'
        # 'ulimit'
        'uris': [uris],
        'docker_parameters': docker_parameters,
        'containerizer': 'DOCKER',
        'environment': native_job_config.get_env_dictionary(),
        'offer_timeout': offer_timeout,
    }
    if cmd:
        kwargs['cmd'] = cmd
    if gpus > 0:
        kwargs['gpus'] = int(gpus)
        kwargs['containerizer'] = 'MESOS'

    config_hash = get_config_hash(
        kwargs,
        force_bounce=native_job_config.get_force_bounce(),
    )

    kwargs['name'] = str(compose_job_id(
        service,
        instance,
        git_hash=get_code_sha_from_dockerurl(docker_image),
        config_hash=config_hash,
        spacer=MESOS_TASK_SPACER,
    ))

    return kwargs


def build_executor_stack(
    # TODO: rename to registry?
    processor,
    service,
    instance,
    cluster,
    role,
    pool,
    # TODO: move run_id into task identifier?
    run_id,
    system_paasta_config,
    framework_staging_timeout,
    region,
):

    cluster_fqdn = system_paasta_config.get_cluster_fqdn_format().format(cluster=cluster)
    mesos_address = '{}:{}'.format(
        mesos_tools.find_mesos_leader(cluster_fqdn),
        mesos_tools.MESOS_MASTER_PORT,
    )

    # TODO: implement DryRunExecutor?
    taskproc_config = system_paasta_config.get_taskproc()

    MesosExecutor = processor.executor_cls('mesos')
    mesos_executor = MesosExecutor(
        role=role,
        pool=pool,
        principal=taskproc_config.get('principal'),
        secret=taskproc_config.get('secret'),
        mesos_address=mesos_address,
        framework_name="paasta-remote {} {} {}".format(
            compose_job_id(service, instance),
            datetime.utcnow().strftime('%Y%m%d%H%M%S%f'),
            run_id,
        ),
        framework_staging_timeout=framework_staging_timeout,
        initial_decline_delay=0.5,
    )

    task_logging_executor = processor.executor_from_config(
        provider='logging',
        provider_config={
            'downstream_executor': mesos_executor,
        },
    )

    credentials_file = taskproc_config.get('boto_credential_file')
    if credentials_file:
        with open(credentials_file) as f:
            credentials = json.loads(f.read())
    else:
        raise ValueError("Required aws credentials")

    if not region:
        region = taskproc_config.get('aws_region')

    endpoint = taskproc_config.get('dynamodb_endpoint')
    session = Session(
        region_name=region,
        aws_access_key_id=credentials['accessKeyId'],
        aws_secret_access_key=credentials['secretAccessKey'],
    )

    StatefulExecutor = processor.executor_cls(provider='stateful')
    stateful_executor = StatefulExecutor(
        downstream_executor=task_logging_executor,
        persister=DynamoDBPersister(
            table_name="taskproc_events_%s" % cluster,
            session=session,
            endpoint_url=endpoint,
        ),
    )

    return stateful_executor


def remote_run_start(args):

    system_paasta_config, service, cluster, \
        soa_dir, instance, instance_type = extract_args(args)
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
            random.choice(string.ascii_uppercase + string.digits)
            for _ in range(8)
        )
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

    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    processor.load_plugin(provider_module='task_processing.plugins.stateful')

    MesosExecutor = processor.executor_cls(provider='mesos')

    native_job_config = load_paasta_native_job_config(
        service,
        instance,
        cluster,
        soa_dir=soa_dir,
        instance_type=instance_type,
        config_overrides=overrides_dict,
        load_deployments=not args.docker_image,
    )
    try:
        task_config = MesosExecutor.TASK_CONFIG_INTERFACE(
            **paasta_to_task_config_kwargs(
                service=service,
                instance=instance,
                system_paasta_config=system_paasta_config,
                native_job_config=native_job_config,
                config_overrides=overrides_dict,
                docker_image=args.docker_image,
                offer_timeout=args.staging_timeout,
            ),
        )
    except InvariantException as e:
        if len(e.missing_fields) > 0:
            paasta_print(
                PaastaColors.red(
                    "Mesos task config is missing following fields: {}".format(
                        ', '.join(e.missing_fields),
                    ),
                ),
            )
        elif len(e.invariant_errors) > 0:
            paasta_print(
                PaastaColors.red(
                    "Mesos task config is failing following checks: {}".format(
                        ', '.join(str(ie) for ie in e.invariant_errors),
                    ),
                ),
            )
        else:
            paasta_print(
                PaastaColors.red(f"Mesos task config error: {e}"),
            )
        traceback.print_exc()
        emit_counter_metric('paasta.remote_run.start.failed', service, instance)
        sys.exit(1)
    except PTypeError as e:
        paasta_print(
            PaastaColors.red(
                f"Mesos task config is failing a type check: {e}",
            ),
        )
        traceback.print_exc()
        emit_counter_metric('paasta.remote_run.start.failed', service, instance)
        sys.exit(1)

    def handle_interrupt(_signum, _frame):
        paasta_print(
            PaastaColors.red("Signal received, shutting down scheduler."),
        )
        if runner is not None:
            runner.stop()
        if _signum == signal.SIGTERM:
            sys.exit(143)
        else:
            sys.exit(1)
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    default_role = system_paasta_config.get_remote_run_config().get('default_role')
    assert default_role

    try:
        executor_stack = build_executor_stack(
            processor=processor,
            service=service,
            instance=instance,
            role=native_job_config.get_role() or default_role,
            pool=native_job_config.get_pool(),
            cluster=cluster,
            run_id=run_id,
            system_paasta_config=system_paasta_config,
            framework_staging_timeout=args.staging_timeout,
            region=args.aws_region,
        )
        runner = Sync(executor_stack)

        terminal_event = runner.run(task_config)
        runner.stop()
    except (Exception, ValueError) as e:
        paasta_print("Except while running executor stack: %s", e)
        traceback.print_exc()
        emit_counter_metric('paasta.remote_run.start.failed', service, instance)
        sys.exit(1)

    if terminal_event.success:
        paasta_print("Task finished successfully")
        sys.exit(0)
    else:
        paasta_print(
            PaastaColors.red(f"Task failed: {terminal_event.raw}"),
        )
        # This is not necessarily an infrastructure failure. It may just be a
        # application failure.
        emit_counter_metric('paasta.remote_run.start.failed', service, instance)
        sys.exit(1)


# TODO: reimplement using build_executor_stack and task uuid instead of run_id
def remote_run_stop(args):
    _, service, cluster, _, instance, _ = extract_args(args)
    if args.framework_id is None and args.run_id is None:
        paasta_print(PaastaColors.red("Must provide either run id or framework id to stop."))
        emit_counter_metric('paasta.remote_run.stop.failed', service, instance)
        sys.exit(1)

    frameworks = [
        f
        for f in get_all_frameworks(active_only=True)
        if re.search(f'^paasta-remote {service}.{instance}', f.name)
    ]
    framework_id = args.framework_id
    if framework_id is None:
        if re.match('\s', args.run_id):
            paasta_print(PaastaColors.red("Run id must not contain whitespace."))
            emit_counter_metric('paasta.remote_run.stop.failed', service, instance)
            sys.exit(1)

        found = [f for f in frameworks if re.search(' %s$' % args.run_id, f.name) is not None]
        if len(found) > 0:
            framework_id = found[0].id
        else:
            paasta_print(PaastaColors.red("Framework with run id %s not found." % args.run_id))
            emit_counter_metric('paasta.remote_run.stop.failed', service, instance)
            sys.exit(1)
    else:
        found = [f for f in frameworks if f.id == framework_id]
        if len(found) == 0:
            paasta_print(
                PaastaColors.red(
                    "Framework id %s does not match any %s.%s remote-run. Check status to find the correct id." %
                    (framework_id, service, instance),
                ),
            )
            emit_counter_metric('paasta.remote_run.stop.failed', service, instance)
            sys.exit(1)

    paasta_print("Tearing down framework %s." % framework_id)
    mesos_master = get_mesos_master()
    teardown = mesos_master.teardown(framework_id)
    if teardown.status_code == 200:
        paasta_print(PaastaColors.green("OK"))
    else:
        paasta_print(teardown.text)


def remote_run_frameworks():
    return get_all_frameworks(active_only=True)


def remote_run_filter_frameworks(service, instance, frameworks=None):
    if frameworks is None:
        frameworks = remote_run_frameworks()

    prefix = f"paasta-remote {service}.{instance}"
    return [f for f in frameworks if f.name.startswith(prefix)]


def remote_run_list_report(service, instance, cluster, frameworks=None):
    filtered = remote_run_filter_frameworks(
        service, instance, frameworks=frameworks,
    )
    filtered.sort(key=lambda x: x.name)
    for f in filtered:
        launch_time, run_id = re.match(
            'paasta-remote [^\s]+ (\w+) (\w+)', f.name,
        ).groups()
        paasta_print("Launch time: %s, run id: %s, framework id: %s" %
                     (launch_time, run_id, f.id))
    if len(filtered) > 0:
        paasta_print(
            (
                "Use `paasta remote-run stop -s {} -c {} -i {} [-R <run id> "
                "| -F <framework id>]` to stop."
            ).format(service, cluster, instance),
        )
    else:
        paasta_print("Nothing found.")


def remote_run_list(args, frameworks=None):
    _, service, cluster, _, instance, _ = extract_args(args)
    return remote_run_list_report(
        service=service,
        instance=instance,
        cluster=cluster,
        frameworks=frameworks,
    )


def main(argv):
    args = parse_args(argv)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)
    # elif args.quiet:
    #     logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.WARNING)

    actions = {
        'start': remote_run_start,
        'stop': remote_run_stop,
        'list': remote_run_list,
    }
    actions[args.action](args)


if __name__ == '__main__':
    main(sys.argv[1:])
