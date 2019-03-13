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
import pprint
import random
import re
import signal
import smtplib
import string
import sys
import traceback
from datetime import datetime
from email.message import EmailMessage

from boto3.session import Session
from pyrsistent import InvariantException
from pyrsistent import PTypeError
from task_processing.metrics import create_counter  # noreorder
from task_processing.metrics import get_metric  # noreorder
from task_processing.plugins.persistence.dynamodb_persistence import DynamoDBPersister  # noreorder
from task_processing.runners.sync import Sync  # noreorder
from task_processing.task_processor import TaskProcessor  # noreorder

from paasta_tools import mesos_tools
from paasta_tools.cli.cmds.remote_run import add_list_parser
from paasta_tools.cli.cmds.remote_run import add_start_parser
from paasta_tools.cli.cmds.remote_run import add_stop_parser
from paasta_tools.cli.cmds.remote_run import get_system_paasta_config
from paasta_tools.cli.cmds.remote_run import split_constraints
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.frameworks.native_service_config import load_paasta_native_job_config
from paasta_tools.mesos_tools import get_all_frameworks
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import validate_service_instance

MESOS_TASK_SPACER = '.'
TASKPROC_OFFER_TIMEOUT_RAW = 'Failed due to offer timeout'


def emit_counter_metric(counter_name, service, instance):
    create_counter(counter_name, {'service': service, 'instance': instance})
    get_metric(counter_name).count(1)


def add_debug_args_to_parser(parser):
    parser.add_argument(
        '-y', '--yelpsoa-config-root',
        dest='yelpsoa_config_root',
        help='A directory from which yelpsoa-configs should be read from',
        default=DEFAULT_SOA_DIR,
    )
    parser.add_argument(
        '--debug',
        help='Show debug output',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--aws-region',
        choices=['us-east-1', 'us-west-1', 'us-west-2'],
        help='aws region of the dynamodb state table',
        default=None,  # load later from system paasta configs
    )


def parse_args(argv):
    parser = argparse.ArgumentParser(description='')
    subs = parser.add_subparsers(
        dest='action',
        help='Subcommands of paasta_remote_run',
    )
    action_parsers = dict(
        start=add_start_parser(subs),
        stop=add_stop_parser(subs),
        list=add_list_parser(subs),
    )
    for ap in action_parsers.values():
        add_debug_args_to_parser(ap)

    action_parsers['start'].add_argument(
        '--constraints-json',
        help='Mesos constraints JSON',
        default=None,
    )

    return parser.parse_args(argv)


def extract_args(args):
    system_paasta_config = get_system_paasta_config()
    soa_dir = args.yelpsoa_config_root
    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)

    cluster = args.cluster or \
        system_paasta_config.get_remote_run_config().get('default_cluster', None)
    if not cluster:
        paasta_print(PaastaColors.red(
            "PaaSTA on this machine has not been configured with a default cluster."
            "Please pass one using '-c'.",
        ))
        emit_counter_metric('paasta.remote_run.' + args.action + '.failed', service, 'UNKNOWN')
        sys.exit(1)

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
            paasta_print(PaastaColors.red(
                "Please use instance declared in adhoc.yaml for use "
                f"with remote-run, {instance} is declared as {instance_type}",
            ))
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


def accumulate_config_overrides(args, service, instance):
    """ Although task configs come with defaults values, certain args can
    override them. We accumulate them in a dict here and return them.
    """
    overrides = {}

    # constraint overrides
    constraints = []
    try:
        if args.constraints_json:
            constraints.extend(json.loads(args.constraints_json))
        if args.constraint:
            constraints.extend(split_constraints(args.constraint))
    except Exception as e:
        paasta_print(f"Error while parsing constraints: {e}")
        emit_counter_metric('paasta.remote_run.start.failed', service, instance)
        sys.exit(1)
    if constraints:
        overrides['constraints'] = constraints
    # cmd overrides
    if args.cmd:
        overrides['cmd'] = args.cmd
    # instance count override
    if args.instances:
        overrides['instances'] = args.instances

    return overrides


def generate_run_id(length=8):
    """ Generates a random string of uppercase letters and digits for use as
    a run identifier
    """
    run_id = ''.join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(length)
    )
    paasta_print(f"Generated random run identifier: {run_id}")
    return run_id


def create_framework_name(service, instance, run_id):
    """ Creates a framework name for our task """
    return "paasta-remote {} {} {}".format(
        compose_job_id(service, instance),
        datetime.utcnow().strftime('%Y%m%d%H%M%S%f'),
        run_id,
    )


def create_mesos_executor(
    processor,
    system_paasta_config,
    taskproc_config,
    cluster,
    framework_name,
    framework_staging_timeout,
    role='*',
    pool='default',
):
    """ Create a Mesos executor specific to our cluster """
    MesosExecutor = processor.executor_cls('mesos_task')

    cluster_fqdn = system_paasta_config.get_cluster_fqdn_format().format(cluster=cluster)
    mesos_address = '{}:{}'.format(
        mesos_tools.find_mesos_leader(cluster_fqdn),
        mesos_tools.MESOS_MASTER_PORT,
    )

    return MesosExecutor(
        role=role,
        pool=pool,
        principal=taskproc_config.get('principal'),
        secret=taskproc_config.get('secret'),
        mesos_address=mesos_address,
        framework_name=framework_name,
        framework_staging_timeout=framework_staging_timeout,
        initial_decline_delay=0.5,
    )


def paasta_to_task_config_kwargs(
    service,
    instance,
    system_paasta_config,
    native_job_config,
    offer_timeout,
    docker_image=None,
):
    kwargs = {
        'cpus': float(native_job_config.get_cpus()),
        'mem': float(native_job_config.get_mem()),
        'disk': float(native_job_config.get_disk(10)),
        'uris': [system_paasta_config.get_dockercfg_location()],
        'environment': native_job_config.get_env_dictionary(),
        'containerizer': 'DOCKER',
        'image': docker_image or native_job_config.get_docker_url(),
        'offer_timeout': offer_timeout,
    }

    # docker kwargs
    kwargs['docker_parameters'] = [
        {'key': param['key'], 'value': param['value']}
        for param in native_job_config.format_docker_parameters()
    ]
    docker_volumes = native_job_config.get_volumes(
        system_volumes=system_paasta_config.get_volumes(),
    )
    kwargs['volumes'] = [
        {
            'container_path': volume['containerPath'],
            'host_path': volume['hostPath'],
            'mode': volume['mode'].upper(),
        }
        for volume in docker_volumes
    ]
    # cmd kwarg
    cmd = native_job_config.get_cmd()
    if cmd:
        kwargs['cmd'] = cmd
    # gpus kwarg
    gpus = native_job_config.get_gpus()
    if gpus:
        kwargs['gpus'] = int(gpus)
        kwargs['containerizer'] = 'MESOS'  # docker containerizer does not support gpus
    # task name kwarg (requires everything else to hash)
    config_hash = get_config_hash(
        kwargs,
        force_bounce=native_job_config.get_force_bounce(),
    )
    kwargs['name'] = str(compose_job_id(
        service,
        instance,
        git_hash=get_code_sha_from_dockerurl(kwargs['image']),
        config_hash=config_hash,
        spacer=MESOS_TASK_SPACER,
    ))

    return kwargs


def create_mesos_task_config(processor, service, instance, *args, **kwargs):
    """ Creates a Mesos task configuration """
    MesosExecutor = processor.executor_cls('mesos_task')
    try:
        return MesosExecutor.TASK_CONFIG_INTERFACE(
            **paasta_to_task_config_kwargs(service, instance, *args, **kwargs),
        )
    except InvariantException as e:
        if len(e.missing_fields) > 0:
            paasta_print(PaastaColors.red(
                "Mesos task config is missing following fields: "
                f"{', '.join(e.missing_fields)}",
            ))
        elif len(e.invariant_errors) > 0:
            paasta_print(PaastaColors.red(
                "Mesos task config is failing following checks: "
                f"{', '.join(str(ie) for ie in e.invariant_errors)}",
            ))
        else:
            paasta_print(PaastaColors.red(f"Mesos task config error: {e}"))
    except PTypeError as e:
        paasta_print(PaastaColors.red(
            f"Mesos task config is failing a type check: {e}",
        ))
    traceback.print_exc()
    emit_counter_metric('paasta.remote_run.start.failed', service, instance)
    sys.exit(1)


def task_config_to_dict(task_config):
    """ Convert a task config to a dict and remove all empty keys """
    dconf = dict(task_config)
    for k in list(dconf.keys()):
        if not dconf[k]:
            del dconf[k]
    return dconf


def create_boto_session(taskproc_config, region):
    # first, try to load credentials
    credentials_file = taskproc_config.get('boto_credential_file')
    if credentials_file:
        with open(credentials_file) as f:
            credentials = json.loads(f.read())
    else:
        raise ValueError("Required aws credentials")

    # second, create the session for the given region
    return Session(
        region_name=region,
        aws_access_key_id=credentials['accessKeyId'],
        aws_secret_access_key=credentials['secretAccessKey'],
    )


# TODO: rename to registry?
def build_executor_stack(
    processor,
    cluster_executor,
    taskproc_config,
    cluster,
    region,
):
    """ Executor stack consists of:
    1. Cluster Executor (e.g. MesosExecutor)
    2. LoggingExecutor
    3. StatefulExecutor
    """
    # logging executor
    task_logging_executor = processor.executor_from_config(
        provider='logging',
        provider_config={
            'downstream_executor': cluster_executor,
        },
    )
    # stateful executor
    StatefulExecutor = processor.executor_cls(provider='stateful')
    stateful_executor = StatefulExecutor(
        downstream_executor=task_logging_executor,
        persister=DynamoDBPersister(
            table_name=f"taskproc_events_{cluster}",
            session=create_boto_session(taskproc_config, region),
            endpoint_url=taskproc_config.get('dynamodb_endpoint'),
        ),
    )
    return stateful_executor


def set_runner_signal_handlers(runner):
    def handle_interrupt(_signum, _frame):
        paasta_print(
            PaastaColors.red("Signal received, shutting down scheduler."),
        )
        if runner is not None:
            runner.stop()
        sys.exit(143 if _signum == signal.SIGTERM else 1)
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)


def run_task(executor, task_config):
    """ Runs a task until a terminal event is received, which is returned. """
    runner = Sync(executor)
    set_runner_signal_handlers(runner)
    terminal_event = runner.run(task_config)
    if getattr(terminal_event, 'platform_type', None) == 'lost':
        runner.kill(task_config.task_id)
    runner.stop()
    return terminal_event


def get_terminal_event_error_message(terminal_event):
    if not terminal_event.terminal or terminal_event.success:
        return None  # not terminal or not an error
    elif TASKPROC_OFFER_TIMEOUT_RAW in terminal_event.raw:
        return terminal_event.raw
    else:
        mesos_type = getattr(terminal_event, 'platform_type', None)
        if mesos_type == 'failed':
            error_message = '- Task exited with non-zero exit code'
        elif mesos_type == 'lost':
            error_message = (
                "- Task was lost probably due to a network partition or an "
                "agent going away. It probably isn't coming back :("
            )
        elif mesos_type == 'error':
            error_message = "- Encountered an unexpected error with Mesos"
        else:
            error_message = "- Unknown failure"

        error_parts = [error_message] if error_message else []
        error_parts.append(f"- Raw: {pprint.pformat(terminal_event.raw)}")
        return '\n'.join(error_parts)


def run_tasks_with_retries(executor_factory, task_config_factory, retries=0):
    # use max in case retries is negative, +1 for initial try
    tries_left = max(retries, 0) + 1
    terminals = []

    while tries_left > 0:
        paasta_print(PaastaColors.yellow(
            f"Scheduling task on Mesos (tries left: {tries_left})",
        ))

        try:
            executor = executor_factory()
            task_config = task_config_factory()
            terminal_event = run_task(executor, task_config)
        except (Exception, ValueError) as e:
            # implies an error with our code, and not with mesos, so just return
            # immediately
            paasta_print(f"Except while running executor stack: {e}")
            traceback.print_exc()
            terminals.append((None, task_config))
            return terminals

        terminals.append((terminal_event, task_config))
        if terminal_event.success:
            paasta_print(PaastaColors.green("Task finished successfully"))
            break
        else:
            # TODO: add reconciliation and other more specific behavior
            error_msg = get_terminal_event_error_message(terminal_event)
            paasta_print(PaastaColors.red(f"Task failed:\n{error_msg}"))

        tries_left -= 1

    return terminals


def send_notification_email(
    email_address,
    framework_config,
    task_config,
    run_id,
    success=True,
    error_message=None,
):
    success_str = 'succeeded' if success else 'failed'

    msg = EmailMessage()
    msg['From'] = email_address
    msg['To'] = email_address
    msg['Subject'] = (f"remote-run {success_str.upper()} - {task_config['name']}")

    email_content = [
        f"Task '{task_config['name']}' {success_str}",
        f"Run id: {run_id}\n",
    ]
    if not success and error_message:  # show errors first
        email_content.extend(['Error message from the last attempt:', f'{error_message}\n'])
    email_content.extend([
        'Framework configuration:',
        f'{pprint.pformat(framework_config)}\n',
    ])
    email_content.extend([
        'Task configuration:',
        pprint.pformat(task_config),
    ])
    msg.set_content('\n'.join(email_content))

    with smtplib.SMTP('localhost') as s:
        s.send_message(msg)


def handle_terminal_event(
    event,
    service,
    instance,
    run_id,
    email_address=None,
    framework_config=None,
    task_config=None,
):
    """ Given a terminal event:
    1. Emit metrics
    2. Notify users
    3. Produce exit code
    """
    if event and event.success:
        exit_code = 0
        error_message = None
    else:
        emit_counter_metric('paasta.remote_run.start.failed', service, instance)
        exit_code = 1
        if not event:
            error_message = (
                "Encountered an exception while running task:\n"
                f'{traceback.format_exc()}'
            )
        elif not event.success:
            error_message = get_terminal_event_error_message(event)

    if email_address:
        framework_config['service'] = service
        framework_config['instance'] = instance
        send_notification_email(
            email_address,
            framework_config,
            task_config_to_dict(task_config),
            run_id,
            success=event.success,
            error_message=error_message,
        )
    return exit_code


def remote_run_start(args):
    """ Start a task in Mesos
    Steps:
    1. Accumulate overrides
    2. Create task configuration
    3. Build executor stack
    4. Run the task on the executor stack
    """
    # accumulate all configuration needed to build what we need to run a task
    system_paasta_config, service, cluster, \
        soa_dir, instance, instance_type = extract_args(args)
    # TODO: move run_id into task identifier?
    run_id = args.run_id or generate_run_id(length=10)
    framework_name = create_framework_name(service, instance, run_id)
    overrides = accumulate_config_overrides(args, service, instance)
    # TODO: implement DryRunExecutor?
    taskproc_config = system_paasta_config.get_taskproc()
    native_job_config = load_paasta_native_job_config(
        service,
        instance,
        cluster,
        soa_dir=soa_dir,
        instance_type=instance_type,
        config_overrides=overrides,
        load_deployments=not args.docker_image,
    )
    region = args.aws_region or taskproc_config.get('aws_region')
    default_role = system_paasta_config.get_remote_run_config().get('default_role')
    assert default_role
    role = native_job_config.get_role() or default_role
    pool = native_job_config.get_pool()
    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.stateful')
    processor.load_plugin(provider_module='task_processing.plugins.mesos')

    if args.detach:
        paasta_print("Running in background")
        if os.fork() > 0:
            return
        os.setsid()
        if os.fork() > 0:
            return
        sys.stdout = open('/dev/null', 'w')
        sys.stderr = open('/dev/null', 'w')

    # create factory functions for task_config and executors, which makes it
    # easier to recreate them for retry purposes
    def task_config_factory():
        return create_mesos_task_config(
            processor=processor,
            service=service,
            instance=instance,
            system_paasta_config=system_paasta_config,
            native_job_config=native_job_config,
            offer_timeout=args.staging_timeout,
            docker_image=args.docker_image,
        )

    framework_config = dict(
        cluster=cluster,
        framework_name=framework_name,
        framework_staging_timeout=args.staging_timeout,
        role=role,
        pool=pool,
    )
    executor_kwargs = dict(  # used to create mesos executor
        processor=processor,
        system_paasta_config=system_paasta_config,
        taskproc_config=taskproc_config,
        **framework_config,
    )

    def executor_factory():
        mesos_executor = create_mesos_executor(**executor_kwargs)
        return build_executor_stack(
            processor, mesos_executor, taskproc_config, cluster, region,
        )

    if args.dry_run:
        task_config_dict = task_config_to_dict(task_config_factory())
        pp = pprint.PrettyPrinter(indent=2)
        paasta_print(
            PaastaColors.green("Would have run task with:"),
            PaastaColors.green("Framework config:"),
            pp.pformat(framework_config),
            PaastaColors.green("Task config:"),
            pp.pformat(task_config_dict),
            sep='\n',
        )
        return

    terminals = run_tasks_with_retries(
        executor_factory,
        task_config_factory,
        retries=args.retries,
    )
    final_event, final_task_config = terminals[-1]
    exit_code = handle_terminal_event(
        event=final_event,
        service=service,
        instance=instance,
        run_id=run_id,
        email_address=args.notification_email,
        framework_config=framework_config,
        task_config=final_task_config,
    )
    sys.exit(exit_code)


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
        if re.match(r'\s', args.run_id):
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
            r'paasta-remote [^\s]+ (\w+) (\w+)', f.name,
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
