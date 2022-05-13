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
import os
import traceback
import random
import string
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

from pyrsistent import InvariantException
from pyrsistent import PTypeError
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.task_processor import TaskProcessor
from task_processing.runners.sync import Sync  # noreorder
from task_processing.metrics import create_counter  # noreorder
from task_processing.metrics import get_metric  # noreorder

from paasta_tools.cli.utils import failure
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import compose_job_id
from paasta_tools.cli.utils import get_okta_auth_token
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_clusters
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import success
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_human_kube_config
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig

# from task_processing.interfaces.event import Event
# from task_processing.runners.subscription import Subscription

ARG_DEFAULTS = dict(
    common=dict(
        service=None,
        instance=None,
        cluster=None,  # load from system paasta config later
        verbose=False,
    ),
    start=dict(
        cmd=None,
        detach=False,
        staging_timeout=240.0,
        instances=1,
        docker_image=None,
        dry_run=False,
        constraint=[],
        notification_email=None,
        retries=0,
    ),
    stop=dict(run_id=None, framework_id=None),
    list=dict(),
)


def get_system_paasta_config():
    try:
        return load_system_paasta_config()
    except PaastaNotConfiguredError:
        print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This "
                "indicates PaaSTA is not configured locally on this host, and "
                "remote-run may not behave the same way it would behave on a "
                "server configured for PaaSTA."
            ),
            sep="\n",
        )
        return SystemPaastaConfig({"volumes": []}, "/etc/paasta")


def add_common_args_to_parser(parser):
    parser.add_argument(
        "-s",
        "--service",
        help="The name of the service you wish to inspect. Required.",
        required=True,
    ).completer = lazy_choices_completer(list_services)
    parser.add_argument(
        "-i",
        "--instance",
        help=(
            "Simulate a docker run for a particular instance of the "
            "service, like 'main' or 'canary'. Required."
        ),
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    parser.add_argument(
        "-c",
        "--cluster",
        help=(
            "The name of the cluster you wish to run your task on. "
            "If omitted, uses the default cluster defined in the paasta "
            f"remote-run configs."
        ),
        default=ARG_DEFAULTS["common"]["cluster"],
    ).completer = lazy_choices_completer(list_clusters)
    parser.add_argument(
        "-v",
        "--verbose",
        help="Show more output",
        action="store_true",
        default=ARG_DEFAULTS["common"]["verbose"],
    )


def add_start_parser(subparser):
    parser = subparser.add_parser("start", help="Start task subcommand")
    add_common_args_to_parser(parser)
    parser.add_argument(
        "-C",
        "--cmd",
        help=(
            "Run Docker container with particular command, for example: "
            '"bash". By default will use the command or args specified by the '
            "soa-configs or what was specified in the Dockerfile"
        ),
        default=ARG_DEFAULTS["start"]["cmd"],
    ),
    parser.add_argument(
        "-D",
        "--detach",
        help="Launch in background",
        action="store_true",
        default=ARG_DEFAULTS["start"]["detach"],
    )
    default_staging_timeout = ARG_DEFAULTS["start"]["staging_timeout"]
    parser.add_argument(
        "-t",
        "--staging-timeout",
        help=(
            "A timeout in seconds for the task to be launching before killed. "
            f"Default: {default_staging_timeout}s"
        ),
        default=ARG_DEFAULTS["start"]["staging_timeout"],
        type=float,
    )
    parser.add_argument(
        "-j",
        "--instances",
        help="Number of copies of the task to launch",
        default=ARG_DEFAULTS["start"]["instances"],
        type=int,
    )
    parser.add_argument(
        "--docker-image",
        help=(
            "URL of docker image to use. "
            "Defaults to using the deployed docker image."
        ),
        default=ARG_DEFAULTS["start"]["docker_image"],
    )
    parser.add_argument(
        "-R",
        "--run-id",
        help="ID of task to stop",
        default=ARG_DEFAULTS["stop"]["run_id"],
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        help=(
            "Don't launch the task. "
            "Instead output task that would have been launched"
        ),
        action="store_true",
        default=ARG_DEFAULTS["start"]["dry_run"],
    )
    parser.add_argument(
        "-X",
        "--constraint",
        help="Constraint option, format: <attr>,OP[,<value>], OP can be one "
        "of the following: EQUALS matches attribute value exactly, LIKE and "
        "UNLIKE match on regular expression, MAX_PER constrains number of "
        "tasks per attribute value, UNIQUE is the same as MAX_PER,1",
        action="append",
        default=ARG_DEFAULTS["start"]["constraint"],
    )
    default_email = os.environ.get("EMAIL", None)
    parser.add_argument(
        "-E",
        "--notification-email",
        help=(
            "Email address to send remote-run notifications to. "
            "A notification will be sent when a task either succeeds or fails. "
            "Defaults to env variable $EMAIL: "
        )
        + (default_email if default_email else "(currently not set)"),
        default=default_email,
    )
    default_retries = ARG_DEFAULTS["start"]["retries"]
    parser.add_argument(
        "-r",
        "--retries",
        help=(
            "Number of times to retry if task fails at launch or at runtime. "
            f"Default: {default_retries}"
        ),
        type=int,
        default=default_retries,
    )
    return parser


def add_stop_parser(subparser):
    parser = subparser.add_parser("stop", help="Stop task subcommand")
    add_common_args_to_parser(parser)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-R",
        "--run-id",
        help="ID of task to stop",
        default=ARG_DEFAULTS["stop"]["run_id"],
    )
    group.add_argument(
        "-F",
        "--framework-id",
        help=(
            "ID of framework to stop. Must belong to remote-run of selected "
            "service instance."
        ),
        type=str,
        default=ARG_DEFAULTS["stop"]["framework_id"],
    )
    return parser


def add_list_parser(subparser):
    parser = subparser.add_parser("list", help="List tasks subcommand")
    add_common_args_to_parser(parser)
    return parser


def add_subparser(subparsers):
    main_parser = subparsers.add_parser(
        "k8s-remote-run",
        help="Schedule Mesos to run adhoc command in context of a service",
        description=(
            "`paasta remote-run` is useful for running adhoc commands in "
            "context of a service's Docker image. The command will be "
            "scheduled on a Mesos cluster and stdout/stderr printed after "
            "execution is finished."
        ),
        epilog=(
            "Note: `paasta remote-run` uses Mesos API that may require "
            "authentication."
        ),
    )
    main_subs = main_parser.add_subparsers(
        dest="action", help="Subcommands of remote-run"
    )
    add_start_parser(main_subs)
    add_stop_parser(main_subs)
    add_list_parser(main_subs)
    main_parser.set_defaults(command=paasta_k8s_remote_run)


def extract_args(args):
    system_paasta_config = get_system_paasta_config()
    soa_dir = args.yelpsoa_config_root
    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)

    cluster = args.cluster or system_paasta_config.get_remote_run_config().get(
        "default_cluster", None
    )
    if not cluster:
        print(
            PaastaColors.red(
                "PaaSTA on this machine has not been configured with a default cluster."
                "Please pass one using '-c'."
            )
        )
        emit_counter_metric(
            "paasta.remote_run." + args.action + ".failed", service, "UNKNOWN"
        )
        sys.exit(1)

    instance = args.instance
    if instance is None:
        instance_type = "adhoc"
        instance = "remote"
    else:
        try:
            instance_type = validate_service_instance(
                service, instance, cluster, soa_dir
            )
        except NoConfigurationForServiceError as e:
            print(e)
            emit_counter_metric(
                "paasta.remote_run." + args.action + ".failed", service, instance
            )
            sys.exit(1)

        if instance_type != "adhoc":
            print(
                PaastaColors.red(
                    "Please use instance declared in adhoc.yaml for use "
                    f"with remote-run, {instance} is declared as {instance_type}"
                )
            )
            emit_counter_metric(
                "paasta.remote_run." + args.action + ".failed", service, instance
            )
            sys.exit(1)

    return (system_paasta_config, service, cluster, soa_dir, instance, instance_type)


def paasta_k8s_remote_run(args): # type?
    system_paasta_config = get_system_paasta_config()

    if not args.cluster:
        default_cluster = system_paasta_config.get_remote_run_config().get(
            "default_cluster"
        )
        if not default_cluster:
            print(
                PaastaColors.red(
                    "Error: no cluster specified and no default cluster available"
                )
            )
            return 1
        args.cluster = default_cluster

    #TODO: Get instance config

    # TODO: dry_run
    if args.dry_run:
        task_config_dict = task_config_to_dict(task_config_factory())
        pp = pprint.PrettyPrinter(indent=2)
        print(
            PaastaColors.green("Would have run task with:"),
            PaastaColors.green("Framework config:"),
            pp.pformat(framework_config),
            PaastaColors.green("Task config:"),
            pp.pformat(task_config_dict),
            sep="\n",
        )
        return

    #TODO: stop, list
    actions = {
        "start": remote_run_start,
        "stop": remote_run_stop,
        "list": remote_run_list,
    }
    return actions[args.action](args)
### from paasta_remote_run
def task_config_to_dict(task_config):
    """Convert a task config to a dict and remove all empty keys"""
    dconf = dict(task_config)
    for k in list(dconf.keys()):
        if not dconf[k]:
            del dconf[k]
    return dconf


def emit_counter_metric(counter_name, service, instance):
    create_counter(counter_name, {"service": service, "instance": instance})
    get_metric(counter_name).count(1)


def generate_run_id(length=8):
    """Generates a random string of uppercase letters and digits for use as
    a run identifier
    """
    run_id = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )
    print(f"Generated random run identifier: {run_id}")
    return run_id



def remote_run_list_report(service, instance, cluster, frameworks=None):
    filtered = remote_run_filter_frameworks(service, instance, frameworks=frameworks)
    filtered.sort(key=lambda x: x.name)
    for f in filtered:
        launch_time, run_id = re.match(
            r"paasta-remote [^\s]+ (\w+) (\w+)", f.name
        ).groups()
        print(
            "Launch time: %s, run id: %s, framework id: %s"
            % (launch_time, run_id, f.id)
        )
    if len(filtered) > 0:
        print(
            (
                "Use `paasta remote-run stop -s {} -c {} -i {} [-R <run id> "
                "| -F <framework id>]` to stop."
            ).format(service, cluster, instance)
        )
    else:
        print("Nothing found.")


def remote_run_list(args, frameworks=None):
    _, service, cluster, _, instance, _ = extract_args(args)
    return remote_run_list_report(
        service=service, instance=instance, cluster=cluster, frameworks=frameworks
    )

def remote_run_stop(args):
    return "Not implemented"

def set_runner_signal_handlers(runner):
    def handle_interrupt(_signum, _frame):
        print(PaastaColors.red("Signal received, shutting down scheduler."))
        if runner is not None:
            runner.stop()
        sys.exit(143 if _signum == signal.SIGTERM else 1)

    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)


def run_task(executor, task_config):
    """Runs a task until a terminal event is received, which is returned."""
    #runner = Sync(executor) # requires task_id
    runner = executor
    set_runner_signal_handlers(runner)
    terminal_event = runner.run(task_config)
    if getattr(terminal_event, "platform_type", None) == "lost":
        runner.kill(task_config.task_id)
    runner.stop()
    return terminal_event

def get_terminal_event_error_message(terminal_event):
    if not terminal_event.terminal or terminal_event.success:
        return None  # not terminal or not an error
    elif TASKPROC_OFFER_TIMEOUT_RAW in terminal_event.raw:
        return terminal_event.raw
    else:
        mesos_type = getattr(terminal_event, "platform_type", None)
        if mesos_type == "failed":
            error_message = "- Task exited with non-zero exit code"
        elif mesos_type == "lost":
            error_message = (
                "- Task was lost probably due to a network partition or an "
                "agent going away. It probably isn't coming back :("
            )
        elif mesos_type == "error":
            error_message = "- Encountered an unexpected error with Mesos"
        else:
            error_message = "- Unknown failure"

        error_parts = [error_message] if error_message else []
        error_parts.append(f"- Raw: {pprint.pformat(terminal_event.raw)}")
        return "\n".join(error_parts)

def run_tasks_with_retries(executor_factory, task_config_factory, retries=0):
    # use max in case retries is negative, +1 for initial try
    tries_left = max(retries, 0) + 1
    terminals = []

    while tries_left > 0:
        print(
            PaastaColors.yellow(f"Scheduling task on Mesos (tries left: {tries_left})")
        )
        task_config = None
        terminal_event = None
        try:
            executor = executor_factory()
            task_config = task_config_factory()
            terminal_event = run_task(executor, task_config)
        except (Exception, ValueError) as e:
            # implies an error with our code, and not with mesos, so just return
            # immediately
            print(f"Except while running executor stack: {e}")
            traceback.print_exc()
            terminals.append((None, task_config))
            return terminals

        terminals.append((terminal_event, task_config))
        if terminal_event.success:
            print(PaastaColors.green("Task finished successfully"))
            break
        else:
            # TODO: add reconciliation and other more specific behavior
            error_msg = get_terminal_event_error_message(terminal_event)
            print(PaastaColors.red(f"Task failed:\n{error_msg}"))

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
    success_str = "succeeded" if success else "failed"

    msg = EmailMessage()
    msg["From"] = email_address
    msg["To"] = email_address
    msg["Subject"] = f"remote-run {success_str.upper()} - {task_config['name']}"

    email_content = [
        f"Task '{task_config['name']}' {success_str}",
        f"Run id: {run_id}\n",
    ]
    if not success and error_message:  # show errors first
        email_content.extend(
            ["Error message from the last attempt:", f"{error_message}\n"]
        )
    email_content.extend(
        ["Framework configuration:", f"{pprint.pformat(framework_config)}\n"]
    )
    email_content.extend(["Task configuration:", pprint.pformat(task_config)])
    msg.set_content("\n".join(email_content))

    with smtplib.SMTP("localhost") as s:
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
    """Given a terminal event:
    1. Emit metrics
    2. Notify users
    3. Produce exit code
    """
    if event and event.success:
        exit_code = 0
        error_message = None
    else:
        emit_counter_metric("paasta.remote_run.start.failed", service, instance)
        exit_code = 1
        if not event:
            error_message = (
                "Encountered an exception while running task:\n"
                f"{traceback.format_exc()}"
            )
        elif not event.success:
            error_message = get_terminal_event_error_message(event)

    if email_address:
        if framework_config:
            framework_config["service"] = service
            framework_config["instance"] = instance
        send_notification_email(
            email_address,
            framework_config,
            task_config_to_dict(task_config),
            run_id,
            success=event.success,
            error_message=error_message,
        )
    return exit_code

# TODO: Do we need this? we can't dynamo from the client :/ 
def build_executor_stack(processor, cluster_executor, cluster=None, region=None):
    """Executor stack consists of:
    1. Cluster Executor (e.g. MesosExecutor)
    2. LoggingExecutor
    3. StatefulExecutor
    """
    return cluster_executor
    # logging executor
    #
    #task_logging_executor = processor.executor_from_config(
    #    provider="logging", provider_config={"downstream_executor": cluster_executor}
    #)
    # stateful executor
    #StatefulExecutor = processor.executor_cls(provider="stateful")
    #stateful_executor = StatefulExecutor(
    #    downstream_executor=task_logging_executor,
    #    persister=DynamoDBPersister(
    #        table_name=f"taskproc_events_{cluster}",
    #        session=create_boto_session(taskproc_config, region),
    #        endpoint_url=taskproc_config.get("dynamodb_endpoint"),
    #    ),
    #)
    return task_logging_executor

###

def create_k8s_executor(
    processor,
    system_paasta_config,
    cluster,
    kube_config,
    role="*",
):
    """Create a Kubernetes executor specific to our cluster"""
    return processor.executor_from_config(
            provider="kubernetes",
            provider_config={
                "namespace": "paasta",  # TODO get namespace from instance
                "kubeconfig_dict": kube_config,
            },
        )


def paasta_to_task_config_kwargs(
    service,
    instance,
    cluster,
    system_paasta_config,
    instance_config,
    docker_image=None,
):

    kwargs = {
        "cpus": float(instance_config.get_cpus()),
        "memory": float(instance_config.get_mem()),
        "disk": float(instance_config.get_disk(10)),
        "environment": instance_config.get_env_dictionary(),
        "image": docker_image or instance_config.get_docker_url(),
        "cap_add": [c['value'] for c in instance_config.get_cap_add()],
        "cap_drop": [c['value'] for c in instance_config.get_cap_drop()],
    }
    # k8s-only things?
    # TODO: persistent volumes
    # start with defaults
    labels = {
        "paasta.yelp.com/cluster": instance_config.get_cluster(),
        "paasta.yelp.com/pool": instance_config.get_pool(),
        "paasta.yelp.com/service": instance_config.get_service(),
        "paasta.yelp.com/instance": limit_size_with_hash(
            instance_config.get_instance(),
            limit=63,
            suffix=4,
        ),
    }

    annotations = {
        "paasta.yelp.com/routable_ip": "false"
    }

    k8s_args = {
        "secret_environment": {},
        "node_selectors": [],
        "node_affinities": [],
        "labels": labels, #todo: helper
        "annotations": annotations, #todo: helper
        "service_account_name": None, #todo: helper; 
    }

    if (
        instance_config.get_iam_role_provider() == "aws"
        and instance_config.get_iam_role()
        and not instance_config.for_validation
    ):
        # this service account will be used for normal Tron batches as well as for Spark executors
        k8s_args["service_account_name"] = create_or_find_service_account_name(
            iam_role=instance_config.get_iam_role(),
            namespace="paasta", # TODO: dynamic namespace
            k8s_role=None,
            dry_run=instance_config.for_validation,
        )

    kwargs.update(k8s_args)

    docker_volumes = instance_config.get_volumes(
        system_volumes=system_paasta_config.get_volumes()
    )
    kwargs["volumes"] = [
        {
            "container_path": volume["containerPath"],
            "host_path": volume["hostPath"],
            "mode": volume["mode"].upper(),
        }
        for volume in docker_volumes
    ]
    # cmd kwarg
    print(instance_config)
    cmd = instance_config.get_cmd()
    print(instance_config.get_cmd())
    if cmd:
        if isinstance(cmd, str):
            kwargs["command"] = cmd
        elif isinstance(cmd, list):
            kwargs["command"] = " ".join(cmd)
    else:
        kwargs["command"] = 'sleep 240' # get from args
    print(f"COMMAND WAS: {kwargs['command']}")
    # if not cmd, then what? 
    # gpus kwarg
    gpus = instance_config.get_gpus()
    if gpus:
        kwargs["gpus"] = int(gpus)
    # task name kwarg (requires everything else to hash)

    kwargs["name"] = compose_job_id(service, instance, 'remote', os.environ.get("USER"))

    return kwargs


def create_k8s_task_config(processor, service, instance, cluster, *args, **kwargs):
    """Creates a Kubernetes task configuration"""
    # why so roundabout
    executor = processor.executor_cls("kubernetes")
    try:
        return executor.TASK_CONFIG_INTERFACE(
            **paasta_to_task_config_kwargs(service, instance, cluster, *args, **kwargs)
        )
    except InvariantException as e:
        if len(e.missing_fields) > 0:
            print(
                PaastaColors.red(
                    "Remote task config is missing following fields: "
                    f"{', '.join(e.missing_fields)}"
                )
            )
        elif len(e.invariant_errors) > 0:
            print(
                PaastaColors.red(
                    "Remote task config is failing following checks: "
                    f"{', '.join(str(ie) for ie in e.invariant_errors)}"
                )
            )
        else:
            print(PaastaColors.red(f"Remote task config error: {e}"))
    except PTypeError as e:
        print(PaastaColors.red(f"Remote task config is failing a type check: {e}"))
    traceback.print_exc()
    emit_counter_metric("paasta.remote_run.start.failed", service, instance)
    sys.exit(1)


def remote_run_start(args):
    """Start a task in k8s"""
    # TODO: Only handles the most basic thing and tries to exec a pod
    system_paasta_config = get_system_paasta_config()
    auth_token = get_okta_auth_token()
    kube_config = get_human_kube_config(cluster=args.cluster, auth_token=auth_token)

    kube_client = KubeClient(config_dict=kube_config)

    pods = get_all_pods(kube_client)
    print(f"Found {len(pods)} pods!")
    #    return
    processor = TaskProcessor()
    processor.load_plugin(provider_module="task_processing.plugins.kubernetes")

    # TODO : handle overrides via cli args
    instance_config = get_instance_config(args.service, args.instance, args.cluster, load_deployments=not args.docker_image)

    if args.cmd:
        instance_config.config_dict['cmd'] = args.cmd

    def task_config_factory():
        return create_k8s_task_config(
            processor=processor,
            service=args.service,
            instance=args.instance,
            cluster=args.cluster,
            system_paasta_config=system_paasta_config,
            instance_config=instance_config,
            docker_image=(args.docker_image if args.docker_image else None),
        )

    def executor_factory():
        k8s_executor = create_k8s_executor(processor=processor, system_paasta_config=system_paasta_config, cluster=args.cluster, kube_config=kube_config)
        return build_executor_stack(
            processor, k8s_executor, args.cluster
        )


    try:
        # remote-run style
        #terminals = run_tasks_with_retries(executor_factory, task_config_factory, retries=args.retries)
        #print(terminals)
        #final_event, final_task_config = terminals[-1]
        #run_id = generate_run_id()
        #exit_code = handle_terminal_event(
        #    event=final_event,
        #    service=args.service,
        #    instance=args.instance,
        #    run_id=run_id,
        #    email_address=args.notification_email,
        #    framework_config=None,
        #    task_config=final_task_config,
        #)
        #sys.exit(exit_code)


    # toy
 #   try:
        executor = create_k8s_executor(processor=processor, system_paasta_config=system_paasta_config, cluster=args.cluster, kube_config=kube_config)
 #       # TODO get taskconfig from cli.util.get_instance_config
        user = os.environ.get("USER", "Anonymous")
        task_config = task_config_factory()
        print(task_config)
        task_id = executor.run(task_config)

        event_queue = executor.get_event_queue()

        terminal_event = None

        
        while True:
            event = event_queue.get()
            if event.kind == 'control' and \
               event.message == 'stop':
                log.info('Stop event received: {}'.format(event))
                return event

            if event.terminal:
                terminal_event = event
                print('Terminal event. Stopping')
                executor.stop()
                break
        success(f'Ran task on pod: {task_id}')
 #       task_config = KubernetesTaskConfig(
 #           name=f"test-task-{args.service}-{args.instance}",
 #           command=f'/bin/sh -c "echo i am {user} doing TaskProc things at $(date)"',
 #           image="docker-paasta.yelpcorp.com:443/services-compute-infra-test-service:paasta-833c8de1d7a4879aa2b5544c4f9bcd6fe035ffe2",
 #           cpus=0.1,
 #           memory=128,
 #           disk=10,
 #           node_selectors={"yelp.com/pool": "default"},
 #           labels={
 #               "paasta.yelp.com/service": "compute-infra-test-service",
 #               "paasta.yelp.com/instance": "test-remoterun-jfong",
 #           },
 #           annotations={"paasta.yelp.com/routable_ip": "true"},
 #       )
 #       task_id = executor.run(task_config)
 #       success(f"Started pod: {task_id}")
#
        # TODO: if interactive:
        #  create kubeclient here
        #  exec into pod
        # TODO: if noninteractive:
        # wait here for executor to finish
        # tail logs from pod?
        # print status results

        # TODO: copy remote_run to handle retries (i.e. use executor_factory/task_config_factory)?

    except Exception as e:
        failure(f"Hit exception: {repr(e)}", None)
        raise
    # Status results are streamed. This print is for possible error messages.


#    if status is not None:
#        for line in status.rstrip().split("\n"):
#            print("    %s" % line)
#
