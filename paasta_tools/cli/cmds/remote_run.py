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
import json
import os
import re
from shlex import quote

from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_clusters
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import run_on_master
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig


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
        "remote-run",
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
    main_parser.set_defaults(command=paasta_remote_run)


def split_constraints(constraints):
    return [c.split(",", 2) for c in constraints]


def create_remote_run_command(args):
    cmd_parts = ["/usr/bin/paasta_remote_run", args.action]
    arg_vars = vars(args)
    arg_defaults = dict(ARG_DEFAULTS[args.action])  # copy dict
    arg_defaults.update(ARG_DEFAULTS["common"])
    arg_defaults.pop("constraint")  # needs conversion to json

    for k in arg_vars.keys():
        if k not in arg_defaults:  # skip keys we don't know about
            continue
        v = arg_vars[k]
        if v == arg_defaults[k]:  # skip values that have default value
            continue
        k = re.sub(r"_", "-", k)
        if isinstance(v, bool) and v:
            cmd_parts.append(f"--{k}")
        else:
            cmd_parts.extend([f"--{k}", quote(str(v))])

    # constraint, convert to json
    if len(arg_vars["constraint"]) > 0:
        constraints = split_constraints(arg_vars["constraint"])
        cmd_parts.extend(["--constraints-json", quote(json.dumps(constraints))])

    return cmd_parts


def paasta_remote_run(args):
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

    cmd_parts = create_remote_run_command(args)
    graceful_exit = args.action == "start" and not args.detach
    return_code, status = run_on_master(
        cluster=args.cluster,
        system_paasta_config=system_paasta_config,
        cmd_parts=cmd_parts,
        graceful_exit=graceful_exit,
    )

    # Status results are streamed. This print is for possible error messages.
    if status is not None:
        for line in status.rstrip().split("\n"):
            print("    %s" % line)

    return return_code
