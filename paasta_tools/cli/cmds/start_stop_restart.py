#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import datetime
import socket
import sys
from typing import Dict
from typing import List

import choice

from paasta_tools import remote_git
from paasta_tools import utils
from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cli.cmds.mark_for_deployment import can_user_deploy_service
from paasta_tools.cli.cmds.mark_for_deployment import get_deploy_info
from paasta_tools.cli.cmds.status import add_instance_filter_arguments
from paasta_tools.cli.cmds.status import apply_args_filters
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import get_paasta_oapi_api_clustername
from paasta_tools.cli.utils import trigger_deploys
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.flinkeks_tools import FlinkEksDeploymentConfig
from paasta_tools.generate_deployments_for_service import get_latest_deployment_tag
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers):
    for command, lower, upper, cmd_func in [
        ("start", "start or restart", "Start or restart", paasta_start),
        ("restart", "start or restart", "Start or restart", paasta_restart),
        ("stop", "stop", "Stop", paasta_stop),
    ]:
        status_parser = subparsers.add_parser(
            command,
            help="%ss a PaaSTA service in a graceful way." % upper,
            description=(
                "%ss a PaaSTA service in a graceful way. This uses the Git control plane."
                % upper
            ),
            epilog=(
                "This command uses Git, and assumes access and authorization to the Git repo "
                "for the service is available."
            ),
        )
        add_instance_filter_arguments(status_parser, verb=lower)
        status_parser.add_argument(
            "-d",
            "--soa-dir",
            dest="soa_dir",
            metavar="SOA_DIR",
            default=DEFAULT_SOA_DIR,
            help="define a different soa config directory",
        )
        status_parser.set_defaults(command=cmd_func)


def format_tag(branch, force_bounce, desired_state):
    return f"refs/tags/paasta-{branch}-{force_bounce}-{desired_state}"


def make_mutate_refs_func(service_config, force_bounce, desired_state):
    """Create a function that will inform send_pack that we want to create tags
    corresponding to the set of branches passed, with the given force_bounce
    and desired_state parameters. These tags will point at the current tip of
    the branch they associate with.

    dulwich's send_pack wants a function that takes a dictionary of ref name
    to sha and returns a modified version of that dictionary. send_pack will
    then diff what is returned versus what was passed in, and inform the remote
    git repo of our desires."""

    def mutate_refs(refs):
        deploy_group = service_config.get_deploy_group()
        (_, head_sha, _) = get_latest_deployment_tag(refs, deploy_group)
        refs[
            format_tag(service_config.get_branch(), force_bounce, desired_state)
        ] = head_sha
        return refs

    return mutate_refs


def log_event(service_config, desired_state):
    user = utils.get_username()
    host = socket.getfqdn()
    line = "Issued request to change state of {} (an instance of {}) to '{}' by {}@{}".format(
        service_config.get_instance(),
        service_config.get_service(),
        desired_state,
        user,
        host,
    )
    utils._log(
        service=service_config.get_service(),
        level="event",
        cluster=service_config.get_cluster(),
        instance=service_config.get_instance(),
        component="deploy",
        line=line,
    )

    utils._log_audit(
        action=desired_state,
        service=service_config.get_service(),
        cluster=service_config.get_cluster(),
        instance=service_config.get_instance(),
    )


def issue_state_change_for_service(service_config, force_bounce, desired_state):
    ref_mutator = make_mutate_refs_func(
        service_config=service_config,
        force_bounce=force_bounce,
        desired_state=desired_state,
    )
    git_url = utils.get_git_url(service_config.get_service())
    remote_git.create_remote_refs(git_url, ref_mutator)
    if "yelpcorp.com" in git_url:
        trigger_deploys(service_config.get_service())
    log_event(service_config=service_config, desired_state=desired_state)


def print_kubernetes_message(desired_state):
    if desired_state == "start":
        print(
            "This service will soon be gracefully started/restarted, replacing old instances according "
            "to the bounce method chosen in soa-configs. "
        )
    elif desired_state == "stop":
        print(
            "This service will be gracefully stopped soon. It will be started back up again on the next deploy.\n"
            "To stop this service permanently. Set this in the soa-configs definition:\n"
            "\n"
            "    instances: 0\n"
        )


def print_flink_message(desired_state):
    if desired_state == "start":
        print("'Start' will tell Flink operator to start the cluster.")
    elif desired_state == "stop":
        print(
            "'Stop' will put Flink cluster in stopping mode, it may"
            "take some time before shutdown is completed."
        )


def confirm_to_continue(cluster_service_instances, desired_state):
    print(f"You are about to {desired_state} the following instances:")
    print("Either --instances or --clusters not specified. Asking for confirmation.")
    i_count = 0
    for cluster, services_instances in cluster_service_instances:
        for service, instances in services_instances.items():
            for instance in instances.keys():
                print(f"cluster = {cluster}, instance = {instance}")
                i_count += 1
    if sys.stdin.isatty():
        return choice.Binary(
            f"Are you sure you want to {desired_state} these {i_count} instances?",
            False,
        ).ask()
    return True


REMOTE_REFS: Dict[str, List[str]] = {}


def get_remote_refs(service, soa_dir):
    if service not in REMOTE_REFS:
        REMOTE_REFS[service] = remote_git.list_remote_refs(
            utils.get_git_url(service, soa_dir)
        )
    return REMOTE_REFS[service]


def paasta_start_or_stop(args, desired_state):
    """Requests a change of state to start or stop given branches of a service."""
    soa_dir = args.soa_dir

    pargs = apply_args_filters(args)
    if len(pargs) == 0:
        return 1

    affected_services = {
        s for service_list in pargs.values() for s in service_list.keys()
    }
    if len(affected_services) > 1:
        print(
            PaastaColors.red("Warning: trying to start/stop/restart multiple services:")
        )

        for cluster, services_instances in pargs.items():
            print("Cluster %s:" % cluster)
            for service, instances in services_instances.items():
                print("    Service %s:" % service)
                print("        Instances %s" % ",".join(instances.keys()))

        if sys.stdin.isatty():
            confirm = choice.Binary("Are you sure you want to continue?", False).ask()
        else:
            confirm = False
        if not confirm:
            print()
            print("exiting")
            return 1

    if not all(
        [
            can_user_deploy_service(get_deploy_info(service, soa_dir), service)
            for service in affected_services
        ]
    ):
        print(PaastaColors.red("Exiting due to missing deploy permissions"))
        return 1

    invalid_deploy_groups = []
    kubernetes_message_printed = False
    affected_flinks = []

    if args.clusters is None or args.instances is None:
        if confirm_to_continue(pargs.items(), desired_state) is False:
            print()
            print("exiting")
            return 1

    for cluster, services_instances in pargs.items():
        for service, instances in services_instances.items():
            for instance in instances.keys():
                service_config = get_instance_config(
                    service=service,
                    cluster=cluster,
                    instance=instance,
                    soa_dir=soa_dir,
                    load_deployments=False,
                )
                if isinstance(service_config, FlinkDeploymentConfig):
                    affected_flinks.append(service_config)
                    continue

                try:
                    remote_refs = get_remote_refs(service, soa_dir)
                except remote_git.LSRemoteException as e:
                    msg = (
                        "Error talking to the git server: %s\n"
                        "This PaaSTA command requires access to the git server to operate.\n"
                        "The git server may be down or not reachable from here.\n"
                        "Try again from somewhere where the git server can be reached, "
                        "like your developer environment."
                    ) % str(e)
                    print(msg)
                    return 1

                deploy_group = service_config.get_deploy_group()
                (deploy_tag, _, _) = get_latest_deployment_tag(
                    remote_refs, deploy_group
                )

                if deploy_tag not in remote_refs:
                    invalid_deploy_groups.append(deploy_group)
                else:
                    force_bounce = utils.format_timestamp(datetime.datetime.utcnow())
                    if (
                        isinstance(service_config, KubernetesDeploymentConfig)
                        and not kubernetes_message_printed
                    ):
                        print_kubernetes_message(desired_state)
                        kubernetes_message_printed = True

                    issue_state_change_for_service(
                        service_config=service_config,
                        force_bounce=force_bounce,
                        desired_state=desired_state,
                    )

    return_val = 0

    # TODO: Refactor to discover if set_state is available for given
    #       instance_type in API
    if affected_flinks:
        print_flink_message(desired_state)

        system_paasta_config = load_system_paasta_config()
        for service_config in affected_flinks:
            cluster = service_config.cluster
            service = service_config.service
            instance = service_config.instance
            is_eks = isinstance(service_config, FlinkEksDeploymentConfig)

            client = get_paasta_oapi_client(
                cluster=get_paasta_oapi_api_clustername(cluster=cluster, is_eks=is_eks),
                system_paasta_config=system_paasta_config,
            )
            if not client:
                print("Cannot get a paasta-api client")
                exit(1)

            try:
                client.service.instance_set_state(
                    service=service,
                    instance=instance,
                    desired_state=desired_state,
                )
            except client.api_error as exc:
                print(exc.reason)
                return exc.status

            return_val = 0

    if invalid_deploy_groups:
        print(f"No deploy tags found for {', '.join(invalid_deploy_groups)}.")
        print(f"Has {service} been deployed there yet?")
        return_val = 1

    return return_val


def paasta_start(args):
    return paasta_start_or_stop(args, "start")


def paasta_restart(args):
    pargs = apply_args_filters(args)
    soa_dir = args.soa_dir

    affected_flinks = []
    affected_non_flinks = []
    for cluster, service_instances in pargs.items():
        for service, instances in service_instances.items():
            for instance in instances.keys():
                service_config = get_instance_config(
                    service=service,
                    cluster=cluster,
                    instance=instance,
                    soa_dir=soa_dir,
                    load_deployments=False,
                )
                if isinstance(service_config, FlinkDeploymentConfig):
                    affected_flinks.append(service_config)
                else:
                    affected_non_flinks.append(service_config)

    if affected_flinks:
        flinks_info = ", ".join([f"{f.service}.{f.instance}" for f in affected_flinks])
        print(
            f"WARN: paasta restart is currently unsupported for Flink instances ({flinks_info})."
        )
        print("To restart, please run:", end="\n\n")
        for flink in affected_flinks:
            print(
                f"paasta stop -s {flink.service} -i {flink.instance} -c {flink.cluster}"
            )
            print(
                f"paasta start -s {flink.service} -i {flink.instance} -c {flink.cluster}",
                end="\n\n",
            )

        if not affected_non_flinks:
            return 1

        non_flinks_info = ", ".join(
            [f"{f.service}.{f.instance}" for f in affected_non_flinks]
        )
        proceed = choice.Binary(
            f"Would you like to restart the other instances ({non_flinks_info}) anyway?",
            False,
        ).ask()

        if not proceed:
            return 1

    return paasta_start(args)


PAASTA_STOP_UNDERSPECIFIED_ARGS_MESSAGE = PaastaColors.red(
    "paasta stop requires explicit specification of cluster, service, and instance."
)


def paasta_stop(args):
    if not args.clusters:
        print(PAASTA_STOP_UNDERSPECIFIED_ARGS_MESSAGE)
        return 1
    elif not args.service_instance and not (args.service and args.instances):
        print(PAASTA_STOP_UNDERSPECIFIED_ARGS_MESSAGE)
        return 1
    return paasta_start_or_stop(args, "stop")
