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
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""
import argparse
import asyncio
import concurrent
import datetime
import functools
import getpass
import logging
import math
import os
import socket
import sys
import time
import traceback
from threading import Thread
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple

import a_sync
import humanize
import progressbar
from service_configuration_lib import read_deploy
from slackclient import SlackClient
from sticht import state_machine
from sticht.slo import SLOSlackDeploymentProcess
from sticht.slo import SLOWatcher

from paasta_tools import remote_git
from paasta_tools.api import client
from paasta_tools.cassandracluster_tools import CassandraClusterDeploymentConfig
from paasta_tools.cli.cmds.push_to_registry import is_docker_image_already_in_registry
from paasta_tools.cli.cmds.status import get_main_container
from paasta_tools.cli.cmds.status import get_version_table_entry
from paasta_tools.cli.cmds.status import recent_container_restart
from paasta_tools.cli.utils import get_jenkins_build_output_url
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import trigger_deploys
from paasta_tools.cli.utils import validate_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.cli.utils import validate_short_git_sha
from paasta_tools.deployment_utils import get_currently_deployed_sha
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.paastaapi.models import InstanceStatusKubernetesV2
from paasta_tools.paastaapi.models import KubernetesPodV2
from paasta_tools.slack import get_slack_client
from paasta_tools.utils import _log
from paasta_tools.utils import _log_audit
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_tag
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_paasta_tag_from_deploy_group
from paasta_tools.utils import get_username
from paasta_tools.utils import ldap_user_search
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import RollbackTypes
from paasta_tools.utils import TimeoutError


DEFAULT_DEPLOYMENT_TIMEOUT = 3 * 3600  # seconds
DEFAULT_WARN_PERCENT = 17  # ~30min for default timeout
DEFAULT_AUTO_CERTIFY_DELAY = 600  # seconds
DEFAULT_SLACK_CHANNEL = "#deploy"
DEFAULT_STUCK_BOUNCE_RUNBOOK = "y/stuckbounce"


log = logging.getLogger(__name__)


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    list_parser = subparsers.add_parser(
        "mark-for-deployment",
        help="Mark a docker image for deployment in git",
        description=(
            "'paasta mark-for-deployment' uses Git as the control-plane, to "
            "signal to other PaaSTA components that a particular docker image "
            "is ready to be deployed."
        ),
        epilog=(
            "Note: Access and credentials to the Git repo of a service are required "
            "for this command to work."
        ),
    )
    list_parser.add_argument(
        "-u",
        "--git-url",
        help=(
            "Git url for service -- where magic mark-for-deployment tags are pushed. "
            "Defaults to the normal git URL for the service."
        ),
        default=None,
    )
    list_parser.add_argument(
        "-c",
        "-k",
        "--commit",
        help="Git sha to mark for deployment",
        required=True,
        type=validate_short_git_sha,
    )
    arg_deploy_group = list_parser.add_argument(
        "-l",
        "--deploy-group",
        "--clusterinstance",
        help="Mark the service ready for deployment in this deploy group (e.g. "
        "cluster1.canary, cluster2.main). --clusterinstance is deprecated and "
        "should be replaced with --deploy-group",
        required=True,
    )
    arg_deploy_group.completer = lazy_choices_completer(list_deploy_groups)  # type: ignore
    arg_service = list_parser.add_argument(
        "-s",
        "--service",
        help="Name of the service which you wish to mark for deployment. Leading "
        '"services-" will be stripped.',
        required=True,
    )
    arg_service.completer = lazy_choices_completer(list_services)  # type: ignore
    list_parser.add_argument(
        "--verify-image-exists",
        help="Check the docker registry and verify the image has been pushed",
        dest="verify_image",
        action="store_true",
        default=False,
    )
    list_parser.add_argument(
        "--wait-for-deployment",
        help="Set to poll paasta and wait for the deployment to finish, "
        "the default strategy is to mark for deployment and exit straightaway",
        dest="block",
        action="store_true",
        default=False,
    )
    list_parser.add_argument(
        "-t",
        "--timeout",
        dest="timeout",
        type=int,
        default=DEFAULT_DEPLOYMENT_TIMEOUT,
        help=(
            "Time in seconds to wait for paasta to deploy the service. "
            "If the timeout is exceeded we return 1. "
            "Default is %(default)s seconds."
        ),
    )
    list_parser.add_argument(
        "-w",
        "--warn",
        dest="warn",
        type=int,
        default=DEFAULT_WARN_PERCENT,
        help=(
            "Percent of timeout to warn at if the deployment hasn't finished. "
            "For example, --warn=75 will warn at 75%% of the timeout. "
            "Defaults to %(default)s."
        ),
    )
    list_parser.add_argument(
        "--auto-rollback",
        help="Automatically roll back to the previously deployed sha if the deployment "
        "times out or is canceled (ctrl-c). Only applicable with --wait-for-deployment. "
        "Defaults to false.",
        dest="auto_rollback",
        action="store_true",
        default=False,
    )
    list_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        default=0,
        help="Print out more output.",
    )
    list_parser.add_argument(
        "--auto-certify-delay",
        dest="auto_certify_delay",
        type=int,
        default=None,  # the logic for this is complicated. See MarkForDeploymentProcess.get_auto_certify_delay.
        help="After a deploy finishes, wait this many seconds before automatically certifying."
        f"Default {DEFAULT_AUTO_CERTIFY_DELAY} when --auto-rollback is enabled",
    )
    list_parser.add_argument(
        "--auto-abandon-delay",
        dest="auto_abandon_delay",
        type=int,
        default=600,
        help="After a rollback finishes, wait this many seconds before automatically abandoning.",
    )
    list_parser.add_argument(
        "--auto-rollback-delay",
        dest="auto_rollback_delay",
        type=int,
        default=30,
        help="After noticing an SLO failure, wait this many seconds before automatically rolling back.",
    )
    list_parser.add_argument(
        "--author",
        dest="authors",
        default=None,
        action="append",
        help="Additional author(s) of the deploy, who will be pinged in Slack",
    )
    list_parser.add_argument(
        "--polling-interval",
        dest="polling_interval",
        type=float,
        default=None,
        help="How long to wait between each time we check to see if an instance is done deploying.",
    )
    list_parser.add_argument(
        "--diagnosis-interval",
        dest="diagnosis_interval",
        type=float,
        default=None,
        help="How long to wait between diagnoses of why the bounce isn't done.",
    )
    list_parser.add_argument(
        "--time-before-first-diagnosis",
        dest="time_before_first_diagnosis",
        type=float,
        default=None,
        help="Wait this long before trying to diagnose why the bounce isn't done.",
    )

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def mark_for_deployment(
    git_url: str, deploy_group: str, service: str, commit: str
) -> int:
    """Mark a docker image for deployment"""
    tag = get_paasta_tag_from_deploy_group(
        identifier=deploy_group, desired_state="deploy"
    )
    remote_tag = format_tag(tag)
    ref_mutator = remote_git.make_force_push_mutate_refs_func(
        targets=[remote_tag], sha=commit
    )

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            remote_git.create_remote_refs(
                git_url=git_url, ref_mutator=ref_mutator, force=True
            )
            if "yelpcorp.com" in git_url:
                trigger_deploys(service)
        except Exception as e:
            logline = f"Failed to mark {commit} for deployment in deploy group {deploy_group}! (attempt \
                        {attempt}/{max_attempts}, error: {e}) \n Have you pushed your commit?"
            _log(service=service, line=logline, component="deploy", level="event")
            time.sleep(5 * attempt)
        else:
            logline = f"Marked {commit} for deployment in deploy group {deploy_group}"
            _log(service=service, line=logline, component="deploy", level="event")

            audit_action_details = {"deploy_group": deploy_group, "commit": commit}
            _log_audit(
                action="mark-for-deployment",
                action_details=audit_action_details,
                service=service,
            )

            return 0
    return 1


def deploy_authz_check(deploy_info: Dict[str, Any], service: str) -> None:
    deploy_username = get_username()
    system_paasta_config = load_system_paasta_config()
    allowed_groups = (
        deploy_info["allowed_push_groups"]
        if deploy_info.get("allowed_push_groups") is not None
        else system_paasta_config.get_default_push_groups()
    )
    if allowed_groups is not None:
        search_base = system_paasta_config.get_ldap_search_base()
        search_ou = system_paasta_config.get_ldap_search_ou()
        host = system_paasta_config.get_ldap_host()
        ldap_username = system_paasta_config.get_ldap_reader_username()
        ldap_password = system_paasta_config.get_ldap_reader_password()
        if not any(
            [
                deploy_username
                in ldap_user_search(
                    group, search_base, search_ou, host, ldap_username, ldap_password
                )
                for group in allowed_groups
            ]
        ):
            logline = f"current user is not authorized to perform this action (should be in one of {allowed_groups})"
            _log(service=service, line=logline, component="deploy", level="event")
            print(logline, file=sys.stderr)
            sys.exit(1)


def report_waiting_aborted(service: str, deploy_group: str) -> None:
    print(
        PaastaColors.red(
            "Waiting for deployment aborted."
            " PaaSTA will continue trying to deploy this code."
        )
    )
    print("If you wish to see the status, run:")
    print()
    print(f"    paasta status -s {service} -l {deploy_group} -v")
    print()


def get_authors_to_be_notified(
    git_url: str, from_sha: str, to_sha: str, authors: Optional[Collection[str]]
) -> str:
    if from_sha is None:
        return ""

    if authors:
        authors_to_notify = authors
    elif "git.yelpcorp.com" in git_url:
        ret, git_authors = remote_git.get_authors(
            git_url=git_url, from_sha=from_sha, to_sha=to_sha
        )
        if ret == 0:
            authors_to_notify = git_authors.split()
        else:
            return f"(Could not get authors: {git_authors})"
    else:
        # We have no way of getting authors on the fly if the repository is not on gitolite
        return ""

    slacky_authors = ", ".join({f"<@{a}>" for a in authors_to_notify})
    log.debug(f"Authors: {slacky_authors}")
    return f"^ {slacky_authors}"


def deploy_group_is_set_to_notify(
    deploy_info: Dict[str, Any], deploy_group: str, notify_type: str
) -> bool:
    for step in deploy_info.get("pipeline", []):
        if step.get("step", "") == deploy_group:
            # Use the specific notify_type if available else use slack_notify
            return step.get(notify_type, step.get("slack_notify", False))
    return False


def get_deploy_info(service: str, soa_dir: str) -> Dict[str, Any]:
    file_path = os.path.join(soa_dir, service, "deploy.yaml")
    return read_deploy(file_path)


def print_rollback_cmd(
    old_git_sha: str, commit: str, auto_rollback: bool, service: str, deploy_group: str
) -> None:
    if old_git_sha is not None and old_git_sha != commit and not auto_rollback:
        print()
        print("If you wish to roll back, you can run:")
        print()
        print(
            PaastaColors.bold(
                "    paasta rollback --service {} --deploy-group {} --commit {} ".format(
                    service, deploy_group, old_git_sha
                )
            )
        )


def paasta_mark_for_deployment(args: argparse.Namespace) -> None:
    """Wrapping mark_for_deployment"""
    if args.verbose:
        log.setLevel(level=logging.DEBUG)
    else:
        log.setLevel(level=logging.INFO)

    service = args.service
    if service and service.startswith("services-"):
        service = service.split("services-", 1)[1]
    validate_service_name(service, soa_dir=args.soa_dir)

    deploy_group = args.deploy_group
    in_use_deploy_groups = list_deploy_groups(service=service, soa_dir=args.soa_dir)
    _, invalid_deploy_groups = validate_given_deploy_groups(
        in_use_deploy_groups, [deploy_group]
    )

    if len(invalid_deploy_groups) == 1:
        print(
            PaastaColors.red(
                "ERROR: These deploy groups are not currently used anywhere: %s.\n"
                % (",").join(invalid_deploy_groups)
            )
        )
        print(
            PaastaColors.red(
                "This isn't technically wrong because you can mark-for-deployment before deploying there"
            )
        )
        print(
            PaastaColors.red(
                "but this is probably a typo. Did you mean one of these in-use deploy groups?:"
            )
        )
        print(PaastaColors.red("   %s" % (",").join(in_use_deploy_groups)))
        print()
        print(PaastaColors.red("Continuing regardless..."))

    if args.git_url is None:
        args.git_url = get_git_url(service=service, soa_dir=args.soa_dir)

    commit = validate_git_sha(sha=args.commit, git_url=args.git_url)

    old_git_sha = get_currently_deployed_sha(service=service, deploy_group=deploy_group)
    if old_git_sha == commit:
        print(
            "Warning: The sha asked to be deployed already matches what is set to be deployed:"
        )
        print(old_git_sha)
        print("Continuing anyway.")

    if args.verify_image:
        if not is_docker_image_already_in_registry(service, args.soa_dir, commit):
            raise ValueError(
                "Failed to find image in the registry for the following sha %s" % commit
            )

    deploy_info = get_deploy_info(service=service, soa_dir=args.soa_dir)
    deploy_authz_check(deploy_info, service)

    deploy_process = MarkForDeploymentProcess(
        service=service,
        deploy_info=deploy_info,
        deploy_group=deploy_group,
        commit=commit,
        old_git_sha=old_git_sha,
        git_url=args.git_url,
        auto_rollback=args.auto_rollback,
        block=args.block,
        soa_dir=args.soa_dir,
        timeout=args.timeout,
        warn_pct=args.warn,
        auto_certify_delay=args.auto_certify_delay,
        auto_abandon_delay=args.auto_abandon_delay,
        auto_rollback_delay=args.auto_rollback_delay,
        authors=args.authors,
        polling_interval=args.polling_interval,
        diagnosis_interval=args.diagnosis_interval,
        time_before_first_diagnosis=args.time_before_first_diagnosis,
    )
    ret = deploy_process.run()
    return ret


class Progress:
    waiting_on: Mapping[str, Collection[str]]
    percent: float

    def __init__(
        self, percent: float = 0, waiting_on: Mapping[str, Collection[str]] = None
    ) -> None:
        self.percent = percent
        self.waiting_on = waiting_on

    def human_readable(self, summary: bool) -> str:
        if self.percent != 0 and self.percent != 100 and not summary:
            s = f"{round(self.percent)}% (Waiting on {self.human_waiting_on()})"
        else:
            s = f"{round(self.percent)}%"
        return s

    def human_waiting_on(self) -> str:
        if self.waiting_on is None:
            return "N/A"
        things = []
        for cluster, instances in self.waiting_on.items():
            num_instances = len(instances)
            if num_instances == 0:
                continue
            elif num_instances == 1:
                (one_instance,) = instances
                things.append(f"`{cluster}`: `{one_instance}`")
            else:
                things.append(f"`{cluster}`: {len(instances)} instances")
        return ", ".join(things)


class MarkForDeploymentProcess(SLOSlackDeploymentProcess):
    rollback_states = ["start_rollback", "rolling_back", "rolled_back"]
    rollforward_states = ["start_deploy", "deploying", "deployed"]
    default_slack_channel = DEFAULT_SLACK_CHANNEL

    paasta_status_reminder_handle: asyncio.TimerHandle

    def __init__(
        self,
        service: str,
        deploy_info: Dict,
        deploy_group: str,
        commit: str,
        old_git_sha: str,
        git_url: str,
        auto_rollback: bool,
        block: bool,
        soa_dir: str,
        timeout: float,
        warn_pct: float,
        auto_certify_delay: float,
        auto_abandon_delay: float,
        auto_rollback_delay: float,
        authors: Optional[List[str]] = None,
        polling_interval: float = None,
        diagnosis_interval: float = None,
        time_before_first_diagnosis: float = None,
    ) -> None:
        self.service = service
        self.deploy_info = deploy_info
        self.deploy_group = deploy_group
        self.commit = commit
        self.old_git_sha = old_git_sha
        self.git_url = git_url
        self.auto_rollback = (
            auto_rollback and old_git_sha is not None and old_git_sha != commit
        )
        self.auto_rollbacks_ever_enabled = self.auto_rollback
        self.block = block
        self.soa_dir = soa_dir
        self.timeout = timeout
        self.warn_pct = warn_pct
        self.mark_for_deployment_return_code = -1
        self.auto_certify_delay = auto_certify_delay
        self.auto_abandon_delay = auto_abandon_delay
        self.auto_rollback_delay = auto_rollback_delay
        self.authors = authors
        self.polling_interval = polling_interval
        self.diagnosis_interval = diagnosis_interval
        self.time_before_first_diagnosis = time_before_first_diagnosis

        # Keep track of each wait_for_deployment task so we can cancel it.
        self.wait_for_deployment_tasks: Dict[str, asyncio.Task] = {}

        self.human_readable_status = "Waiting on mark-for-deployment to initialize..."
        self.progress = Progress()
        self.last_action = None
        self.slo_watchers: List[SLOWatcher] = []

        self.start_slo_watcher_threads(self.service, self.soa_dir)
        # Initialize Slack threads and send the first message
        super().__init__()
        self.print_who_is_running_this()

    def get_progress(self, summary: bool = False) -> str:
        return self.progress.human_readable(summary)

    def print_who_is_running_this(self) -> None:
        build_url = get_jenkins_build_output_url()
        if build_url is not None:
            message = f"(<{build_url}|Jenkins Job>)"
        else:
            message = f"(Run by `{getpass.getuser()}` on {socket.getfqdn()})"
        self.update_slack_thread(message)

    def get_authors(self) -> str:
        # In order to avoid notifying people who aren't part of the current
        # service push, we calculate authors based on commits different since
        # the current production SHA, as opposed to the old SHA on this deploy
        # group.
        #
        # This avoids situations such as:
        #   * Notifying people from a previous push which went through stagef,
        #     if the new push goes through stageg.
        #   * Notifying everybody who has committed to a repo in the past year
        #     when updating a "legacy" deploy group (e.g. for yelp-main).
        prod_deploy_group = self.deploy_info.get("production_deploy_group")
        from_sha = None
        if prod_deploy_group is not None:
            from_sha = get_currently_deployed_sha(
                service=self.service, deploy_group=prod_deploy_group
            )
        # If there's no production deploy group, or the production deploy group
        # has never been deployed to, just use the old SHA from this deploy group.
        if from_sha is None:
            from_sha = self.old_git_sha
        return get_authors_to_be_notified(
            git_url=self.git_url,
            from_sha=from_sha,
            to_sha=self.commit,
            authors=self.authors,
        )

    def ping_authors(self, message: str = None) -> None:
        if message:
            self.update_slack_thread(f"{message}\n{self.get_authors()}")
        else:
            self.update_slack_thread(self.get_authors())

    def get_slack_client(self) -> SlackClient:
        return get_slack_client().sc

    def get_slack_channel(self) -> str:
        """ Safely get some slack channel to post to. Defaults to ``DEFAULT_SLACK_CHANNEL``.
        Currently only uses the first slack channel available, and doesn't support
        multi-channel notifications. """
        if self.deploy_info.get("slack_notify", True):
            try:
                channel = self.deploy_info.get("slack_channels")[0]
                # Nightly jenkins builds will often re-deploy master. This causes Slack noise that wasn't present before
                # the auto-rollbacks work.
                if self.commit == self.old_git_sha:
                    print(
                        f"Rollback SHA matches rollforward SHA: {self.commit}, "
                        f"Sending slack notifications to {DEFAULT_SLACK_CHANNEL} instead of {channel}."
                    )
                    return DEFAULT_SLACK_CHANNEL
                else:
                    return channel
            except (IndexError, AttributeError, TypeError):
                return DEFAULT_SLACK_CHANNEL
        else:
            return DEFAULT_SLACK_CHANNEL

    def get_deployment_name(self) -> str:
        return f"Deploy of `{self.commit[:8]}` of `{self.service}` to `{self.deploy_group}`:"

    def on_enter_start_deploy(self) -> None:
        self.update_slack_status(
            f"Marking `{self.commit[:8]}` for deployment for {self.deploy_group}..."
        )
        self.mark_for_deployment_return_code = mark_for_deployment(
            git_url=self.git_url,
            deploy_group=self.deploy_group,
            service=self.service,
            commit=self.commit,
        )
        if self.mark_for_deployment_return_code != 0:
            self.trigger("mfd_failed")
        else:
            self.update_slack_thread(
                f"Marked `{self.commit[:8]}` for {self.deploy_group}."
                + (
                    "\n" + self.get_authors()
                    if self.deploy_group_is_set_to_notify("notify_after_mark")
                    else ""
                )
            )
            log.debug("triggering mfd_succeeded")
            self.trigger("mfd_succeeded")

    def schedule_paasta_status_reminder(self) -> None:
        def waiting_on_to_status(
            waiting_on: Mapping[str, Collection[str]]
        ) -> List[str]:
            if waiting_on is None:
                return [
                    f"`paasta status --service {self.service} --{self.deploy_group}` -vv"
                ]
            commands = []
            for cluster, instances in waiting_on.items():
                num_instances = len(instances)
                if num_instances == 0:
                    continue
                else:
                    commands.append(
                        f"`paasta status --service {self.service} --cluster {cluster} --instance {','.join(instances)} -vv`"
                    )
            return commands

        def times_up() -> None:
            try:
                if self.state == "deploying":
                    human_max_deploy_time = humanize.naturaldelta(
                        datetime.timedelta(seconds=self.timeout)
                    )
                    stuck_bounce_runbook = os.environ.get(
                        "STUCK_BOUNCE_RUNBOOK", DEFAULT_STUCK_BOUNCE_RUNBOOK,
                    )
                    status_commands = "\n".join(
                        waiting_on_to_status(self.progress.waiting_on)
                    )

                    self.notify_users(
                        (
                            f"It has been {self.warn_pct}% of the "
                            f"maximum deploy time ({human_max_deploy_time}), "
                            "which means the deployment may be stuck. "
                            "Here are some things you can try:\n\n"
                            f"* See {stuck_bounce_runbook} for debugging help\n"
                            f"* Run these commands to see the status of instances that "
                            "have not yet finished deploying:\n\n"
                            f"{status_commands}"
                        )
                    )
            except Exception as e:
                log.error(
                    f"Non-fatal exception encountered when processing the status reminder: {e}"
                )

        def schedule_callback() -> None:
            time_to_notify = self.timeout * self.warn_pct / 100
            self.paasta_status_reminder_handle = self.event_loop.call_later(
                time_to_notify, times_up
            )

        try:
            self.event_loop.call_soon_threadsafe(schedule_callback)
        except Exception as e:
            log.error(
                f"Non-fatal error encountered scheduling the status reminder callback: {e}"
            )

    def cancel_paasta_status_reminder(self) -> None:
        try:
            handle = self.get_paasta_status_reminder_handle()
            if handle is not None:
                handle.cancel()
                self.paasta_status_reminder_handle = None
        except Exception as e:
            log.error(
                f"Non-fatal error encountered when canceling the paasta status reminder: {e}"
            )

    def get_paasta_status_reminder_handle(self) -> Optional[asyncio.TimerHandle]:
        try:
            return self.paasta_status_reminder_handle
        except AttributeError:
            return None

    def states(self) -> Collection[str]:
        return [
            "_begin",
            "start_deploy",
            "deploying",
            "deployed",
            "mfd_failed",
            "deploy_errored",
            "deploy_cancelled",
            "start_rollback",
            "rolling_back",
            "rolled_back",
            "abandon",
            "complete",
        ]

    def start_state(self) -> str:
        return "_begin"

    def start_transition(self) -> str:
        return "start_deploy"

    def valid_transitions(self) -> Iterator[state_machine.TransitionDefinition]:
        rollback_is_possible = (
            self.old_git_sha is not None and self.old_git_sha != self.commit
        )

        yield {"source": "_begin", "dest": "start_deploy", "trigger": "start_deploy"}
        yield {
            "source": "start_deploy",
            "dest": "deploying",
            "trigger": "mfd_succeeded",
        }
        yield {"source": "deploying", "dest": "deployed", "trigger": "deploy_finished"}

        yield {
            "source": ["start_deploy", "start_rollback"],
            "dest": "mfd_failed",
            "trigger": "mfd_failed",
        }
        yield {
            "source": [s for s in self.states() if not self.is_terminal_state(s)],
            "dest": "deploy_errored",
            "trigger": "deploy_errored",
        }
        yield {
            "source": [s for s in self.states() if not self.is_terminal_state(s)],
            "dest": "deploy_cancelled",
            "trigger": "deploy_cancelled",
        }

        if rollback_is_possible:
            yield {
                "source": self.rollforward_states,
                "dest": "start_rollback",
                "trigger": "rollback_button_clicked",
                "before": self.log_user_rollback,
            }
            yield {
                "source": self.rollback_states,
                "dest": None,  # this makes it an "internal transition", effectively a noop.
                "trigger": "rollback_button_clicked",
            }
            yield {
                "source": self.rollforward_states,
                "dest": "start_rollback",
                "trigger": "rollback_slo_failure",
                "before": self.log_slo_rollback,
            }
            yield {
                "source": self.rollback_states,
                "dest": None,  # this makes it an "internal transition", effectively a noop.
                "trigger": "rollback_slo_failure",
            }
            yield {
                "source": self.rollback_states,
                "dest": "start_deploy",
                "trigger": "forward_button_clicked",
            }
            yield {
                "source": self.rollforward_states,
                "dest": None,  # this makes it an "internal transition", effectively a noop.
                "trigger": "forward_button_clicked",
            }
            yield {
                "source": "start_rollback",
                "dest": "rolling_back",
                "trigger": "mfd_succeeded",
            }
            yield {
                "source": "rolling_back",
                "dest": "rolled_back",
                "trigger": "deploy_finished",
            }

        yield {
            "source": "deployed",
            "dest": "complete",
            "trigger": "complete_button_clicked",
        }
        yield {"source": "deployed", "dest": "complete", "trigger": "auto_certify"}
        yield {
            "source": ["rolled_back", "rolling_back"],
            "dest": "abandon",
            "trigger": "abandon_button_clicked",
        }
        yield {"source": "rolled_back", "dest": "abandon", "trigger": "auto_abandon"}

        if rollback_is_possible:
            # Suppress these buttons if it doesn't make sense to roll back.
            yield {
                "source": "*",
                "dest": None,  # Don't actually change state, just call the before function.
                "trigger": "enable_auto_rollbacks_button_clicked",
                "unless": [self.auto_rollbacks_enabled],
                "before": self.enable_auto_rollbacks,
            }
            yield {
                "source": "*",
                "dest": None,  # Don't actually change state, just call the before function.
                "trigger": "disable_auto_rollbacks_button_clicked",
                "conditions": [self.any_slo_failing, self.auto_rollbacks_enabled],
                "before": self.disable_auto_rollbacks,
            }
        yield {
            "source": "*",
            "dest": None,
            "trigger": "slos_started_failing",
            "conditions": [self.auto_rollbacks_enabled],
            "unless": [self.already_rolling_back],
            "before": self.start_auto_rollback_countdown,
        }
        yield {
            "source": "*",
            "dest": None,
            "trigger": "slos_stopped_failing",
            "before": self.cancel_auto_rollback_countdown,
        }
        yield {
            "source": "*",
            "dest": None,
            "trigger": "snooze_button_clicked",
            "before": self.restart_timer,
            "conditions": [self.is_timer_running],
        }

    def disable_auto_rollbacks(self) -> None:
        self.cancel_auto_rollback_countdown()
        self.auto_rollback = False
        self.update_slack_status(
            f"Automatic rollback disabled for this deploy. To disable this permanently for this step, edit `deploy.yaml` and set `auto_rollback: false` for the `{self.deploy_group}` step."
        )

    def enable_auto_rollbacks(self) -> None:
        self.auto_rollback = True
        self.auto_rollbacks_ever_enabled = True
        self.update_slack_status(
            f"Automatic rollback enabled for this deploy. Will watch for failures and rollback when necessary. To set this permanently, edit `deploy.yaml` and set `auto_rollback: false` for the `{self.deploy_group}` step."
        )

    def auto_rollbacks_enabled(self) -> bool:
        """This getter exists so it can be a condition on transitions, since those need to be callables."""
        return self.auto_rollback

    def get_auto_rollback_delay(self) -> float:
        return self.auto_rollback_delay

    def get_auto_certify_delay(self) -> float:
        if self.auto_certify_delay is not None:
            return self.auto_certify_delay
        else:
            if self.auto_rollbacks_ever_enabled:
                return DEFAULT_AUTO_CERTIFY_DELAY
            else:
                return 0

    def already_rolling_back(self) -> bool:
        return self.state in self.rollback_states

    def status_code_by_state(self) -> Mapping[str, int]:
        codes = {
            "deploy_errored": 2,
            "deploy_cancelled": 1,
            "mfd_failed": self.mark_for_deployment_return_code,
            "abandon": 1,
            "complete": 0,
        }

        if not self.block:
            # If we don't pass --wait-for-deployment, then exit immediately after mark-for-deployment succeeds.
            codes["deploying"] = 0
        if self.get_auto_certify_delay() <= 0:
            # Instead of setting a 0-second timer to move to certify, just exit 0 when the deploy finishes.
            codes["deployed"] = 0

        return codes

    def get_active_button(self) -> Optional[str]:
        return {
            "start_deploy": "forward",
            "deploying": "forward",
            "deployed": None,
            "start_rollback": "rollback",
            "rolling_back": "rollback",
            "rolled_back": None,
        }.get(self.state)

    def on_enter_mfd_failed(self) -> None:
        self.update_slack_status(
            f"Marking `{self.commit[:8]}` for deployment for {self.deploy_group} failed. Please see Jenkins for more output."
        )  # noqa E501

    def on_enter_deploying(self) -> None:
        # if self.block is False, then deploying is a terminal state so we will promptly exit.
        # Don't bother starting the background thread in this case.
        if self.block:
            thread = Thread(
                target=self.do_wait_for_deployment, args=(self.commit,), daemon=True
            )
            thread.start()
            self.cancel_paasta_status_reminder()
            self.schedule_paasta_status_reminder()

    def on_exit_deploying(self) -> None:
        self.stop_waiting_for_deployment(self.commit)
        self.cancel_paasta_status_reminder()

    def on_enter_start_rollback(self) -> None:
        self.update_slack_status(
            f"Rolling back ({self.deploy_group}) to {self.old_git_sha}"
        )
        self.mark_for_deployment_return_code = mark_for_deployment(
            git_url=self.git_url,
            deploy_group=self.deploy_group,
            service=self.service,
            commit=self.old_git_sha,
        )

        if self.mark_for_deployment_return_code != 0:
            self.trigger("mfd_failed")
        else:
            self.update_slack_thread(
                f"Marked `{self.old_git_sha[:8]}` for {self.deploy_group}."
                + (
                    "\n" + self.get_authors()
                    if self.deploy_group_is_set_to_notify("notify_after_mark")
                    else ""
                )
            )

            self.trigger("mfd_succeeded")

    def on_enter_rolling_back(self) -> None:
        if self.block:
            thread = Thread(
                target=self.do_wait_for_deployment,
                args=(self.old_git_sha,),
                daemon=True,
            )
            thread.start()

    def on_exit_rolling_back(self) -> None:
        self.stop_waiting_for_deployment(self.old_git_sha)

    def on_enter_deploy_errored(self) -> None:
        report_waiting_aborted(self.service, self.deploy_group)
        self.update_slack_status(f"Deploy aborted, but it will still try to converge.")
        self.send_manual_rollback_instructions()
        if self.deploy_group_is_set_to_notify("notify_after_abort"):
            self.ping_authors("Deploy errored")

    def on_enter_deploy_cancelled(self) -> None:
        if self.deploy_group_is_set_to_notify("notify_after_abort"):
            self.ping_authors("Deploy cancelled")

    def stop_waiting_for_deployment(self, target_commit: str) -> None:
        try:
            self.wait_for_deployment_tasks[target_commit].cancel()
            del self.wait_for_deployment_tasks[target_commit]
        except (KeyError, asyncio.InvalidStateError):
            pass

    @a_sync.to_blocking
    async def do_wait_for_deployment(self, target_commit: str) -> None:
        try:
            self.stop_waiting_for_deployment(target_commit)
            wait_for_deployment_task = asyncio.create_task(
                wait_for_deployment(
                    service=self.service,
                    deploy_group=self.deploy_group,
                    git_sha=target_commit,
                    soa_dir=self.soa_dir,
                    timeout=self.timeout,
                    progress=self.progress,
                    polling_interval=self.polling_interval,
                    diagnosis_interval=self.diagnosis_interval,
                    time_before_first_diagnosis=self.time_before_first_diagnosis,
                    notify_fn=self.ping_authors,
                )
            )
            self.wait_for_deployment_tasks[target_commit] = wait_for_deployment_task
            await wait_for_deployment_task
            if self.deploy_group_is_set_to_notify("notify_after_wait"):
                self.ping_authors(f"Finished waiting for deployment of {target_commit}")
            else:
                self.update_slack_thread(
                    f"Finished waiting for deployment of {target_commit}"
                )
            self.trigger("deploy_finished")

        except (KeyboardInterrupt, TimeoutError):
            self.trigger("deploy_cancelled")
        except NoSuchCluster:
            self.trigger("deploy_errored")
        except asyncio.CancelledError:
            # Don't trigger deploy_errored when someone calls stop_waiting_for_deployment.
            pass
        except Exception:
            log.error("Caught exception in wait_for_deployment:")
            log.error(traceback.format_exc())
            self.trigger("deploy_errored")

    def on_enter_rolled_back(self) -> None:
        self.update_slack_status(
            f"Finished rolling back to `{self.old_git_sha[:8]}` in {self.deploy_group}"
        )
        line = f"Rollback to {self.old_git_sha[:8]} for {self.deploy_group} complete"
        _log(service=self.service, component="deploy", line=line, level="event")
        self.start_timer(self.auto_abandon_delay, "auto_abandon", "abandon")

    def on_enter_deployed(self) -> None:
        self.update_slack_status(
            f"Finished deployment of `{self.commit[:8]}` to {self.deploy_group}"
        )
        line = f"Deployment of {self.commit[:8]} for {self.deploy_group} complete"
        _log(service=self.service, component="deploy", line=line, level="event")
        self.send_manual_rollback_instructions()
        if self.any_slo_failing() and self.auto_rollbacks_enabled():
            self.ping_authors(
                "Because an SLO is currently failing, we will not automatically certify. Instead, we will wait indefinitely until you click one of the buttons above."
            )
        else:
            if self.get_auto_certify_delay() > 0:
                self.start_timer(
                    self.get_auto_certify_delay(), "auto_certify", "certify"
                )
                if self.deploy_group_is_set_to_notify("notify_after_good_deploy"):
                    self.ping_authors()

    def on_enter_complete(self) -> None:
        if self.deploy_group_is_set_to_notify("notify_after_good_deploy"):
            self.ping_authors()

    def send_manual_rollback_instructions(self) -> None:
        if self.old_git_sha != self.commit:
            message = (
                "If you need to roll back manually, run: "
                f"`paasta rollback --service {self.service} --deploy-group {self.deploy_group} "
                f"--commit {self.old_git_sha}`"
            )
            self.update_slack_thread(message)
            print(message)

    def after_state_change(self) -> None:
        self.update_slack()
        super().after_state_change()

    def get_signalfx_api_token(self) -> str:
        return (
            load_system_paasta_config()
            .get_monitoring_config()
            .get("signalfx_api_key", None)
        )

    def get_button_text(self, button: str, is_active: bool) -> str:
        active_button_texts = {
            "forward": f"Rolling Forward to {self.commit[:8]} :zombocom:"
        }
        inactive_button_texts = {
            "forward": f"Continue Forward to {self.commit[:8]} :arrow_forward:",
            "complete": f"Complete deploy to {self.commit[:8]} :white_check_mark:",
            "snooze": f"Reset countdown",
            "enable_auto_rollbacks": "Enable auto rollbacks :eyes:",
            "disable_auto_rollbacks": "Disable auto rollbacks :close_eyes_monkey:",
        }

        if self.old_git_sha is not None:
            active_button_texts.update(
                {"rollback": f"Rolling Back to {self.old_git_sha[:8]} :zombocom:"}
            )
            inactive_button_texts.update(
                {
                    "rollback": f"Roll Back to {self.old_git_sha[:8]} :arrow_backward:",
                    "abandon": f"Abandon deploy, staying on {self.old_git_sha[:8]} :x:",
                }
            )

        return (active_button_texts if is_active else inactive_button_texts)[button]

    def start_auto_rollback_countdown(self, extra_text: str = "") -> None:
        cancel_button_text = self.get_button_text(
            "disable_auto_rollbacks", is_active=False
        )
        super().start_auto_rollback_countdown(
            extra_text=f'Click "{cancel_button_text}" to cancel this!'
        )
        if self.deploy_group_is_set_to_notify("notify_after_auto_rollback"):
            self.ping_authors()

    def deploy_group_is_set_to_notify(self, notify_type: str) -> bool:
        return deploy_group_is_set_to_notify(
            self.deploy_info, self.deploy_group, notify_type
        )

    def __build_rollback_audit_details(
        self, rollback_type: RollbackTypes
    ) -> Dict[str, str]:
        return {
            "rolled_back_from": self.commit,
            "rolled_back_to": self.old_git_sha,
            "rollback_type": rollback_type.value,
            "deploy_group": self.deploy_group,
        }

    def log_slo_rollback(self) -> None:
        _log_audit(
            action="rollback",
            action_details=self.__build_rollback_audit_details(
                RollbackTypes.AUTOMATIC_SLO_ROLLBACK
            ),
            service=self.service,
        )

    def log_user_rollback(self) -> None:
        _log_audit(
            action="rollback",
            action_details=self.__build_rollback_audit_details(
                RollbackTypes.USER_INITIATED_ROLLBACK
            ),
            service=self.service,
        )


async def wait_until_instance_is_done(
    executor: concurrent.futures.Executor,
    service: str,
    instance: str,
    cluster: str,
    git_sha: str,
    instance_config: LongRunningServiceConfig,
    polling_interval: float,
    diagnosis_interval: float,
    time_before_first_diagnosis: float,
    should_ping_for_unhealthy_pods: bool,
    notify_fn: Optional[Callable[[str], None]] = None,
) -> Tuple[str, str]:
    loop = asyncio.get_running_loop()
    diagnosis_task = asyncio.create_task(
        periodically_diagnose_instance(
            executor,
            service,
            instance,
            cluster,
            git_sha,
            instance_config,
            diagnosis_interval,
            time_before_first_diagnosis,
            should_ping_for_unhealthy_pods,
            notify_fn,
        )
    )
    try:
        while not await loop.run_in_executor(
            executor,
            functools.partial(
                check_if_instance_is_done,
                service,
                instance,
                cluster,
                git_sha,
                instance_config,
            ),
        ):
            await asyncio.sleep(polling_interval)
        return (
            cluster,
            instance,
        )  # for the convenience of the caller, to know which future is finishing.
    finally:
        diagnosis_task.cancel()


async def periodically_diagnose_instance(
    executor: concurrent.futures.Executor,
    service: str,
    instance: str,
    cluster: str,
    git_sha: str,
    instance_config: LongRunningServiceConfig,
    diagnosis_interval: float,
    time_before_first_diagnosis: float,
    should_ping_for_unhealthy_pods: bool,
    notify_fn: Optional[Callable[[str], None]] = None,
) -> None:
    await asyncio.sleep(time_before_first_diagnosis)
    loop = asyncio.get_running_loop()
    while True:
        try:
            await loop.run_in_executor(
                executor,
                functools.partial(
                    diagnose_why_instance_is_stuck,
                    service,
                    instance,
                    cluster,
                    git_sha,
                    instance_config,
                    should_ping_for_unhealthy_pods,
                    notify_fn,
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            print(f"Couldn't get status of {service}.{instance}:")
            traceback.print_exc()
        await asyncio.sleep(diagnosis_interval)


def diagnose_why_instance_is_stuck(
    service: str,
    instance: str,
    cluster: str,
    git_sha: str,
    instance_config: LongRunningServiceConfig,
    should_ping_for_unhealthy_pods: bool,
    notify_fn: Optional[Callable[[str], None]] = None,
) -> None:
    api = client.get_paasta_oapi_client(cluster=cluster)
    try:
        status = api.service.status_instance(
            service=service,
            instance=instance,
            include_smartstack=False,
            include_envoy=False,
            include_mesos=False,
            new=True,
        )
    except api.api_error as e:
        log.warning(
            "Error getting service status from PaaSTA API for "
            f"{cluster}: {e.status} {e.reason}"
        )
        return

    print(f"  Status for {service}.{instance} in {cluster}:")
    for version in status.kubernetes_v2.versions:
        # We call get_version_table_entry directly so that we can set version_name_suffix based on git_sha instead of
        # creation time of the version (which is what get_versions_table does.)
        # Without this, we'd call the old version "new" until the new version is actually created, which would be confusing.
        for line in get_version_table_entry(
            version,
            service,
            instance,
            cluster,
            version_name_suffix="new" if version.git_sha == git_sha else "old",
            show_config_sha=True,
            verbose=0,
        ):
            print(f"    {line}")
    print("")

    if should_ping_for_unhealthy_pods and notify_fn:
        maybe_ping_for_unhealthy_pods(
            service, instance, cluster, git_sha, status, notify_fn
        )


already_pinged = False


def maybe_ping_for_unhealthy_pods(
    service: str,
    instance: str,
    cluster: str,
    git_sha: str,
    status: InstanceStatusKubernetesV2,
    notify_fn: Callable[[str], None],
) -> None:
    global already_pinged

    if not already_pinged:
        # there can be multiple current versions, e.g. if someone changes yelpsoa-configs during a bounce.
        current_versions = [
            v for v in status.kubernetes_v2.versions if v.git_sha == git_sha
        ]
        pingable_pods = [
            pod
            for version in current_versions
            for pod in version.pods
            if should_ping_for_pod(pod)
        ]
        if pingable_pods:
            already_pinged = True
            ping_for_pods(service, instance, cluster, pingable_pods, notify_fn)


def should_ping_for_pod(pod: KubernetesPodV2) -> bool:
    return recent_container_restart(get_main_container(pod))


def ping_for_pods(
    service: str,
    instance: str,
    cluster: str,
    pods: List[KubernetesPodV2],
    notify_fn: Callable[[str], None],
) -> None:
    pods_by_reason: Dict[str, List[KubernetesPodV2]] = {}
    for pod in pods:
        pods_by_reason.setdefault(get_main_container(pod).reason, []).append(pod)

    for reason, pods_with_reason in pods_by_reason.items():
        explanation = {
            "Error": "crashed on startup",
            "OOMKilled": "run out of memory",
            "CrashLoopBackOff": "crashed on startup several times, and Kubernetes is backing off restarting them",
        }.get(reason, f"restarted ({reason})")

        status_tip = f"Take a look at the output of your unhealthy pods with `paasta status -s {service} -i {instance} -c {cluster} -vv` (more -v for more output.)"

        tip = {
            "Error": (
                f"This may indicate a bug in your code, a misconfiguration in yelpsoa-configs, or missing srv-configs. {status_tip}"
            ),
            "CrashLoopBackOff": f"This may indicate a bug in your code, a misconfiguration in yelpsoa-configs, or missing srv-configs. {status_tip}",
            "OOMKilled": " ".join(
                (
                    "This probably means your new version of code requires more memory than the old version."
                    "You may want to increase memory in yelpsoa-configs or roll back."
                    "Ask #paasta if you need help with this.",
                )
            ),
        }.get(reason, "")

        notify_fn(
            f"Some of the replicas of your new version have {explanation}: {', '.join(f'`{p.name}`' for p in pods_with_reason)}\n{tip}"
        )


def check_if_instance_is_done(
    service: str,
    instance: str,
    cluster: str,
    git_sha: str,
    instance_config: LongRunningServiceConfig,
    api: Optional[client.PaastaOApiClient] = None,
) -> bool:
    if api is None:
        api = client.get_paasta_oapi_client(cluster=cluster)
        if not api:
            log.warning(
                "Couldn't reach the PaaSTA api for {}! Assuming it is not "
                "deployed there yet.".format(cluster)
            )
            return False

    inst_str = f"{service}.{instance} in {cluster}"
    log.debug(f"Inspecting the deployment status of {inst_str}")

    status = None
    try:
        status = api.service.bounce_status_instance(service=service, instance=instance)
    except api.api_error as e:
        if e.status == 404:  # non-existent instance
            # TODO(PAASTA-17290): just print the error message so that we
            # can distinguish between sources of 404s
            log.warning(
                "Can't get status for instance {}, service {} in "
                "cluster {}. This is normally because it is a new "
                "service that hasn't been deployed by PaaSTA yet.".format(
                    instance, service, cluster
                )
            )
        else:  # 500 - error talking to api
            log.warning(
                "Error getting service status from PaaSTA API for "
                f"{cluster}: {e.status} {e.reason}"
            )

        log.debug(f"No status for {inst_str}. Not deployed yet.")
        return False

    if not status:  # 204 - instance is not bounceable
        log.debug(
            f"{inst_str} is not a supported bounceable instance. "
            "Only long-running instances running on Kubernetes are currently "
            "supported. Continuing without watching."
        )
        return True

    # Case: instance is stopped
    if status.expected_instance_count == 0 or status.desired_state == "stop":
        log.debug(f"{inst_str} is marked as stopped. Ignoring it.")
        return True

    short_git_sha = git_sha[:8]
    active_shas = {g[:8] for g, c in status.active_shas}
    if short_git_sha in active_shas:
        non_desired_shas = active_shas.difference({short_git_sha})
        # Case: bounce in-progress
        if len(non_desired_shas) == 1:
            (other_sha,) = non_desired_shas
            print(
                f"  {inst_str} is still bouncing, from {other_sha} to {short_git_sha}"
            )
            return False

        # Case: previous bounces not yet finished when this one was triggered
        elif len(non_desired_shas) > 1:
            print(
                f"  {inst_str} is still bouncing to {short_git_sha}, but there are "
                f"multiple other bouncing versions running: {non_desired_shas}"
            )
            return False
    else:
        # Case: bounce not yet started
        print(
            f"  {inst_str} hasn't started bouncing to {short_git_sha}; "
            f"only the following versions are running: {active_shas}"
        )
        return False

    # Case: instance is in not running
    if status.deploy_status not in {"Running", "Deploying", "Waiting"}:
        print(
            f"  {inst_str} isn't running yet; it is in the state: {status.deploy_status}"
        )
        return False

    # Case: not enough replicas are up for the instance to be considered bounced
    # The bounce margin factor defines what proportion of instances we need to be "safe",
    # so consider it scaled up "enough" if we have that proportion of instances ready.
    required_instance_count = int(
        math.ceil(
            instance_config.get_bounce_margin_factor() * status.expected_instance_count
        )
    )
    if required_instance_count > status.running_instance_count:
        print(
            f"  {inst_str} has only {status.running_instance_count} replicas up, "
            f"below the required minimum of {required_instance_count}"
        )
        return False

    # Case: completed
    print(
        f"Complete: {service}.{instance} on {cluster} is 100% deployed at "
        f"{status.running_instance_count} replicas on {status.active_shas[0][0]}"
    )
    return True


WAIT_FOR_INSTANCE_CLASSES = [
    MarathonServiceConfig,
    KubernetesDeploymentConfig,
    CassandraClusterDeploymentConfig,
]


def get_instance_configs_for_service_in_cluster_and_deploy_group(
    service_configs: PaastaServiceConfigLoader, cluster: str, deploy_group: str
) -> Iterator[LongRunningServiceConfig]:
    for instance_class in WAIT_FOR_INSTANCE_CLASSES:
        for instance_config in service_configs.instance_configs(
            cluster=cluster, instance_type_class=instance_class
        ):
            if instance_config.get_deploy_group() == deploy_group:
                yield instance_config


def get_instance_configs_for_service_in_deploy_group_all_clusters(
    service: str, deploy_group: str, git_sha: str, soa_dir: str
) -> Dict[str, List[LongRunningServiceConfig]]:
    service_configs = PaastaServiceConfigLoader(
        service=service, soa_dir=soa_dir, load_deployments=False
    )

    instance_configs_per_cluster = {}

    api_endpoints = load_system_paasta_config().get_api_endpoints()
    for cluster in service_configs.clusters:
        if cluster not in api_endpoints:
            print(
                PaastaColors.red(
                    "Cluster %s is NOT in paasta-api endpoints config." % cluster
                )
            )
            raise NoSuchCluster

        instance_configs_per_cluster[cluster] = list(
            get_instance_configs_for_service_in_cluster_and_deploy_group(
                service_configs, cluster, deploy_group
            )
        )

    return instance_configs_per_cluster


async def wait_for_deployment(
    service: str,
    deploy_group: str,
    git_sha: str,
    soa_dir: str,
    timeout: float,
    progress: Optional[Progress] = None,
    polling_interval: float = None,
    diagnosis_interval: float = None,
    time_before_first_diagnosis: float = None,
    notify_fn: Optional[Callable[[str], None]] = None,
) -> Optional[int]:
    instance_configs_per_cluster: Dict[
        str, List[LongRunningServiceConfig]
    ] = get_instance_configs_for_service_in_deploy_group_all_clusters(
        service, deploy_group, git_sha, soa_dir
    )
    total_instances = sum(len(ics) for ics in instance_configs_per_cluster.values())

    if not instance_configs_per_cluster:
        _log(
            service=service,
            component="deploy",
            line=(
                "Couldn't find any long-running instances for service {} in deploy group {}. Exiting.".format(
                    service, deploy_group
                )
            ),
            level="event",
        )
        return None

    print(
        "Waiting for deployment of {} for '{}' to complete...".format(
            git_sha, deploy_group
        )
    )

    system_paasta_config = load_system_paasta_config()
    max_workers = system_paasta_config.get_mark_for_deployment_max_polling_threads()
    if polling_interval is None:
        polling_interval = (
            system_paasta_config.get_mark_for_deployment_default_polling_interval()
        )
    if diagnosis_interval is None:
        diagnosis_interval = (
            system_paasta_config.get_mark_for_deployment_default_diagnosis_interval()
        )
    if time_before_first_diagnosis is None:
        time_before_first_diagnosis = (
            system_paasta_config.get_mark_for_deployment_default_time_before_first_diagnosis()
        )

    with progressbar.ProgressBar(maxval=total_instances) as bar:
        instance_done_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for cluster, instance_configs in instance_configs_per_cluster.items():
                for instance_config in instance_configs:
                    instance_done_futures.append(
                        asyncio.ensure_future(
                            wait_until_instance_is_done(
                                executor,
                                service,
                                instance_config.get_instance(),
                                cluster,
                                git_sha,
                                instance_config,
                                polling_interval=polling_interval,
                                diagnosis_interval=diagnosis_interval,
                                time_before_first_diagnosis=time_before_first_diagnosis,
                                should_ping_for_unhealthy_pods=instance_config.get_should_ping_for_unhealthy_pods(
                                    system_paasta_config.get_mark_for_deployment_should_ping_for_unhealthy_pods()
                                ),
                                notify_fn=notify_fn,
                            ),
                        )
                    )

            remaining_instances: Dict[str, Set[str]] = {
                cluster: {ic.get_instance() for ic in instance_configs}
                for cluster, instance_configs in instance_configs_per_cluster.items()
            }
            finished_instances = 0

            async def periodically_update_progressbar() -> None:
                while True:
                    await asyncio.sleep(60)
                    bar.update(finished_instances)
                    print()

            periodically_update_progressbar_task = asyncio.create_task(
                periodically_update_progressbar()
            )

            try:
                for coro in asyncio.as_completed(
                    instance_done_futures, timeout=timeout
                ):
                    cluster, instance = await coro
                    finished_instances += 1
                    bar.update(finished_instances)
                    if progress is not None:
                        progress.percent = bar.percentage
                        remaining_instances[cluster].remove(instance)
                        progress.waiting_on = remaining_instances
            except asyncio.TimeoutError:
                _log(
                    service=service,
                    component="deploy",
                    line=compose_timeout_message(
                        remaining_instances, timeout, deploy_group, service, git_sha
                    ),
                    level="event",
                )
                raise TimeoutError
            except asyncio.CancelledError:
                # Wait for all the tasks to finish before closing out the ThreadPoolExecutor, to avoid RuntimeError('cannot schedule new futures after shutdown')
                for coro in instance_done_futures:
                    coro.cancel()
                    try:
                        await coro
                    except asyncio.CancelledError:
                        pass
                raise
            else:
                sys.stdout.flush()
                if progress is not None:
                    progress.percent = 100.0
                    progress.waiting_on = None
                return 0
            finally:
                periodically_update_progressbar_task.cancel()


def compose_timeout_message(
    remaining_instances: Mapping[str, Collection[str]],
    timeout: float,
    deploy_group: str,
    service: str,
    git_sha: str,
) -> str:
    paasta_status = []
    paasta_logs = []
    for cluster, instances in sorted(remaining_instances.items()):
        if instances:
            joined_instances = ",".join(instances)
            paasta_status.append(
                "paasta status -c {cluster} -s {service} -i {instances}".format(
                    cluster=cluster, service=service, instances=joined_instances
                )
            )
            paasta_logs.append(
                "paasta logs -c {cluster} -s {service} -i {instances} -C deploy -l 1000".format(
                    cluster=cluster, service=service, instances=joined_instances
                )
            )

    return (
        "\n\nTimed out after {timeout} seconds, waiting for {service} "
        "in {deploy_group} to be deployed by PaaSTA.\n"
        "This probably means the deploy hasn't succeeded. The new service "
        "might not be healthy or one or more clusters could be having issues.\n\n"
        "To debug, follow steps in {stuck_bounce_runbook}, "
        "or try running the following to see the status of instances we tried to deploy:\n\n"
        "  {status_commands}\n\n  {logs_commands}"
        "\n\nIf the service is known to be slow to start you may wish to "
        "increase the timeout on this step.\n"
        "To wait a little longer run:\n\n"
        "  paasta wait-for-deployment -s {service} -l {deploy_group} -c {git_sha}".format(
            timeout=timeout,
            deploy_group=deploy_group,
            service=service,
            git_sha=git_sha,
            status_commands="\n  ".join(paasta_status),
            logs_commands="\n  ".join(paasta_logs),
            stuck_bounce_runbook=os.environ.get(
                "STUCK_BOUNCE_RUNBOOK", DEFAULT_STUCK_BOUNCE_RUNBOOK,
            ),
        )
    )


class NoSuchCluster(Exception):
    """To be raised by wait_for_deployment() when a service has a marathon or
    kubernetes config for a cluster that is not listed in /etc/paasta/api_endpoints.json.
    """

    pass
