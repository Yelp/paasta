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
import getpass
import logging
import math
import os
import socket
import sys
import time
import traceback
from collections import defaultdict
from queue import Empty
from queue import Queue
from threading import Event
from threading import Thread
from typing import Collection
from typing import Iterator
from typing import Mapping
from typing import Optional

import progressbar
from bravado.exception import HTTPError
from requests.exceptions import ConnectionError
from service_configuration_lib import read_deploy
from slackclient import SlackClient

from paasta_tools import remote_git
from paasta_tools.api import client
from paasta_tools.automatic_rollbacks import state_machine
from paasta_tools.automatic_rollbacks.slo import SLOSlackDeploymentProcess
from paasta_tools.cli.cmds.push_to_registry import is_docker_image_already_in_registry
from paasta_tools.cli.utils import get_jenkins_build_output_url
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import validate_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.cli.utils import validate_short_git_sha
from paasta_tools.deployment_utils import get_currently_deployed_sha
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.slack import get_slack_client
from paasta_tools.utils import _log
from paasta_tools.utils import _log_audit
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_tag
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_paasta_tag_from_deploy_group
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import TimeoutError


DEFAULT_DEPLOYMENT_TIMEOUT = 3600  # seconds
DEFAULT_AUTO_CERTIFY_DELAY = 600  # seconds
DEFAULT_SLACK_CHANNEL = "#deploy"

log = logging.getLogger(__name__)


def add_subparser(subparsers):
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
    list_parser.add_argument(
        "-l",
        "--deploy-group",
        "--clusterinstance",
        help="Mark the service ready for deployment in this deploy group (e.g. "
        "cluster1.canary, cluster2.main). --clusterinstance is deprecated and "
        "should be replaced with --deploy-group",
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        "-s",
        "--service",
        help="Name of the service which you wish to mark for deployment. Leading "
        '"services-" will be stripped.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
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

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def mark_for_deployment(git_url, deploy_group, service, commit):
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
        except Exception:
            logline = "Failed to mark {} for deployment in deploy group {}! (attempt {}/{})".format(
                commit, deploy_group, attempt, max_attempts
            )
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


def report_waiting_aborted(service, deploy_group):
    paasta_print(
        PaastaColors.red(
            "Waiting for deployment aborted."
            " PaaSTA will continue trying to deploy this code."
        )
    )
    paasta_print("If you wish to see the status, run:")
    paasta_print()
    paasta_print(f"    paasta status -s {service} -l {deploy_group} -v")
    paasta_print()


def get_authors_to_be_notified(git_url, from_sha, to_sha):
    if from_sha is None:
        return ""
    ret, authors = remote_git.get_authors(
        git_url=git_url, from_sha=from_sha, to_sha=to_sha
    )
    if ret == 0:
        if authors == "":
            return ""
        else:
            slacky_authors = ", ".join({f"<@{a}>" for a in authors.split()})
            log.debug(f"Authors: {slacky_authors}")
            return f"^ {slacky_authors}"
    else:
        return f"(Could not get authors: {authors})"


def deploy_group_is_set_to_notify(deploy_info, deploy_group, notify_type):
    for step in deploy_info.get("pipeline", []):
        if step.get("step", "") == deploy_group:
            # Use the specific notify_type if available else use slack_notify
            return step.get(notify_type, step.get("slack_notify", False))
    return False


def get_deploy_info(service, soa_dir):
    file_path = os.path.join(soa_dir, service, "deploy.yaml")
    return read_deploy(file_path)


def print_rollback_cmd(old_git_sha, commit, auto_rollback, service, deploy_group):
    if old_git_sha is not None and old_git_sha != commit and not auto_rollback:
        paasta_print()
        paasta_print("If you wish to roll back, you can run:")
        paasta_print()
        paasta_print(
            PaastaColors.bold(
                "    paasta rollback --service {} --deploy-group {} --commit {} ".format(
                    service, deploy_group, old_git_sha
                )
            )
        )


def paasta_mark_for_deployment(args):
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
        paasta_print(
            PaastaColors.red(
                "ERROR: These deploy groups are not currently used anywhere: %s.\n"
                % (",").join(invalid_deploy_groups)
            )
        )
        paasta_print(
            PaastaColors.red(
                "This isn't technically wrong because you can mark-for-deployment before deploying there"
            )
        )
        paasta_print(
            PaastaColors.red(
                "but this is probably a typo. Did you mean one of these in-use deploy groups?:"
            )
        )
        paasta_print(PaastaColors.red("   %s" % (",").join(in_use_deploy_groups)))
        paasta_print()
        paasta_print(PaastaColors.red("Continuing regardless..."))

    if args.git_url is None:
        args.git_url = get_git_url(service=service, soa_dir=args.soa_dir)

    commit = validate_git_sha(sha=args.commit, git_url=args.git_url)

    old_git_sha = get_currently_deployed_sha(service=service, deploy_group=deploy_group)
    if old_git_sha == commit:
        paasta_print(
            "Warning: The sha asked to be deployed already matches what is set to be deployed:"
        )
        paasta_print(old_git_sha)
        paasta_print("Continuing anyway.")

    if args.verify_image:
        if not is_docker_image_already_in_registry(service, args.soa_dir, commit):
            raise ValueError(
                "Failed to find image in the registry for the following sha %s" % commit
            )

    deploy_info = get_deploy_info(service=service, soa_dir=args.soa_dir)
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
        auto_certify_delay=args.auto_certify_delay,
        auto_abandon_delay=args.auto_abandon_delay,
        auto_rollback_delay=args.auto_rollback_delay,
    )
    ret = deploy_process.run()
    return ret


class Progress:
    def __init__(self, percent=0, waiting_on=None, eta=None):
        self.percent = percent
        self.waiting_on = waiting_on

    def human_readable(self, summary: bool):
        if self.percent != 0 and self.percent != 100 and not summary:
            s = f"{round(self.percent)}% (Waiting on {self.human_waiting_on()})"
        else:
            s = f"{round(self.percent)}%"
        return s

    def human_waiting_on(self):
        if self.waiting_on is None:
            return "N/A"
        things = []
        for cluster, queue in self.waiting_on.items():
            queue_length = len(queue)
            if queue_length == 0:
                continue
            elif queue_length == 1:
                things.append(f"`{cluster}`: `{queue[0].get_instance()}`")
            else:
                things.append(f"`{cluster}`: {len(queue)} instances")
        return ", ".join(things)


class MarkForDeploymentProcess(SLOSlackDeploymentProcess):
    rollback_states = ["start_rollback", "rolling_back", "rolled_back"]
    rollforward_states = ["start_deploy", "deploying", "deployed"]
    default_slack_channel = DEFAULT_SLACK_CHANNEL

    def __init__(
        self,
        service,
        deploy_info,
        deploy_group,
        commit,
        old_git_sha,
        git_url,
        auto_rollback,
        block,
        soa_dir,
        timeout,
        auto_certify_delay,
        auto_abandon_delay,
        auto_rollback_delay,
    ):
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
        self.mark_for_deployment_return_code = -1
        self.auto_certify_delay = auto_certify_delay
        self.auto_abandon_delay = auto_abandon_delay
        self.auto_rollback_delay = auto_rollback_delay

        # Separate green_light per commit, so that we can tell wait_for_deployment for one commit to shut down
        # and quickly launch wait_for_deployment for another commit without causing a race condition.
        self.wait_for_deployment_green_lights = defaultdict(Event)

        self.human_readable_status = "Waiting on mark-for-deployment to initialize..."
        self.progress = Progress()
        self.last_action = None
        self.slo_watchers = []

        self.start_slo_watcher_threads(self.service)
        # Initialize Slack threads and send the first message
        super().__init__()
        self.ping_authors()
        self.print_who_is_running_this()

    def get_progress(self, summary=False) -> str:
        return self.progress.human_readable(summary)

    def print_who_is_running_this(self):
        build_url = get_jenkins_build_output_url()
        if build_url is not None:
            message = f"(<{build_url}|Jenkins Job>)"
        else:
            message = f"(Run by <@{getpass.getuser()}> on {socket.getfqdn()})"
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
            git_url=self.git_url, from_sha=from_sha, to_sha=self.commit
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
                    paasta_print(
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

    def on_enter_start_deploy(self):
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

    def disable_auto_rollbacks(self):
        self.cancel_auto_rollback_countdown()
        self.auto_rollback = False
        self.update_slack_status(
            f"Automatic rollback disabled for this deploy. To disable this permanently for this step, edit `deploy.yaml` and set `auto_rollback: false` for the `{self.deploy_group}` step."
        )

    def enable_auto_rollbacks(self):
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

    def on_enter_mfd_failed(self):
        self.update_slack_status(
            f"Marking `{self.commit[:8]}` for deployment for {self.deploy_group} failed. Please see Jenkins for more output."
        )  # noqa E501

    def on_enter_deploying(self):
        # if self.block is False, then deploying is a terminal state so we will promptly exit.
        # Don't bother starting the background thread in this case.
        if self.block:
            thread = Thread(
                target=self.do_wait_for_deployment, args=(self.commit,), daemon=True
            )
            thread.start()

    def on_exit_deploying(self):
        self.wait_for_deployment_green_lights[self.commit].clear()

    def on_enter_start_rollback(self):
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

    def on_enter_rolling_back(self):
        if self.block:
            thread = Thread(
                target=self.do_wait_for_deployment,
                args=(self.old_git_sha,),
                daemon=True,
            )
            thread.start()

    def on_exit_rolling_back(self):
        self.wait_for_deployment_green_lights[self.old_git_sha].clear()

    def on_enter_deploy_errored(self):
        report_waiting_aborted(self.service, self.deploy_group)
        self.update_slack_status(f"Deploy aborted, but it will still try to converge.")
        self.send_manual_rollback_instructions()
        if self.deploy_group_is_set_to_notify("notify_after_abort"):
            self.ping_authors("Deploy errored")

    def on_enter_deploy_cancelled(self):
        if self.deploy_group_is_set_to_notify("notify_after_abort"):
            self.ping_authors("Deploy cancelled")

    def do_wait_for_deployment(self, target_commit: str):
        try:
            self.wait_for_deployment_green_lights[target_commit].set()
            wait_for_deployment(
                service=self.service,
                deploy_group=self.deploy_group,
                git_sha=target_commit,
                soa_dir=self.soa_dir,
                timeout=self.timeout,
                green_light=self.wait_for_deployment_green_lights[target_commit],
                progress=self.progress,
            )
            self.update_slack_thread(
                f"Finished waiting for deployment of {target_commit}"
            )
            self.trigger("deploy_finished")

        except (KeyboardInterrupt, TimeoutError):
            if self.wait_for_deployment_green_lights[target_commit].is_set():
                # When we manually trigger a rollback, we clear the green_light, which causes wait_for_deployment to
                # raise KeyboardInterrupt. Don't trigger deploy_cancelled in this case.
                self.trigger("deploy_cancelled")
        except NoSuchCluster:
            self.trigger("deploy_errored")
        except Exception:
            log.error("Caught exception in wait_for_deployment:")
            log.error(traceback.format_exc())
            self.trigger("deploy_errored")

    def on_enter_rolled_back(self):
        self.update_slack_status(
            f"Finished rolling back to `{self.old_git_sha[:8]}` in {self.deploy_group}"
        )
        line = f"Rollback to {self.old_git_sha[:8]} for {self.deploy_group} complete"
        _log(service=self.service, component="deploy", line=line, level="event")
        self.start_timer(self.auto_abandon_delay, "auto_abandon", "abandon")

    def on_enter_deployed(self):
        self.update_slack_status(
            f"Finished deployment of `{self.commit[:8]}` to {self.deploy_group}"
        )
        line = f"Deployment of {self.commit[:8]} for {self.deploy_group} complete"
        _log(service=self.service, component="deploy", line=line, level="event")
        self.send_manual_rollback_instructions()
        if not (self.any_slo_failing() and self.auto_rollbacks_enabled()):
            if self.get_auto_certify_delay() > 0:
                self.start_timer(
                    self.get_auto_certify_delay(), "auto_certify", "certify"
                )
                if self.deploy_group_is_set_to_notify("notify_after_good_deploy"):
                    self.ping_authors()

    def on_enter_complete(self):
        if self.deploy_group_is_set_to_notify("notify_after_good_deploy"):
            self.ping_authors()

    def send_manual_rollback_instructions(self):
        if self.old_git_sha != self.commit:
            message = (
                "If you need to roll back manually, run: "
                f"`paasta rollback --service {self.service} --deploy-group {self.deploy_group} "
                f"--commit {self.old_git_sha}`"
            )
            self.update_slack_thread(message)
            paasta_print(message)

    def after_state_change(self):
        self.update_slack()
        super().after_state_change()

    def get_signalfx_api_token(self) -> str:
        return (
            load_system_paasta_config()
            .get_monitoring_config()
            .get("signalfx_api_key", None)
        )

    def get_button_text(self, button, is_active) -> str:
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

    def start_auto_rollback_countdown(self, extra_text="") -> None:
        cancel_button_text = self.get_button_text(
            "disable_auto_rollbacks", is_active=False
        )
        super().start_auto_rollback_countdown(
            extra_text=f'Click "{cancel_button_text}" to cancel this!'
        )
        if self.deploy_group_is_set_to_notify("notify_after_auto_rollback"):
            self.ping_authors()

    def deploy_group_is_set_to_notify(self, notify_type):
        return deploy_group_is_set_to_notify(
            self.deploy_info, self.deploy_group, notify_type
        )


class ClusterData:
    """An auxiliary data transfer class.

    Used by _query_clusters(), instances_deployed(),
    _run_cluster_worker(), _run_instance_worker().

    :param cluster: the name of the cluster.
    :param service: the name of the service.
    :param git_sha: git sha marked for deployment.
    :param instances_queue: a thread-safe queue. Should contain all cluster
                            instances that need to be checked.
    :type instances_queue: Queue
    """

    def __init__(self, cluster, service, git_sha, instances_queue):
        self.cluster = cluster
        self.service = service
        self.git_sha = git_sha
        self.instances_queue = instances_queue


def instances_deployed(cluster_data, instances_out, green_light):
    """Create a thread pool to run _run_instance_worker()

    :param cluster_data: an instance of ClusterData.
    :param instances_out: a empty thread-safe queue. I will contain
                          instances that are not deployed yet.
    :type instances_out: Queue
    :param green_light: See the docstring for _query_clusters().
    """
    num_threads = min(5, cluster_data.instances_queue.qsize())

    workers_launched = []
    for _ in range(num_threads):
        worker = Thread(
            target=_run_instance_worker, args=(cluster_data, instances_out, green_light)
        )
        worker.start()
        workers_launched.append(worker)

    for worker in workers_launched:
        worker.join()


def _run_instance_worker(cluster_data, instances_out, green_light):
    """Get instances from the instances_in queue and check them one by one.

    If an instance isn't deployed, add it to the instances_out queue
    to re-check it later.

    :param cluster_data: an instance of ClusterData.
    :param instances_out: See the docstring for instances_deployed().
    :param green_light: See the docstring for _query_clusters().
    """

    api = client.get_paasta_api_client(cluster=cluster_data.cluster)
    if not api:
        log.warning(
            "Couldn't reach the PaaSTA api for {}! Assuming it is not "
            "deployed there yet.".format(cluster_data.cluster)
        )
        while not cluster_data.instances_queue.empty():
            try:
                instance_config = cluster_data.instances_queue.get(block=False)
            except Empty:
                return
            cluster_data.instances_queue.task_done()
            instances_out.put(instance_config)

    while not cluster_data.instances_queue.empty() and green_light.is_set():
        try:
            instance_config = cluster_data.instances_queue.get(block=False)
        except Empty:
            return

        instance = instance_config.get_instance()

        log.debug(
            "Inspecting the deployment status of {}.{} on {}".format(
                cluster_data.service, instance, cluster_data.cluster
            )
        )
        try:
            status = None
            status = api.service.status_instance(
                service=cluster_data.service,
                instance=instance,
                omit_smartstack=True,
                omit_mesos=True,
            ).result()
        except HTTPError as e:
            if e.response.status_code == 404:
                log.warning(
                    "Can't get status for instance {}, service {} in "
                    "cluster {}. This is normally because it is a new "
                    "service that hasn't been deployed by PaaSTA yet".format(
                        instance, cluster_data.service, cluster_data.cluster
                    )
                )
            else:
                log.warning(
                    "Error getting service status from PaaSTA API for {}: {}"
                    "{}".format(
                        cluster_data.cluster, e.response.status_code, e.response.text
                    )
                )
        except ConnectionError as e:
            log.warning(
                "Error getting service status from PaaSTA API for {}:"
                "{}".format(cluster_data.cluster, e)
            )

        long_running_status = None
        if status:
            if status.marathon:
                long_running_status = status.marathon
            elif status.kubernetes:
                long_running_status = status.kubernetes
        if not status:
            log.debug(
                "No status for {}.{}, in {}. Not deployed yet.".format(
                    cluster_data.service, instance, cluster_data.cluster
                )
            )
            cluster_data.instances_queue.task_done()
            instances_out.put(instance_config)
        elif not long_running_status:
            log.debug(
                "{}.{} in {} is not a Marathon or Kubernetes job. Marked as deployed.".format(
                    cluster_data.service, instance, cluster_data.cluster
                )
            )
        elif (
            long_running_status.expected_instance_count == 0
            or long_running_status.desired_state == "stop"
        ):
            log.debug(
                "{}.{} in {} is marked as stopped. Marked as deployed.".format(
                    cluster_data.service, status.instance, cluster_data.cluster
                )
            )
        else:
            if long_running_status.app_count != 1:
                paasta_print(
                    "  {}.{} on {} is still bouncing, {} versions "
                    "running".format(
                        cluster_data.service,
                        status.instance,
                        cluster_data.cluster,
                        long_running_status.app_count,
                    )
                )
                cluster_data.instances_queue.task_done()
                instances_out.put(instance_config)
                continue
            if not cluster_data.git_sha.startswith(status.git_sha):
                paasta_print(
                    "  {}.{} on {} doesn't have the right sha yet: {}".format(
                        cluster_data.service,
                        instance,
                        cluster_data.cluster,
                        status.git_sha,
                    )
                )
                cluster_data.instances_queue.task_done()
                instances_out.put(instance_config)
                continue
            if long_running_status.deploy_status not in [
                "Running",
                "Deploying",
                "Waiting",
            ]:
                paasta_print(
                    "  {}.{} on {} isn't running yet: {}".format(
                        cluster_data.service,
                        instance,
                        cluster_data.cluster,
                        long_running_status.deploy_status,
                    )
                )
                cluster_data.instances_queue.task_done()
                instances_out.put(instance_config)
                continue

            # The bounce margin factor defines what proportion of instances we need to be "safe",
            # so consider it scaled up "enough" if we have that proportion of instances ready.
            required_instance_count = int(
                math.ceil(
                    instance_config.get_bounce_margin_factor()
                    * long_running_status.expected_instance_count
                )
            )
            if required_instance_count > long_running_status.running_instance_count:
                paasta_print(
                    "  {}.{} on {} isn't scaled up yet, "
                    "has {} out of {} required instances (out of a total of {})".format(
                        cluster_data.service,
                        instance,
                        cluster_data.cluster,
                        long_running_status.running_instance_count,
                        required_instance_count,
                        long_running_status.expected_instance_count,
                    )
                )
                cluster_data.instances_queue.task_done()
                instances_out.put(instance_config)
                continue
            paasta_print(
                "Complete: {}.{} on {} looks 100% deployed at {} "
                "instances on {}".format(
                    cluster_data.service,
                    instance,
                    cluster_data.cluster,
                    long_running_status.running_instance_count,
                    status.git_sha,
                )
            )
            cluster_data.instances_queue.task_done()


def _query_clusters(clusters_data, green_light):
    """Run _run_cluster_worker() in a separate thread for each paasta cluster

    :param clusters_data: a list of ClusterData instances.
    :param green_light: an instance of threading.Event().
                        It is supposed to be cleared when KeyboardInterrupt is
                        received. All running threads should check it
                        periodically and exit when it is cleared.
    """
    workers_launched = []

    for cluster_data in clusters_data:
        if not cluster_data.instances_queue.empty():
            worker = Thread(
                target=_run_cluster_worker, args=(cluster_data, green_light)
            )
            worker.start()
            workers_launched.append(worker)

    for worker in workers_launched:
        try:
            while green_light.is_set() and worker.isAlive():
                time.sleep(0.2)
        except (KeyboardInterrupt, SystemExit):
            green_light.clear()
            paasta_print("KeyboardInterrupt received. Terminating..")
        worker.join()


def _run_cluster_worker(cluster_data, green_light):
    """Run instances_deployed() for a cluster

    :param cluster_data: an instance of ClusterData.
    :param green_light: See the docstring for _query_clusters().
    """
    instances_out = Queue()
    instances_deployed(
        cluster_data=cluster_data, instances_out=instances_out, green_light=green_light
    )
    cluster_data.instances_queue = instances_out
    if cluster_data.instances_queue.empty():
        paasta_print(f"Deploy to {cluster_data.cluster} complete!")


def wait_for_deployment(
    service, deploy_group, git_sha, soa_dir, timeout, green_light=None, progress=None
):
    # Currently only 'marathon' instances are supported for wait_for_deployment because they
    # are the only thing that are worth waiting on.
    service_configs = PaastaServiceConfigLoader(
        service=service, soa_dir=soa_dir, load_deployments=False
    )

    total_instances = 0
    clusters_data = []
    api_endpoints = load_system_paasta_config().get_api_endpoints()
    for cluster in service_configs.clusters:
        if cluster not in api_endpoints:
            paasta_print(
                PaastaColors.red(
                    "Cluster %s is NOT in paasta-api endpoints config." % cluster
                )
            )
            raise NoSuchCluster

        instances_queue = Queue()
        for instance_config in service_configs.instance_configs(
            cluster=cluster, instance_type_class=MarathonServiceConfig
        ):
            if instance_config.get_deploy_group() == deploy_group:
                instances_queue.put(instance_config)
                total_instances += 1
        for instance_config in service_configs.instance_configs(
            cluster=cluster, instance_type_class=KubernetesDeploymentConfig
        ):
            if instance_config.get_deploy_group() == deploy_group:
                instances_queue.put(instance_config)
                total_instances += 1

        if not instances_queue.empty():
            clusters_data.append(
                ClusterData(
                    cluster=cluster,
                    service=service,
                    git_sha=git_sha,
                    instances_queue=instances_queue,
                )
            )

    if not clusters_data:
        _log(
            service=service,
            component="deploy",
            line=(
                "Couldn't find any marathon instances for service {} in deploy group {}. Exiting.".format(
                    service, deploy_group
                )
            ),
            level="event",
        )
        return

    paasta_print(
        "Waiting for deployment of {} for '{}' to complete...".format(
            git_sha, deploy_group
        )
    )

    deadline = time.time() + timeout
    if green_light is None:
        green_light = Event()
    green_light.set()

    with progressbar.ProgressBar(maxval=total_instances) as bar:
        while time.time() < deadline:
            _query_clusters(clusters_data, green_light)
            if not green_light.is_set():
                raise KeyboardInterrupt

            finished_instances = total_instances - sum(
                (c.instances_queue.qsize() for c in clusters_data)
            )
            bar.update(finished_instances)
            if progress is not None:
                progress.percent = bar.percentage
                progress.waiting_on = {
                    c.cluster: list(c.instances_queue.queue) for c in clusters_data
                }

            if all((cluster.instances_queue.empty() for cluster in clusters_data)):
                sys.stdout.flush()
                if progress is not None:
                    progress.percent = 100.0
                    progress.waiting_on = None
                return 0
            else:
                time.sleep(min(60, timeout))
            sys.stdout.flush()

    _log(
        service=service,
        component="deploy",
        line=compose_timeout_message(
            clusters_data, timeout, deploy_group, service, git_sha
        ),
        level="event",
    )
    raise TimeoutError


def compose_timeout_message(clusters_data, timeout, deploy_group, service, git_sha):
    cluster_instances = {}
    for c_d in clusters_data:
        while c_d.instances_queue.qsize() > 0:
            cluster_instances.setdefault(c_d.cluster, []).append(
                c_d.instances_queue.get(block=False).get_instance()
            )
            c_d.instances_queue.task_done()

    paasta_status = []
    paasta_logs = []
    for cluster, instances in sorted(cluster_instances.items()):
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
        "To debug try running:\n\n"
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
        )
    )


class NoSuchCluster(Exception):
    """To be raised by wait_for_deployment() when a service has a marathon config for
    a cluster that is not listed in /etc/paasta/api_endpoints.json.
    """

    pass
