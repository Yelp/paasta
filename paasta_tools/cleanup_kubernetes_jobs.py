#!/usr/bin/env python
# Copyright 2019-2020 Yelp Inc.
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
"""
Usage: ./cleanup_kubernetes_jobs.py [options]

Clean up kubernetes apps that aren't supposed to run on this cluster by deleting them.

Gets the current app list from kubernetes, and then a 'valid_app_list'
via utils.get_services_for_cluster

If an app in the kubernetes app list isn't in the valid_app_list, it's
deleted.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
- -t <KILL_THRESHOLD>, --kill-threshold: The decimal fraction of apps we think
    is sane to kill when this job runs
- -f, --force: Force the killing of apps if we breach the threshold
"""
import argparse
import logging
import sys
import traceback
from contextlib import contextmanager
from typing import Generator

from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet

from paasta_tools.kubernetes.application.tools import Application  # type: ignore
from paasta_tools.kubernetes.application.tools import (
    list_namespaced_applications,
)  # type: ignore
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)
APPLICATION_TYPES = [V1StatefulSet, V1Deployment]


class DontKillEverythingError(Exception):
    pass


@contextmanager
def alert_state_change(application: Application, soa_dir: str) -> Generator:
    service = application.kube_deployment.service
    instance = application.kube_deployment.instance
    cluster = load_system_paasta_config().get_cluster()
    try:
        yield
        log_line = (
            "Deleted stale Kubernetes apps that looks lost: %s"
            % application.item.metadata.name
        )
        _log(
            service=service,
            component="deploy",
            level="event",
            cluster=cluster,
            instance=instance,
            line=log_line,
        )

    except Exception:
        loglines = ["Exception raised during cleanup of service %s:" % application]
        loglines.extend(traceback.format_exc().rstrip().split("\n"))
        for logline in loglines:
            _log(
                service=service,
                component="deploy",
                level="debug",
                cluster=cluster,
                instance=instance,
                line=logline,
            )
        raise


def cleanup_unused_apps(
    soa_dir: str, kill_threshold: float = 0.5, force: bool = False
) -> None:
    """Clean up old or invalid jobs/apps from kubernetes. Retrieves
    both a list of apps currently in kubernetes and a list of valid
    app ids in order to determine what to kill.

    :param soa_dir: The SOA config directory to read from
    :param kill_threshold: The decimal fraction of apps we think is
        sane to kill when this job runs.
    :param force: Force the cleanup if we are above the kill_threshold"""
    log.info("Creating KubeClient")
    kube_client = KubeClient()

    log.info("Loading running Kubernetes apps")
    applications = list_namespaced_applications(
        kube_client, "paasta", APPLICATION_TYPES
    )

    log.info("Retrieving valid apps from yelpsoa_configs")
    valid_services = set(
        get_services_for_cluster(instance_type="kubernetes", soa_dir=soa_dir)
    )

    log.info("Determining apps to be killed")
    applications_to_kill = [
        applicaton
        for applicaton in applications
        if (applicaton.kube_deployment.service, applicaton.kube_deployment.instance)
        not in valid_services
    ]

    log.debug("Running apps: %s" % applications)
    log.debug("Valid apps: %s" % valid_services)
    log.debug("Terminating: %s" % applications_to_kill)
    if applications_to_kill:
        above_kill_threshold = float(len(applications_to_kill)) / float(
            len(applications)
        ) > float(kill_threshold)
        if above_kill_threshold and not force:
            log.critical(
                "Paasta was about to kill more than %s of the running services, this "
                "is probably a BAD mistake!, run again with --force if you "
                "really need to destroy everything" % kill_threshold
            )
            raise DontKillEverythingError

    for applicaton in applications_to_kill:
        with alert_state_change(applicaton, soa_dir):
            applicaton.deep_delete(kube_client)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Cleans up stale kubernetes jobs.")
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-t",
        "--kill-threshold",
        dest="kill_threshold",
        default=0.5,
        help="The decimal fraction of apps we think is "
        "sane to kill when this job runs",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        dest="force",
        default=False,
        help="Force the cleanup if we are above the " "kill_threshold",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    soa_dir = args.soa_dir
    kill_threshold = args.kill_threshold
    force = args.force
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    try:
        cleanup_unused_apps(soa_dir, kill_threshold=kill_threshold, force=force)
    except DontKillEverythingError:
        sys.exit(1)


if __name__ == "__main__":
    main()
