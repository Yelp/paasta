import argparse
import logging
import sys
from datetime import datetime
from typing import Sequence

from dateutil.tz import tzutc
from kubernetes.client import V1Pod

from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_pod_condition
from paasta_tools.kubernetes_tools import is_pod_completed
from paasta_tools.kubernetes_tools import KubeClient

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Terminates completed pods based on namespace and minutes since completion"
    )
    parser.add_argument(
        "-n",
        "--namespace",
        help="Namespace of the pods to terminate from",
        required=True,
    )
    parser.add_argument(
        "-m",
        "--minutes",
        help="Minutes since the pods' completion. Terminates pods based on time since completion.",
        required=True,
        type=int,
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Print pods to be terminated, instead of terminating them",
    )
    parser.add_argument(
        "-v", "--verbose", dest="verbose", action="store_true", default=False
    )
    args = parser.parse_args()
    return args


def setup_logging(verbose):
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level)
    # Remove pod metadata logs
    logging.getLogger("kubernetes.client.rest").setLevel(logging.ERROR)


def _completed_since(pod: V1Pod, allowed_uptime_minutes: int) -> bool:
    seconds_per_minute = 60
    time_finished = get_pod_condition(pod, "ContainersReady").last_transition_time
    time_now = datetime.now(tzutc())
    # convert total seconds since completion to minutes
    completed_since = (time_now - time_finished).total_seconds() / seconds_per_minute

    return completed_since > allowed_uptime_minutes


def terminate_pods(pods: Sequence[V1Pod], kube_client) -> tuple:
    successes = []
    errors = []

    for pod in pods:
        try:
            kube_client.core.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                grace_period_seconds=0,
                propagation_policy="Background",
            )
            successes.append(pod.metadata.name)
        except Exception as e:
            errors.append((pod.metadata.name, e))

    return (successes, errors)


def main():
    args = parse_args()
    setup_logging(args.verbose)

    kube_client = KubeClient()
    pods = get_all_pods(kube_client, args.namespace)
    allowed_uptime_minutes = args.minutes
    completed_pods = []

    for pod in pods:
        if is_pod_completed(pod) and _completed_since(pod, allowed_uptime_minutes):
            completed_pods.append(pod)

    if not len(completed_pods):
        log.debug("No completed pods to terminate.")
        sys.exit(0)

    if args.dry_run:
        log.debug(
            "Dry run would have terminated the following completed pods:\n "
            + "\n ".join([pod.metadata.name for pod in completed_pods])
        )
        sys.exit(0)

    successes, errors = terminate_pods(completed_pods, kube_client)

    if successes:
        log.debug(
            "Successfully terminated the following completed pods:\n"
            + "\n ".join(successes)
        )

    if errors:
        log.error(
            "Failed to terminate the following completed pods:\n"
            + "\n  ".join(
                [
                    "{pod_name}: {error}".format(pod_name=pod_name, error=str(error))
                    for pod_name, error in errors
                ]
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
