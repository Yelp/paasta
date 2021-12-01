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
        "-e",
        "--error-minutes",
        help="Minutes since the pod encountered an error. Terminates pods based on time since failure.",
        # this can't be required until we've rolled this out everywhere AND have updated all the callsites
        required=False,
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


def __condition_transition_longer_than_threshold(
    pod: V1Pod, condition: str, threshold: int
) -> bool:
    time_finished = get_pod_condition(pod, condition).last_transition_time
    time_now = datetime.now(tzutc())

    # convert total seconds since completion to minutes
    since_minutes = (time_now - time_finished).total_seconds() / 60

    return since_minutes > threshold


def _completed_longer_than_threshold(pod: V1Pod, threshold: int) -> bool:
    return __condition_transition_longer_than_threshold(
        pod, "ContainersReady", threshold
    )


def _scheduled_longer_than_threshold(pod: V1Pod, threshold: int) -> bool:
    return __condition_transition_longer_than_threshold(pod, "PodScheduled", threshold)


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
    allowed_error_minutes = args.error_minutes

    completed_pods = []
    errored_pods = []

    for pod in pods:
        if is_pod_completed(pod) and _completed_longer_than_threshold(
            pod, allowed_uptime_minutes
        ):
            completed_pods.append(pod)
        elif (
            # this is currently optional
            allowed_error_minutes is not None
            # there's no direct way to get what type of "bad" state these Pods ended up
            # (kubectl looks at phase and then container statuses to give something descriptive)
            # but, in the end, we really just care that a Pod is in a Failed phase
            and pod.status.phase == "Failed"
        ):
            try:
                # and that said Pod has been around for a while (generally longer than we'd leave
                # Pods that exited sucessfully)
                # NOTE: we do this in a try-except since we're intermittently seeing pods in an error
                # state without a PodScheduled condition (even though that should be impossible)
                # this is not ideal, but its fine to skip these since this isn't a critical process
                if _scheduled_longer_than_threshold(pod, allowed_error_minutes):
                    errored_pods.append(pod)
            except AttributeError:
                log.exception(
                    f"Unable to check {pod.metadata.name}'s schedule time. Pod status: {pod.status}.'"
                )

    if not (completed_pods or errored_pods):
        log.debug("No pods to terminate.")
        sys.exit(0)

    if args.dry_run:
        log.debug(
            "Dry run would have terminated the following completed pods:\n "
            + "\n ".join([pod.metadata.name for pod in completed_pods])
        )
        log.debug(
            "Dry run would have terminated the following errored pods:\n "
            + "\n ".join([pod.metadata.name for pod in errored_pods])
        )
        sys.exit(0)

    completed_successes, completed_errors = terminate_pods(completed_pods, kube_client)
    errored_successes, errored_errors = terminate_pods(errored_pods, kube_client)

    successes = {
        "completed": completed_successes,
        "errored": errored_successes,
    }
    errors = {
        "completed": completed_errors,
        "errored": errored_errors,
    }

    for typ, pod_names in successes.items():
        if pod_names:
            log.debug(
                f"Successfully terminated the following {typ} pods:\n"
                + "\n ".join(pod_names)
            )

    # we've only really seen this fail recently due to the k8s API being flaky and returning
    # 404s for Pods that its returning to us when we get all Pods, so we just print the error
    # here for now and don't exit with a non-zero exit code since, again, this isn't a critical
    # process
    for typ, pod_names_and_errors in errors.items():
        if pod_names_and_errors:
            log.error(
                f"Failed to terminate the following {typ} pods:\n"
                + "\n  ".join(
                    f"{pod_name}: {error}" for pod_name, error in pod_names_and_errors
                )
            )


if __name__ == "__main__":
    main()
