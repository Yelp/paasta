import sys
from datetime import datetime

from dateutil.tz import tzutc

from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_pod_condition
from paasta_tools.kubernetes_tools import is_pod_completed
from paasta_tools.kubernetes_tools import KubeClient


def main():
    kube_client = KubeClient()
    pods = get_all_pods(kube_client, "tron")
    allowed_uptime_minutes = 10
    seconds_per_minute = 60
    successes = []
    errors = []
    for pod in pods:
        if is_pod_completed(pod):
            time_finished = get_pod_condition(
                pod, "ContainersReady"
            ).last_transition_time
            time_now = datetime.now(tzutc())
            # convert total seconds since completion to minutes
            completed_since = (
                time_now - time_finished
            ).total_seconds() / seconds_per_minute

            if completed_since > allowed_uptime_minutes:
                pod_name = pod.metadata.name
                try:
                    kube_client.core.delete_namespaced_pod(
                        name=pod.metadata.name,
                        namespace=pod.metadata.namespace,
                        grace_period_seconds=0,
                        propagation_policy="Background",
                    )
                    successes.append(pod_name)
                except Exception as e:
                    errors.append((pod_name, e))

    if successes:
        print(
            "Successfully terminated the following completed pods:\n",
            "\n ".join(successes),
        )

    if errors:
        print(
            "Failed to terminate the following completed pods:\n",
            "\n  ".join(
                [
                    "{pod_name}: {error}".format(pod_name=pod_name, error=str(error))
                    for pod_name, error in errors
                ]
            ),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
