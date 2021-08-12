from datetime import datetime

from dateutil.tz import tzutc

from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_pod_condition
from paasta_tools.kubernetes_tools import is_pod_completed
from paasta_tools.kubernetes_tools import KubeClient


def main():
    kube_client = KubeClient()
    pods = get_all_pods(kube_client, "tron")
    minutes_until_termination = 10
    for pod in pods:
        if is_pod_completed(pod):
            time_finished = get_pod_condition(
                pod, "ContainersReady"
            ).last_transition_time
            time_now = datetime.now(tzutc())
            # convert total seconds since completion to minutes
            completed_since = (time_now - time_finished).total_seconds() / 60

            if completed_since > minutes_until_termination:
                kube_client.core.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    grace_period_seconds=0,
                    propagation_policy="Background",
                )


if __name__ == "__main__":
    main()
