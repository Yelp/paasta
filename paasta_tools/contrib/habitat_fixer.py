#!/usr/bin/env python
import argparse
from pathlib import Path

from kubernetes.client import V1Node

from paasta_tools.kubernetes_tools import KUBE_CONFIG_PATH
from paasta_tools.kubernetes_tools import KUBE_CONFIG_USER_PATH
from paasta_tools.kubernetes_tools import KubeClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Karpenter Habitat Corruption Workaround"
    )
    parser.add_argument("-c", "--cluster", required=True)
    parser.add_argument(
        "-k",
        "--kubeconfig",
        default=KUBE_CONFIG_PATH
        if Path(KUBE_CONFIG_PATH).exists()
        else KUBE_CONFIG_USER_PATH,
    )
    parser.add_argument(
        "-t", "--context", default=None  # -c is taken, so lets use the last letter :p
    )
    parser.add_argument(
        "--for-real",
        action="store_true",
    )
    parsed_args = parser.parse_args()

    if not parsed_args.context:
        if parsed_args.kubeconfig == KUBE_CONFIG_USER_PATH:
            # in the user kubeconfig, context names are just the cluster names
            parsed_args.context = parsed_args.cluster
        else:
            print(
                f"NOTE: no context specified - will use the current context selected in {parsed_args.kubeconfig} "
                "(or the KUBECONTEXT environment variable if set)."
            )

    return parsed_args


def is_affected_node(node: V1Node) -> bool:
    try:
        int(node.metadata.labels["yelp.com/habitat"])
        return True
    except ValueError:
        return False


def get_desired_habitat(node: V1Node) -> str:
    zone = node.metadata.labels["topology.kubernetes.io/zone"].replace("-", "")
    ecosystem = node.metadata.labels["yelp.com/ecosystem"]

    return f"{zone}{ecosystem}"


def main():
    args = parse_args()
    client = KubeClient(config_file=args.kubeconfig, context=args.context)
    for node in client.core.list_node().items:
        if not is_affected_node(node):
            continue

        if args.for_real:
            client.core.patch_node(
                name=node.metadata.name,
                body={
                    "metadata": {
                        "labels": {
                            "yelp.com/habitat": get_desired_habitat(node),
                        },
                    }
                },
            )
        else:
            print(
                f"Would have edited {node.metadata.name} in pool={node.metadata.labels['yelp.com/pool']} to have habitat={get_desired_habitat(node)} (from {node.metadata.labels['yelp.com/habitat']})",
            )


if __name__ == "__main__":
    main()
