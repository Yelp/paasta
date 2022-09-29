import json
import os

from render_template import render_values


def main():
    cluster = os.environ.get(
        "PAASTA_TEST_CLUSTER", "kind-{USER}-k8s-test".format(**os.environ)
    )
    config_path = "etc_paasta_playground"

    # create an etc_paasta_playground directory if it doesn't exist
    # and copy fake_etc_paasta content into etc_paasta_playground directory
    if not os.path.isdir("etc_paasta_playground"):
        os.mkdir("etc_paasta_playground")

    render_values(
        "./k8s_itests/deployments/paasta/fake_etc_paasta", "etc_paasta_playground", None
    )

    # Generate Cluster name
    clusters_names = {"clusters": [cluster], "cluster": cluster}
    clusters_path = os.path.join(config_path, "clusters.json")
    with open(clusters_path, "w") as f:
        json.dump(clusters_names, f)

    # Add in volumes.json
    hacheck_sidecar_volumes = {"volumes": [], "hacheck_sidecar_volumes": []}
    volumes_path = os.path.join(config_path, "volumes.json")
    with open(volumes_path, "w") as f:
        json.dump(hacheck_sidecar_volumes, f)

    # create a fake_soa_config directory if it doesn't exist
    # and copy fake_soa_config content into soa_config_playground directory
    if not os.path.isdir("soa_config_playground"):
        os.mkdir("soa_config_playground")
    render_values(
        "./k8s_itests/deployments/paasta/fake_soa_config", "soa_config_playground", None
    )

    # Code below will rename kubernetes-<%cluster%>.yaml with your cluster name
    # if the cluster is not already renamed
    src_path = os.path.join(
        "soa_config_playground/compute-infra-test-service",
        "kubernetes-<%cluster%>.yaml",
    )
    if os.path.exists(src_path):
        dst_path = os.path.join(
            "soa_config_playground/compute-infra-test-service",
            f"kubernetes-{cluster}.yaml",
        )
        os.rename(src_path, dst_path)


if __name__ == "__main__":
    main()
