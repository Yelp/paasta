import json
import os
import subprocess


def main():
    cluster = os.environ.get("PAASTA_TEST_CLUSTER", "kind-emanelsabban-k8s-test")
    config_path = "etc_paasta_playground"

    # create an etc_paasta_playground directory if it doesn't exist
    # and copy fake_etc_paasta content into etc_paasta_playground directory
    if not os.path.isdir("etc_paasta_playground"):
        os.mkdir(os.path.join(os.getcwd(), "etc_paasta_playground"))
    subprocess.run(
        [
            ".tox/py37-linux/bin/python",
            "./k8s_itests/scripts/render_template.py",
            "-s",
            "./k8s_itests/deployments/paasta/fake_etc_paasta",
            "-d",
            "etc_paasta_playground",
        ],
        text=True,
        capture_output=True,
    )

    # Generate Cluster name
    clusters_names = {"clusters": [cluster], "cluster": cluster}
    clusters_path = os.path.join(os.getcwd(), config_path, "clusters.json")
    os.chmod(clusters_path, 0o777)
    with open(clusters_path, "w") as f:
        json.dump(clusters_names, f)

    # Add in volumes.json
    hacheck_sidecar_volumes = {"volumes": [], "hacheck_sidecar_volumes": []}
    volumes_path = os.path.join(os.getcwd(), config_path, "volumes.json")
    os.chmod(volumes_path, 0o777)
    with open(volumes_path, "w") as f:
        json.dump(hacheck_sidecar_volumes, f)

    # rename kubernetes-<%cluster%>.yaml with your cluster name
    # check if the cluster is not already renamed
    src_path = os.path.join(
        os.getcwd(),
        "k8s_itests/deployments/paasta/fake_soa_config/compute-infra-test-service",
        "kubernetes-<%cluster%>.yaml",
    )
    if os.path.exists(src_path):
        dst_path = os.path.join(
            os.getcwd(),
            "k8s_itests/deployments/paasta/fake_soa_config/compute-infra-test-service",
            f"kubernetes-{cluster}.yaml",
        )
        os.rename(src_path, dst_path)


if __name__ == "__main__":
    main()
