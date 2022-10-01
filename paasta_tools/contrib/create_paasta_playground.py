import json
import os

from render_template import render_values


def main():

    config_path = "etc_paasta_playground"
    values_path = "./k8s_itests/deployments/paasta/values.yaml"

    # create an etc_paasta_playground directory if it doesn't exist
    # and copy fake_etc_paasta content into etc_paasta_playground directory
    if not os.path.isdir("etc_paasta_playground"):
        os.mkdir("etc_paasta_playground")

    render_values(
        src="./k8s_itests/deployments/paasta/fake_etc_paasta",
        dst=config_path,
        values=values_path,
    )

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
        src="./k8s_itests/deployments/paasta/fake_soa_config",
        dst="soa_config_playground",
        values=values_path,
    )


if __name__ == "__main__":
    main()
