import json
import os
import socket
import subprocess

from render_template import render_values

from paasta_tools.cli.utils import pick_random_port
from paasta_tools.utils import get_docker_client


def main():
    config_path = "etc_paasta_playground"
    values_path = "./k8s_itests/deployments/paasta/values.yaml"
    user = os.getenv("USER")

    # start a local copy of zookeeper on a random port
    zookeeper_port = pick_random_port(f"{user}-paasta-zookeeper")
    ensure_running_local_zookeeper(user, zookeeper_port)
    os.environ["ZOOKEEPER_PORT"] = str(zookeeper_port)
    os.environ["HOST_IP"] = socket.gethostbyname(socket.gethostname())

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
        overwrite=False,
    )


def ensure_running_local_zookeeper(user: str, zookeeper_port: int) -> None:
    client = get_docker_client()
    containers = client.containers()
    zookeeper_container = next(
        (
            container
            for container in containers
            if container["Names"] == [f"/{user}-paasta-zookeeper"]
        ),
        None,
    )
    if zookeeper_container is None:
        run_zookeeper_container(user, zookeeper_port)
    elif zookeeper_container.get("Status") != "running":
        client.remove_container(zookeeper_container, force=True)
        run_zookeeper_container(user, zookeeper_port)


def run_zookeeper_container(user: str, zookeeper_port: int) -> None:
    if "yelpcorp.com" in socket.getfqdn():
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "-p",
                f"{zookeeper_port}:2181",
                "-e",
                '"ALLOW_ANONYMOUS_LOGIN=yes"',
                "--name",
                f"{user}-paasta-zookeeper",
                "docker-dev.yelpcorp.com/zookeeper-testing",
            ]
        )
    else:
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "-p",
                f"{zookeeper_port}:2181",
                "-e",
                '"ALLOW_ANONYMOUS_LOGIN=yes"',
                "--name",
                f"{user}-paasta-zookeeper",
                "zookeeper:3.5",
            ]
        )


if __name__ == "__main__":
    main()
