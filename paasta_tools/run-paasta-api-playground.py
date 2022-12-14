import json
import os

from paasta_tools.cli.utils import pick_random_port


def main():
    print("-------------------------------------------------------")
    print(
        "Please run export PAASTA_SYSTEM_CONFIG_DIR=etc_paasta_playground to continue"
    )
    print(
        "Please set environment variable PAASTA_TEST_CLUSTER to the cluster you want to use."
    )
    print("-------------------------------------------------------")
    user = os.environ["USER"]
    cluster = os.environ.get("PAASTA_TEST_CLUSTER", f"kind-{user}-k8s-test")
    config_path = "etc_paasta_playground"

    # find unused ports
    port = pick_random_port("paasta-dev-api")

    # Generate api endpoints
    api_endpoints = {"api_endpoints": {cluster: f"http://localhost:{port}"}}
    api_endpoints_path = os.path.join(config_path, "api_endpoints.json")
    with open(api_endpoints_path, "w") as f:
        json.dump(api_endpoints, f)

    # export config path
    os.environ["PAASTA_SYSTEM_CONFIG_DIR"] = config_path

    api_single_process = os.environ.get("PAASTA_API_SINGLE_PROCESS")
    if api_single_process is not None and api_single_process.lower() == "true":
        from paasta_tools.api.api import redirect_argv

        with redirect_argv(["-D", "-c", cluster, str(port)]):
            from paasta_tools.api import api

            api.main("dev-mode")
    else:
        os.execl(
            ".tox/py37-linux/bin/python",
            ".tox/py37-linux/bin/python",
            "-m",
            "paasta_tools.api.api",
            *["-D", "-c", cluster, str(port)],
        )


if __name__ == "__main__":
    main()
