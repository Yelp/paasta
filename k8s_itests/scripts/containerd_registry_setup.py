import base64
import json
import os
import subprocess
import sys

import tomli
import tomli_w

REGISTRY = "docker-paasta.yelpcorp.com:443"


def get_credential_helper_auth() -> str:
    """Get registry credentials using docker-credential-yelp-okta (interactive)."""
    env = os.environ.copy()
    env["PATH"] = "/usr/bin:/usr/local/bin:/bin:" + env.get("PATH", "")
    result = subprocess.run(
        ["docker-credential-yelp-okta", "get"],
        input=REGISTRY,
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError("docker-credential-yelp-okta failed")
    creds = json.loads(result.stdout)
    username = creds.get("Username", "")
    secret = creds.get("Secret", "")
    if not secret:
        raise RuntimeError(
            "docker-credential-yelp-okta returned empty secret "
            "(interactive auth may be required — run from a terminal)"
        )
    return base64.b64encode(f"{username}:{secret}".encode()).decode()


def get_static_registry_auth() -> str:
    """Read static credentials from /nail/etc/docker-registry-ro (legacy)."""
    with open("/nail/etc/docker-registry-ro") as f:
        dockercfg = json.load(f)
    registry = list(dockercfg.keys())[0]
    return dockercfg[registry]["auth"]


def get_registry_auth() -> str:
    if os.path.exists("/nail/etc/docker-registry-ro"):
        return get_static_registry_auth()
    return get_credential_helper_auth()


if __name__ == "__main__":
    containerdcfg_file_path = sys.argv[1]
    with open(containerdcfg_file_path, "rb") as containerdcfg_file:
        containerdcfg = tomli.load(containerdcfg_file)

    auth_token = sys.argv[2] if len(sys.argv) > 2 else get_registry_auth()

    containerdcfg["plugins"]["io.containerd.grpc.v1.cri"]["registry"] = {
        "configs": {REGISTRY: {"auth": {"auth": auth_token}}},
        "mirrors": {REGISTRY: {"endpoint": [f"https://{REGISTRY}"]}},
    }

    with open(containerdcfg_file_path, "wb") as containerdcfg_file:
        tomli_w.dump(containerdcfg, containerdcfg_file)
