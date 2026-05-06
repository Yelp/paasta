import base64
import json
import os
import subprocess

REGISTRY = "docker-paasta.yelpcorp.com:443"


def get_registry_auth() -> str:
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


if __name__ == "__main__":
    print(get_registry_auth())
