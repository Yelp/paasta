import os
import subprocess
import sys
from subprocess import PIPE


def cmd(args, capture_output=True):
    try:
        return subprocess.run(
            args.split(" "),
            stdout=PIPE if capture_output else None,
            stderr=PIPE if capture_output else None,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        raise


def start_paasta_api():
    print("Starting Paasta API Server")
    p = subprocess.Popen(
        "python -m paasta_tools.api.api -D -c {} {}".format(
            os.environ["KIND_CLUSTER"], os.environ["PAASTA_API_PORT"]
        ).split(" ")
    )
    return p


def paasta_apply():
    print("Applying SOA configurations")
    service_instances = cmd("python -m paasta_tools.list_kubernetes_service_instances")
    cmd(
        "python -m paasta_tools.setup_kubernetes_job {} -v".format(
            service_instances.stdout.strip()
        ),
        True,
    )


def init_all():
    paasta_apply()
