import argparse
import sys
import time
from typing import Optional

from paasta_tools.cli.cmds.push_to_registry import is_docker_image_already_in_registry
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import PaastaColors

DEFAULT_POLL_PERIOD = 30


def add_subparser(subparsers):
    parser = subparsers.add_parser(
        "verify-image-exists",
        help="Check if a docker image exists in the registry for a service and SHA",
        description=(
            "Checks the Docker registry for the existence of an image for a given "
            "service and git SHA. Optionally polls until the image appears."
        ),
    )
    parser.add_argument(
        "-s",
        "--service",
        help="Name of the service",
        required=True,
        type=lambda x: x.rstrip("/"),
    ).completer = lazy_choices_completer(list_services)
    parser.add_argument(
        "-c",
        "--commit",
        help="Git SHA to check for",
        required=True,
    )
    parser.add_argument(
        "--image-version",
        help="Optional image version suffix",
        default=None,
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        help="Path to yelpsoa-configs directory",
        default=DEFAULT_SOA_DIR,
    )
    parser.add_argument(
        "--wait",
        help="Poll until the image is found",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--poll-period",
        help="Seconds between poll attempts (default: %(default)s)",
        type=int,
        default=DEFAULT_POLL_PERIOD,
    )
    parser.add_argument(
        "--timeout",
        help="Max seconds to wait; 0 means wait forever (default: %(default)s)",
        type=int,
        default=0,
    )
    parser.set_defaults(command=paasta_verify_image_exists)


def verify_image_exists(
    service: str,
    commit: str,
    soa_dir: str = DEFAULT_SOA_DIR,
    image_version: Optional[str] = None,
    wait: bool = False,
    poll_period: int = DEFAULT_POLL_PERIOD,
    timeout: int = 0,
) -> int:
    if is_docker_image_already_in_registry(
        service=service, soa_dir=soa_dir, sha=commit, image_version=image_version
    ):
        print(
            PaastaColors.green(
                f"Image for {service} at {commit} exists in the registry."
            )
        )
        return 0

    if not wait:
        print(
            PaastaColors.red(
                f"Image for {service} at {commit} NOT FOUND in the registry."
            ),
            file=sys.stderr,
        )
        return 1

    start_time = time.time()
    print(f"Waiting for image for {service} at {commit} to appear in the registry...")
    while True:
        time.sleep(poll_period)
        print(".", end="", flush=True)

        if timeout > 0 and (time.time() - start_time) >= timeout:
            print()
            print(
                PaastaColors.red(
                    f"Timed out after {timeout}s waiting for image for {service} at {commit}."
                ),
                file=sys.stderr,
            )
            return 1

        if is_docker_image_already_in_registry(
            service=service, soa_dir=soa_dir, sha=commit, image_version=image_version
        ):
            print()
            print(
                PaastaColors.green(
                    f"Image for {service} at {commit} exists in the registry."
                )
            )
            return 0


def paasta_verify_image_exists(args: argparse.Namespace) -> int:
    return verify_image_exists(
        service=args.service,
        commit=args.commit,
        soa_dir=args.soa_dir,
        image_version=args.image_version,
        wait=args.wait,
        poll_period=args.poll_period,
        timeout=args.timeout,
    )
