import argparse
import sys

from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.remote_git import LSRemoteException
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_rollback_tags_for_sha
from paasta_tools.utils import list_services


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "check-rollback-status",
        help="Check if a commit was previously rolled back for a deploy group",
        description=(
            "Checks whether a given commit has been marked as rolled back "
            "for a deploy group. Exit code 0 means the commit is safe to "
            "deploy, exit code 1 means it was previously rolled back."
        ),
    )
    parser.add_argument(
        "-s",
        "--service",
        help="Name of the service to check",
        required=True,
        type=lambda x: x.rstrip("/"),
    ).completer = lazy_choices_completer(list_services)
    parser.add_argument(
        "-l",
        "--deploy-group",
        help="Deploy group to check for rollback tags",
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    parser.add_argument(
        "-k",
        "--commit",
        help="Git SHA to check",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        help="A directory from which soa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    parser.set_defaults(command=paasta_check_rollback_status)


def paasta_check_rollback_status(args: argparse.Namespace) -> int:
    service = args.service
    deploy_group = args.deploy_group
    commit = args.commit
    soa_dir = args.soa_dir

    try:
        validate_service_name(service, soa_dir)
    except NoSuchService as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    try:
        refs = list_remote_refs(get_git_url(service=service, soa_dir=soa_dir))
    except LSRemoteException as e:
        print(f"ERROR: Could not fetch remote refs: {e}", file=sys.stderr)
        return 2

    rollback_tags = get_rollback_tags_for_sha(refs, deploy_group, commit)
    if rollback_tags:
        print(f"ROLLED BACK: Commit {commit} was rolled back for {deploy_group}")
        for _, tstamp in rollback_tags:
            print(f"  Rolled back at: {tstamp}")
        return 1

    print(f"OK: Commit {commit} has not been rolled back for {deploy_group}")
    return 0
