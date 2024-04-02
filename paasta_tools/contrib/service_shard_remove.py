import argparse
import logging

from paasta_tools.config_utils import AutoConfigUpdater
from paasta_tools.utils import DEFAULT_SOA_CONFIGS_GIT_URL
from paasta_tools.utils import format_git_url
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)


def get_default_git_remote() -> str:
    system_paasta_config = load_system_paasta_config()
    repo_config = system_paasta_config.get_git_repo_config("yelpsoa-configs")
    default_git_remote = format_git_url(
        system_paasta_config.get_git_config()["git_user"],
        repo_config.get("git_server", DEFAULT_SOA_CONFIGS_GIT_URL),
        repo_config["repo_name"],
    )
    return default_git_remote


def parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--git-remote",
        help="Master git repo for soaconfigs",
        default=get_default_git_remote(),
        dest="git_remote",
    )
    parser.add_argument(
        "--branch",
        help="Branch name to push to",
        required=True,
        dest="branch",
    )
    parser.add_argument(
        "--local-dir",
        help="Act on configs in the local directory rather than cloning the git_remote",
        required=False,
        default=None,
        dest="local_dir",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Logging verbosity",
        action="store_true",
        dest="verbose",
    )
    parser.add_argument(
        "--source-id",
        help="String to attribute the changes in the commit message.",
        required=False,
        default=None,
        dest="source_id",
    )
    parser.add_argument(
        "--service",
        help="Service to modify",
        required=True,
        dest="service",
    )
    parser.add_argument(
        "--shard-name",
        help="Shard name to remove",
        required=True,
        dest="shard_name",
    )
    parser.add_argument(
        "--superregion",
        nargs="+",
        help="Superregion to remove shard from",
        required=True,
    )
    return parser.parse_args()


SUPPERREGION_DEPLOY_MAPPINGS = {
    "norcal-devc": "dev",
    "norcal-stagef": "stage",
    "norcal-stageg": "stage",
    "nova-prod": "prod",
    "pnw-prod": "prod",
}


def main(args):
    updater = AutoConfigUpdater(
        config_source=args.source_id,
        git_remote=args.git_remote,
        branch=args.branch,
        working_dir=args.local_dir or "/nail/tmp",
        do_clone=args.local_dir is None,
    )
    with updater:
        deploy_file = updater.get_existing_configs(args.service, "deploy")
        smartstack_file = updater.get_existing_configs(args.service, "smartstack")
        delete_all = set(args.superregion) == set(SUPPERREGION_DEPLOY_MAPPINGS.keys())

        for superregion in args.superregion:
            # Determine / load configuration (PAASTA-18216)
            eks_config = updater.get_existing_configs(
                args.service, f"eks-{superregion}"
            )
            kube_config = updater.get_existing_configs(
                args.service, f"kubernetes-{superregion}"
            )

            if eks_config:
                config_file = eks_config
                config_file_suffix = "eks"
            elif kube_config:
                config_file = kube_config
                config_file_suffix = "kubernetes"
            else:
                log.error(f"No EKS or Kubernetes config found for {args.service}")
                continue

            # Remove shard from deploy pipeline config
            targeted_step = (
                f"{SUPPERREGION_DEPLOY_MAPPINGS[superregion]}.{args.shard_name}"
            )
            deploy_file["pipeline"] = [
                i for i in deploy_file["pipeline"] if i["step"] != targeted_step
            ]
            log.info(f"{targeted_step} removed from deploy config")
            updater.write_configs(args.service, "deploy", deploy_file)

            # Remove shard from corresponding config
            del config_file[args.shard_name]
            updater.write_configs(
                args.service, f"{config_file_suffix}-{superregion}", config_file
            )
            log.info(
                f"{args.shard_name} removed from {config_file_suffix}-{superregion}"
            )

        # If we are removing the shard from all regions, remove it from smartstack too.
        if delete_all:
            del smartstack_file[args.shard_name]
            updater.write_configs(args.service, "smartstack", smartstack_file)
            log.info(f"{args.shard_name} removed from smartstack")
        else:
            log.info(f"not removing {args.shard_name} from smartstack")

        updater.commit_to_remote()


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    main(args)
