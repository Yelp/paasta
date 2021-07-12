import argparse
import logging

from paasta_tools.config_utils import AutoConfigUpdater
from paasta_tools.utils import DEFAULT_SOA_CONFIGS_GIT_URL
from paasta_tools.utils import format_git_url
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--git-remote",
        help="Master git repo for soaconfigs",
        default=None,
        dest="git_remote",
    )
    parser.add_argument(
        "--branch", help="Branch name to push to", required=True, dest="branch",
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
        "--service", help="Service to modify", required=True, dest="service",
    )
    parser.add_argument(
        "--deploy-prefix",
        help="Prefix to prepend to deploy groups",
        required=True,
        dest="deploy_group_prefix",
    )
    parser.add_argument(
        "--instance-count",
        help="If a deploy group is added, the default instance count to create it with",
        required=False,
        default=1,
        dest="instance_count",
    )
    parser.add_argument(
        "--kube-file",
        help="Kubernetes configuration file to inspect and potentially modify",
        required=True,
        dest="kube_file",
    )
    parser.add_argument(
        "--deploy-group",
        help="Deploy group to add if it does not exist",
        required=True,
        dest="deploy_group",
    )
    parser.add_argument(
        "--non-relevant-steps",
        nargs="+",
        help="pipeline steps that are unrelated to deploy groups",
        required=False,
        default=[],
        dest="non_relevant_steps",
    )
    return parser.parse_args()


def get_default_git_remote():
    system_paasta_config = load_system_paasta_config()
    repo_config = system_paasta_config.get_git_repo_config("yelpsoa-configs")
    default_git_remote = format_git_url(
        system_paasta_config.get_git_config()["git_user"],
        repo_config.get("git_server", DEFAULT_SOA_CONFIGS_GIT_URL),
        repo_config["repo_name"],
    )
    return default_git_remote


def main(args):
    updater = AutoConfigUpdater(
        config_source=args.source_id,
        git_remote=args.git_remote or get_default_git_remote(),
        branch=args.branch,
        working_dir=args.local_dir or "/nail/tmp",
        do_clone=args.local_dir is None,
    )
    with updater:
        deploy = updater.get_existing_configs(args.service, "deploy")
        kube_file = updater.get_existing_configs(args.service, args.kube_file)

        deploy_groups = {data["deploy_group"]: name for name, data in kube_file.items()}
        pipeline_steps = {step["step"] for step in deploy["pipeline"]}.difference(
            set(args.non_relevant_steps)
        )

        # Ensure that deploy steps and groups agree before proceeding
        if deploy_groups.keys() != pipeline_steps:
            log.error(
                f"deploy groups {deploy_groups.keys()} did not match deploy steps {pipeline_steps}. cannot proceed until these files are in agreement"
            )

        if args.deploy_group not in deploy_groups:
            kube_file[args.deploy_group] = {
                "deploy_group": f"{args.deploy_group_prefix}.{args.deploy_group}",
                "instances": args.instance_count,
            }
            deploy["pipeline"].append(
                {
                    "step": f"{args.deploy_group_prefix}.{args.deploy_group}",
                    "wait_for_deployment": True,
                    "disabled": True,
                }
            )

            updater.write_configs(args.service, "deploy", deploy)
            updater.write_configs(args.service, args.kube_file, kube_file)

            updater.commit_to_remote(validate=False)


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    main(args)
