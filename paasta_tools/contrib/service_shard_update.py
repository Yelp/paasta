import argparse
import logging
import sys

from paasta_tools.cli.utils import trigger_deploys
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
        "--instance-count",
        help="If a deploy group is added, the default instance count to create it with",
        required=False,
        default=1,
        dest="instance_count",
    )
    parser.add_argument(
        "--shard-name",
        help="Shard name to add if it does not exist",
        required=True,
        dest="shard_name",
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


DEPLOY_MAPPINGS = {
    "dev": ["kubernetes-norcal-devc"],
    "stage": ["kubernetes-norcal-stagef", "kubernetes-norcal-stageg"],
    "prod": ["kubernetes-nova-prod", "kubernetes-pnw-prod"],
}


def main(args):
    changes_made = False
    updater = AutoConfigUpdater(
        config_source=args.source_id,
        git_remote=args.git_remote or get_default_git_remote(),
        branch=args.branch,
        working_dir=args.local_dir or "/nail/tmp",
        do_clone=args.local_dir is None,
    )
    with updater:
        deploy_file = updater.get_existing_configs(args.service, "deploy")
        smartstack_file = updater.get_existing_configs(args.service, "smartstack")
        shard_deploy_groups = {
            f"{prefix}.{args.shard_name}" for prefix in DEPLOY_MAPPINGS.keys()
        }
        pipeline_steps = {step["step"] for step in deploy_file["pipeline"]}

        if not shard_deploy_groups.issubset(pipeline_steps):
            changes_made = True
            steps_to_add = shard_deploy_groups - pipeline_steps

            # If the pipeline does not contain deploy groups for the service shard
            # Add the missing steps and write to deploy config
            for step in steps_to_add:
                deploy_file["pipeline"].append(
                    {"step": step, "wait_for_deployment": True, "disabled": True,}
                )
                log.info(f"{step} added to deploy config")
            updater.write_configs(args.service, "deploy", deploy_file)

            for deploy_prefix, config_paths in DEPLOY_MAPPINGS.items():
                for config_path in config_paths:
                    kube_file = updater.get_existing_configs(args.service, config_path)
                    # If the service config does not contain definitions for the shard in each ecosystem
                    # Add the missing definition and write to the corresponding config
                    if args.shard_name not in kube_file.keys():
                        kube_file[args.shard_name] = {
                            "deploy_group": f"{deploy_prefix}.{args.shard_name}",
                            "instances": args.instance_count,
                            "env": {
                                "PAASTA_SECRET_BUGSNAG_API_KEY": "SECRET(bugsnag_api_key)",
                            },
                        }
                        updater.write_configs(args.service, config_path, kube_file)
                        log.info(
                            f"{deploy_prefix}.{args.shard_name} added to {config_path}"
                        )
        else:
            log.info(f"{args.shard_name} is in deploy config already.")

        # If the service shard is not defined in smartstack
        # Add the definition with a suggested proxy port
        if args.shard_name not in smartstack_file.keys():
            changes_made = True
            smartstack_file[args.shard_name] = {
                "proxy_port": None,
                "extra_advertise": {"ecosystem:devc": ["ecosystem:devc"]},
            }
            updater.write_configs(args.service, "smartstack", smartstack_file)
        else:
            log.info(f"{args.shard_name} is in smartstack config already, skipping.")

        # Only commit to remote if changes were made
        if changes_made:
            updater.commit_to_remote()
            trigger_deploys(args.service)
        else:
            # exit with code to indicate nothing was changed
            sys.exit(129)


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    main(args)
