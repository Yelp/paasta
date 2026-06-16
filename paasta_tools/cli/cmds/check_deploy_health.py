import argparse
import sys

from paasta_tools.cli.cmds.mark_for_deployment import NoSuchCluster
from paasta_tools.cli.cmds.mark_for_deployment import check_if_instance_is_done
from paasta_tools.cli.cmds.mark_for_deployment import (
    get_instance_configs_for_service_in_deploy_group_all_clusters,
)
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.deployment_utils import get_currently_deployed_version
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_services


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "check-deploy-health",
        help="Check if all instances in a deploy group are healthy at a version",
        description=(
            "Checks whether all instances in a deploy group are running and "
            "healthy at the currently deployed version. Exit code 0 means healthy, "
            "1 means unhealthy."
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
        help="Deploy group to check health for (e.g. cluster1.canary, cluster2.main)",
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    parser.add_argument(
        "-d",
        "--soa-dir",
        help="A directory from which soa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    parser.set_defaults(command=paasta_check_deploy_health)


def paasta_check_deploy_health(args: argparse.Namespace) -> int:
    service = args.service
    deploy_group = args.deploy_group
    soa_dir = args.soa_dir

    try:
        validate_service_name(service, soa_dir)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    version = get_currently_deployed_version(
        service=service, deploy_group=deploy_group, soa_dir=soa_dir
    )
    if not version:
        print(
            f"ERROR: No version currently deployed for {service} in {deploy_group}",
            file=sys.stderr,
        )
        return 2

    try:
        instance_configs_per_cluster = (
            get_instance_configs_for_service_in_deploy_group_all_clusters(
                service, deploy_group, soa_dir
            )
        )
    except NoSuchCluster:
        print(
            f"ERROR: A cluster for {service} is not in paasta-api endpoints config",
            file=sys.stderr,
        )
        return 2

    instance_configs = [
        (cluster, instance_config)
        for cluster, configs in instance_configs_per_cluster.items()
        for instance_config in configs
    ]

    if not instance_configs:
        print(
            f"ERROR: No instance configs found for {service} in deploy group {deploy_group}",
            file=sys.stderr,
        )
        return 2

    all_healthy = all(
        check_if_instance_is_done(
            service=service,
            instance=instance_config.get_instance(),
            cluster=cluster,
            version=version,
            instance_config=instance_config,
        )
        for cluster, instance_config in instance_configs
    )

    if all_healthy:
        print(
            f"HEALTHY: All instances of {service} in {deploy_group} are healthy "
            f"at {version}"
        )
        return 0
    else:
        print(
            f"UNHEALTHY: Not all instances of {service} in {deploy_group} are "
            f"healthy at {version}",
            file=sys.stderr,
        )
        return 1
