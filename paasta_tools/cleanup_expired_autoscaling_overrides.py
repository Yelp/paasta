import argparse
import json
import logging
import sys
import time
from datetime import datetime
from datetime import timezone

from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import AUTOSCALING_OVERRIDES_CONFIGMAP_NAME
from paasta_tools.kubernetes_tools import AUTOSCALING_OVERRIDES_CONFIGMAP_NAMESPACE
from paasta_tools.kubernetes_tools import get_namespaced_configmap
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import replace_namespaced_configmap

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description=f"Clean up expired entries from the {AUTOSCALING_OVERRIDES_CONFIGMAP_NAME} configmap"
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Print entries to be removed instead of removing them",
    )
    parser.add_argument(
        "-v", "--verbose", dest="verbose", action="store_true", default=False
    )
    args = parser.parse_args()
    return args


def setup_logging(verbose):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level)
    # silence kubernetes client metadata logs since debug ones are pretty noisy
    logging.getLogger("kubernetes.client.rest").setLevel(logging.ERROR)


def main():
    args = parse_args()
    setup_logging(args.verbose)

    kube_client = KubeClient()

    try:
        configmap = get_namespaced_configmap(
            name=AUTOSCALING_OVERRIDES_CONFIGMAP_NAME,
            namespace=AUTOSCALING_OVERRIDES_CONFIGMAP_NAMESPACE,
            kube_client=kube_client,
        )
    except ApiException as e:
        if e.status == 404:
            log.debug(
                f"ConfigMap {AUTOSCALING_OVERRIDES_CONFIGMAP_NAME} not found, nothing to clean up"
            )
            sys.exit(0)
        else:
            log.error(f"Error retrieving ConfigMap: {e}")
            sys.exit(1)

    if configmap is None:
        log.debug(
            f"ConfigMap {AUTOSCALING_OVERRIDES_CONFIGMAP_NAME} not found, nothing to clean up"
        )
        sys.exit(0)

    if not configmap.data:
        log.debug("ConfigMap has no data, nothing to clean up")
        sys.exit(0)

    current_timestamp = (
        time.time()
    )  # we'll fix the comparison time to the start of the script execution
    expired_entries = []  # used just for logging
    unexpired_data = {}  # used to replace the existing configmap data

    log.debug(f"Current timestamp: {current_timestamp}")
    for service_instance, override_json in configmap.data.items():
        log.debug(f"Processing entry for {service_instance}")

        try:
            override_data = json.loads(override_json)
        except json.JSONDecodeError:
            log.exception(
                f"Failed to parse JSON for {service_instance}, removing entry"
            )
            expired_entries.append(service_instance)
            continue

        if "expire_after" not in override_data:
            log.warning(
                f"Entry for {service_instance} missing expire_after field, removing entry"
            )
            expired_entries.append(service_instance)
            continue

        try:
            expire_after = float(override_data["expire_after"])
        except (ValueError, TypeError):
            log.exception(
                f"Failed to parse expire_after for {service_instance}, removing entry"
            )
            expired_entries.append(service_instance)
            continue

        if current_timestamp > expire_after:
            expire_datetime = datetime.fromtimestamp(expire_after, tz=timezone.utc)
            log.debug(
                f"Entry for {service_instance} expired at {expire_after} ({expire_datetime})"
            )
            expired_entries.append(service_instance)
        else:
            expire_datetime = datetime.fromtimestamp(expire_after, tz=timezone.utc)
            log.debug(
                f"Entry for {service_instance} expires at {expire_after} ({expire_datetime}), keeping"
            )
            unexpired_data[service_instance] = override_json

    if not expired_entries:
        log.info("No expired entries found")
        sys.exit(0)

    if args.dry_run:
        log.info(
            "Would have removed the following expired entries:\n "
            + "\n ".join(expired_entries)
        )
        sys.exit(0)

    try:
        configmap.data = (
            unexpired_data  # naughtily re-use the existing configmap object
        )
        replace_namespaced_configmap(  # NOTE: this is not a patch since you cannot currently remove entries in a patch
            name=AUTOSCALING_OVERRIDES_CONFIGMAP_NAME,
            namespace=AUTOSCALING_OVERRIDES_CONFIGMAP_NAMESPACE,
            body=configmap,
            kube_client=kube_client,
        )
        log.info(
            f"Successfully removed {len(expired_entries)} expired entries:\n "
            + "\n ".join(expired_entries)
        )
    except ApiException as e:
        log.error(f"Failed to update ConfigMap: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
