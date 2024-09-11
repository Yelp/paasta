#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import base64
import contextlib
import hashlib
import json
import logging
import os
import sys
import time
from collections import defaultdict
from functools import partial
from typing import Callable
from typing import Dict
from typing import Generator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple

from kubernetes.client.rest import ApiException
from typing_extensions import Literal

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes_tools import create_secret
from paasta_tools.kubernetes_tools import create_secret_signature
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import get_paasta_secret_name
from paasta_tools.kubernetes_tools import get_paasta_secret_signature_name
from paasta_tools.kubernetes_tools import get_secret_signature
from paasta_tools.kubernetes_tools import get_vault_key_secret_name
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.kubernetes_tools import update_secret
from paasta_tools.kubernetes_tools import update_secret_signature
from paasta_tools.metrics import metrics_lib
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.secret_tools import get_secret_name_from_ref
from paasta_tools.secret_tools import get_secret_provider
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DEFAULT_VAULT_TOKEN_FILE
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import INSTANCE_TYPE_TO_K8S_NAMESPACE
from paasta_tools.utils import INSTANCE_TYPES
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PAASTA_K8S_INSTANCE_TYPES
from paasta_tools.utils import SHARED_SECRETS_K8S_NAMESPACES

log = logging.getLogger(__name__)


K8S_INSTANCE_TYPE_CLASSES = (
    KubernetesDeploymentConfig,
    EksDeploymentConfig,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync paasta secrets into k8s")
    parser.add_argument(
        "service_list",
        nargs="+",
        help="The list of services to sync secrets for",
        metavar="SERVICE",
    )
    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        metavar="CLUSTER",
        default=None,
        help="Kubernetes cluster name",
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-n",
        "--namespace",
        dest="namespace",
        help="Overwrite destination namespace for secrets",
    )
    parser.add_argument(
        "-t",
        "--vault-token-file",
        dest="vault_token_file",
        default=DEFAULT_VAULT_TOKEN_FILE,
        help="Define a different vault token file location",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "--secret-type",
        choices=[
            "all",
            "paasta-secret",
            "boto-key",
            "crypto-key",
            "datastore-credentials",
        ],
        default="all",
        type=str,
        help="Define which type of secret to add/update. Default is 'all' (which does not include datastore-credentials)",
    )
    args = parser.parse_args()
    return args


@contextlib.contextmanager
def set_temporary_environment_variables(
    environ: Mapping[str, str]
) -> Generator[None, None, None]:
    """
    *Note the return value means "yields None, takes None, and when finished, returns None"*

    Modifies the os.environ variable then yields this temporary state. Resets it when finished.

    :param environ: Environment variables to set
    """
    old_environ = dict(os.environ)  # ensure we're storing a copy
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def main() -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    system_paasta_config = load_system_paasta_config()
    if args.cluster:
        cluster = args.cluster
    else:
        cluster = system_paasta_config.get_cluster()

    timer = metrics_lib.system_timer(
        dimensions=dict(
            cluster=cluster,
        )
    )

    timer.start()
    secret_provider_name = system_paasta_config.get_secret_provider_name()
    vault_cluster_config = system_paasta_config.get_vault_cluster_config()
    kube_client = KubeClient()
    services_to_k8s_namespaces_to_allowlist = (
        get_services_to_k8s_namespaces_to_allowlist(
            service_list=args.service_list,
            cluster=cluster,
            soa_dir=args.soa_dir,
            kube_client=kube_client,
        )
    )

    result = sync_all_secrets(
        kube_client=kube_client,
        cluster=cluster,
        services_to_k8s_namespaces_to_allowlist=services_to_k8s_namespaces_to_allowlist,
        secret_provider_name=secret_provider_name,
        vault_cluster_config=vault_cluster_config,
        soa_dir=args.soa_dir,
        vault_token_file=args.vault_token_file,
        overwrite_namespace=args.namespace,
        secret_type=args.secret_type,
    )
    exit_code = 0 if result else 1

    timer.stop(tmp_dimensions={"result": exit_code})
    logging.info(
        f"Stopping timer for {cluster} with result {exit_code}: {timer()}ms elapsed"
    )
    sys.exit(exit_code)


def get_services_to_k8s_namespaces_to_allowlist(
    service_list: List[str], cluster: str, soa_dir: str, kube_client: KubeClient
) -> Dict[
    str,  # service
    Dict[
        str,  # namespace
        Optional[Set[str]],  # allowlist of secret names, None means allow all.
    ],
]:
    """
    Generate a mapping of service -> namespace -> allowlist of secrets, e.g.

    {
        "yelp-main": {
            "paasta": {"secret1", "secret2"},
            "paastasvc-yelp-main": {"secret1", "secret3"},
            "paasta-flinks": None,
        },
        "_shared": {
            "paasta": {"sharedsecret1"},
            "paastasvc-yelp-main": {"sharedsecret1", "sharedsecret2"},
            "paasta-flinks": None,
        }
    }

    This mapping is used by sync_all_secrets / sync_secrets:
    sync_secrets will only sync secrets into a namespace if the allowlist is None or contains that secret's name.
    """
    services_to_k8s_namespaces_to_allowlist: Dict[
        str, Dict[str, Optional[Set[str]]]
    ] = defaultdict(dict)

    for service in service_list:
        if service == "_shared":
            # _shared is handled specially for each service.
            continue

        config_loader = PaastaServiceConfigLoader(service, soa_dir)
        for instance_type_class in K8S_INSTANCE_TYPE_CLASSES:
            for service_instance_config in config_loader.instance_configs(
                cluster=cluster, instance_type_class=instance_type_class
            ):
                secrets_used, shared_secrets_used = get_secrets_used_by_instance(
                    service_instance_config
                )
                allowlist = services_to_k8s_namespaces_to_allowlist[service].setdefault(
                    service_instance_config.get_namespace(),
                    set(),
                )
                if allowlist is not None:
                    allowlist.update(secrets_used)

                if "_shared" in service_list:
                    shared_allowlist = services_to_k8s_namespaces_to_allowlist[
                        "_shared"
                    ].setdefault(
                        service_instance_config.get_namespace(),
                        set(),
                    )
                    if shared_allowlist is not None:
                        shared_allowlist.update(shared_secrets_used)

        for instance_type in INSTANCE_TYPES:
            if instance_type in PAASTA_K8S_INSTANCE_TYPES:
                continue  # handled above.

            instances = get_service_instance_list(
                service=service,
                instance_type=instance_type,
                cluster=cluster,
                soa_dir=soa_dir,
            )
            if instances:
                # Currently, all instance types besides kubernetes use one big namespace, defined in
                # INSTANCE_TYPE_TO_K8S_NAMESPACE. Sync all shared secrets and all secrets belonging to any service
                # which uses that instance type.

                services_to_k8s_namespaces_to_allowlist[service][
                    INSTANCE_TYPE_TO_K8S_NAMESPACE[instance_type]
                ] = None
                if "_shared" in service_list:
                    services_to_k8s_namespaces_to_allowlist["_shared"][
                        INSTANCE_TYPE_TO_K8S_NAMESPACE[instance_type]
                    ] = None

    return dict(services_to_k8s_namespaces_to_allowlist)


def get_secrets_used_by_instance(
    service_instance_config: KubernetesDeploymentConfig,
) -> Tuple[Set[str], Set[str]]:
    (
        secret_env_vars,
        shared_secret_env_vars,
    ) = service_instance_config.get_env_vars_that_use_secrets()

    secrets_used = {get_secret_name_from_ref(v) for v in secret_env_vars.values()}
    shared_secrets_used = {
        get_secret_name_from_ref(v) for v in shared_secret_env_vars.values()
    }

    for secret_volume in service_instance_config.get_secret_volumes():
        # currently, only per-service secrets are supported for secret_volumes.
        secrets_used.add(secret_volume["secret_name"])

    return secrets_used, shared_secrets_used


def sync_all_secrets(
    kube_client: KubeClient,
    cluster: str,
    services_to_k8s_namespaces_to_allowlist: Dict[str, Dict[str, Set[str]]],
    secret_provider_name: str,
    vault_cluster_config: Dict[str, str],
    soa_dir: str,
    vault_token_file: str,
    secret_type: Literal[
        "all", "paasta-secret", "crypto-key", "boto-key", "datastore-credentials"
    ] = "all",
    overwrite_namespace: Optional[str] = None,
) -> bool:
    results = []

    for (
        service,
        namespaces_to_allowlist,
    ) in services_to_k8s_namespaces_to_allowlist.items():
        sync_service_secrets: Dict[str, List[Callable]] = defaultdict(list)

        if overwrite_namespace:
            namespaces_to_allowlist = {
                overwrite_namespace: None
                if overwrite_namespace in SHARED_SECRETS_K8S_NAMESPACES
                else namespaces_to_allowlist.get(overwrite_namespace, set()),
            }
        for namespace, secret_allowlist in namespaces_to_allowlist.items():
            ensure_namespace(kube_client, namespace)
            sync_service_secrets["paasta-secret"].append(
                partial(
                    sync_secrets,
                    kube_client=kube_client,
                    cluster=cluster,
                    service=service,
                    secret_provider_name=secret_provider_name,
                    vault_cluster_config=vault_cluster_config,
                    soa_dir=soa_dir,
                    namespace=namespace,
                    vault_token_file=vault_token_file,
                    secret_allowlist=secret_allowlist,
                )
            )
        sync_service_secrets["boto-key"].append(
            partial(
                sync_boto_secrets,
                kube_client=kube_client,
                cluster=cluster,
                service=service,
                soa_dir=soa_dir,
            )
        )
        sync_service_secrets["crypto-key"].append(
            partial(
                sync_crypto_secrets,
                kube_client=kube_client,
                cluster=cluster,
                service=service,
                secret_provider_name=secret_provider_name,
                vault_cluster_config=vault_cluster_config,
                soa_dir=soa_dir,
                vault_token_file=vault_token_file,
            )
        )

        sync_service_secrets["datastore-credentials"].append(
            partial(
                sync_datastore_credentials,
                kube_client=kube_client,
                cluster=cluster,
                service=service,
                secret_provider_name=secret_provider_name,
                vault_cluster_config=vault_cluster_config,
                soa_dir=soa_dir,
                vault_token_file=vault_token_file,
                overwrite_namespace=overwrite_namespace,
            )
        )

        if secret_type == "all":
            results.append(
                all(sync() for sync in sync_service_secrets["paasta-secret"])
            )
            results.append(all(sync() for sync in sync_service_secrets["boto-key"]))
            results.append(all(sync() for sync in sync_service_secrets["crypto-key"]))
            # note that since datastore-credentials are in a different vault, they're not synced as part of 'all'
        else:
            results.append(all(sync() for sync in sync_service_secrets[secret_type]))

    return all(results)


def sync_secrets(
    kube_client: KubeClient,
    cluster: str,
    service: str,
    secret_provider_name: str,
    vault_cluster_config: Dict[str, str],
    soa_dir: str,
    namespace: str,
    vault_token_file: str,
    secret_allowlist: Optional[Set[str]],
) -> bool:
    secret_dir = os.path.join(soa_dir, service, "secrets")
    secret_provider_kwargs = {
        "vault_cluster_config": vault_cluster_config,
        # TODO: make vault-tools support k8s auth method so we don't have to
        # mount a token in.
        "vault_auth_method": "token",
        "vault_token_file": vault_token_file,
    }
    secret_provider = get_secret_provider(
        secret_provider_name=secret_provider_name,
        soa_dir=soa_dir,
        service_name=service,
        cluster_names=[cluster],
        secret_provider_kwargs=secret_provider_kwargs,
    )
    if not os.path.isdir(secret_dir):
        log.debug(f"No secrets dir for {service}")
        return True

    with os.scandir(secret_dir) as secret_file_paths:
        for secret_file_path in secret_file_paths:
            if secret_file_path.path.endswith("json"):
                secret = secret_file_path.name.replace(".json", "")
                if secret_allowlist is not None:
                    if secret not in secret_allowlist:
                        log.debug(
                            f"Skipping {service}.{secret} in {namespace} because it's not in in secret_allowlist"
                        )
                        continue

                with open(secret_file_path, "r") as secret_file:
                    secret_signature = secret_provider.get_secret_signature_from_data(
                        json.load(secret_file)
                    )

                if secret_signature:
                    create_or_update_k8s_secret(
                        service=service,
                        signature_name=get_paasta_secret_signature_name(
                            namespace, service, sanitise_kubernetes_name(secret)
                        ),
                        secret_name=get_paasta_secret_name(
                            namespace, service, sanitise_kubernetes_name(secret)
                        ),
                        get_secret_data=(
                            lambda: {
                                secret: base64.b64encode(
                                    # If signatures does not match, it'll sys.exit(1)
                                    secret_provider.decrypt_secret_raw(secret)
                                ).decode("utf-8")
                            }
                        ),
                        secret_signature=secret_signature,
                        kube_client=kube_client,
                        namespace=namespace,
                    )

    return True


def sync_datastore_credentials(
    kube_client: KubeClient,
    cluster: str,
    service: str,
    secret_provider_name: str,
    vault_cluster_config: Dict[str, str],
    soa_dir: str,
    vault_token_file: str,
    overwrite_namespace: Optional[str] = None,
) -> bool:
    """
    Map all the passwords requested for this service-instance to a single Kubernetes Secret store.
    Volume mounts will then map the associated secrets to their associated mount paths.
    """
    config_loader = PaastaServiceConfigLoader(service=service, soa_dir=soa_dir)
    system_paasta_config = load_system_paasta_config()
    datastore_credentials_vault_overrides = (
        system_paasta_config.get_datastore_credentials_vault_overrides()
    )

    for instance_type_class in K8S_INSTANCE_TYPE_CLASSES:
        for instance_config in config_loader.instance_configs(
            cluster=cluster, instance_type_class=instance_type_class
        ):
            namespace = (
                overwrite_namespace
                if overwrite_namespace is not None
                else instance_config.get_namespace()
            )
            datastore_credentials = instance_config.get_datastore_credentials()
            with set_temporary_environment_variables(
                datastore_credentials_vault_overrides
            ):
                # expects VAULT_ADDR_OVERRIDE, VAULT_CA_OVERRIDE, and VAULT_TOKEN_OVERRIDE to be set
                # in order to use a custom vault shard. overriden temporarily in this context
                provider = get_secret_provider(
                    secret_provider_name=secret_provider_name,
                    soa_dir=soa_dir,
                    service_name=service,
                    cluster_names=[cluster],
                    # overridden by env variables but still needed here for spec validation
                    secret_provider_kwargs={
                        "vault_cluster_config": vault_cluster_config,
                        "vault_auth_method": "token",
                        "vault_token_file": vault_token_file,
                    },
                )

                secret_data = {}
                for datastore, credentials in datastore_credentials.items():
                    # mypy loses type hints on '.items' and throws false positives. unfortunately have to type: ignore
                    # https://github.com/python/mypy/issues/7178
                    for credential in credentials:  # type: ignore
                        vault_path = f"secrets/datastore/{datastore}/{credential}"
                        secrets = provider.get_data_from_vault_path(vault_path)
                        if not secrets:
                            # no secrets found at this path. skip syncing
                            log.debug(
                                f"Warning: no secrets found at requested path {vault_path}."
                            )
                            continue

                        # decrypt and save in secret_data
                        vault_key_path = get_vault_key_secret_name(vault_path)

                        # kubernetes expects data to be base64 encoded binary in utf-8 when put into secret maps
                        # may look like:
                        # {'master': {'passwd': '****', 'user': 'v-approle-mysql-serv-nVcYexH95A2'}, 'reporting': {'passwd': '****', 'user': 'v-approle-mysql-serv-GgCpRIh9Ut7'}, 'slave': {'passwd': '****', 'user': 'v-approle-mysql-serv-PzjPwqNMbqu'}
                        secret_data[vault_key_path] = base64.b64encode(
                            json.dumps(secrets).encode("utf-8")
                        ).decode("utf-8")

            create_or_update_k8s_secret(
                service=service,
                signature_name=instance_config.get_datastore_credentials_signature_name(),
                secret_name=instance_config.get_datastore_credentials_secret_name(),
                get_secret_data=(lambda: secret_data),
                secret_signature=_get_dict_signature(secret_data),
                kube_client=kube_client,
                namespace=namespace,
            )

    return True


def sync_crypto_secrets(
    kube_client: KubeClient,
    cluster: str,
    service: str,
    secret_provider_name: str,
    vault_cluster_config: Dict[str, str],
    soa_dir: str,
    vault_token_file: str,
) -> bool:
    """
    For each key-name in `crypto_key`,
    1. Fetch all versions of the key-name from Vault superregion mapped from cluster, e.g. `kubestage` maps to `devc` Vault server.
    2. Create K8s secret from JSON blob containing all key versions.
    3. Create signatures as K8s configmap based on JSON blob hash.

    So each replica of a service instance gets the same key, thereby reducing requests to Vault API as we only talk to vault during secret syncing
    """
    config_loader = PaastaServiceConfigLoader(service=service, soa_dir=soa_dir)
    for instance_type_class in K8S_INSTANCE_TYPE_CLASSES:
        for instance_config in config_loader.instance_configs(
            cluster=cluster, instance_type_class=instance_type_class
        ):
            crypto_keys = instance_config.get_crypto_keys_from_config()
            if not crypto_keys:
                continue
            secret_data = {}
            provider = get_secret_provider(
                secret_provider_name=secret_provider_name,
                soa_dir=soa_dir,
                service_name=service,
                cluster_names=[cluster],
                secret_provider_kwargs={
                    "vault_cluster_config": vault_cluster_config,
                    "vault_auth_method": "token",
                    "vault_token_file": vault_token_file,
                },
            )
            for key in crypto_keys:
                key_versions = provider.get_key_versions(key)
                if not key_versions:
                    log.error(
                        f"No key versions found for {key} on {instance_config.get_sanitised_deployment_name()}"
                    )
                    continue

                secret_data[get_vault_key_secret_name(key)] = base64.b64encode(
                    json.dumps(key_versions).encode("utf-8")
                ).decode("utf-8")

            if not secret_data:
                continue

            create_or_update_k8s_secret(
                service=service,
                signature_name=instance_config.get_crypto_secret_signature_name(),
                # the secret name here must match the secret name given in the secret volume config,
                # i.e. `kubernetes.client.V1SecretVolumeSource`'s `secret_name` must match below
                secret_name=instance_config.get_crypto_secret_name(),
                get_secret_data=(lambda: secret_data),
                secret_signature=_get_dict_signature(secret_data),
                kube_client=kube_client,
                namespace=instance_config.get_namespace(),
            )

    return True


def sync_boto_secrets(
    kube_client: KubeClient,
    cluster: str,
    service: str,
    soa_dir: str,
) -> bool:
    config_loader = PaastaServiceConfigLoader(service=service, soa_dir=soa_dir)
    for instance_type_class in K8S_INSTANCE_TYPE_CLASSES:
        for instance_config in config_loader.instance_configs(
            cluster=cluster, instance_type_class=instance_type_class
        ):
            boto_keys = instance_config.config_dict.get("boto_keys", [])
            if not boto_keys:
                continue
            boto_keys.sort()
            secret_data = {}
            for key in boto_keys:
                for filetype in ["sh", "yaml", "json", "cfg"]:
                    this_key = key + "." + filetype
                    sanitised_key = this_key.replace(".", "-").replace("_", "--")
                    try:
                        with open(f"/etc/boto_cfg_private/{this_key}") as f:
                            secret_data[sanitised_key] = base64.b64encode(
                                f.read().encode("utf-8")
                            ).decode("utf-8")
                    except IOError:
                        log.warning(
                            f"Boto key {this_key} required for {service} could not be found."
                        )
                        secret_data[sanitised_key] = base64.b64encode(
                            "This user no longer exists. Remove it from boto_keys.".encode(
                                "utf-8"
                            )
                        ).decode("utf-8")

            if not secret_data:
                continue

            create_or_update_k8s_secret(
                service=service,
                signature_name=instance_config.get_boto_secret_signature_name(),
                secret_name=instance_config.get_boto_secret_name(),
                get_secret_data=(lambda: secret_data),
                secret_signature=_get_dict_signature(secret_data),
                kube_client=kube_client,
                namespace=instance_config.get_namespace(),
            )
    return True


def _get_dict_signature(data: Dict[str, str]) -> str:
    return hashlib.sha1(
        "|".join(f"{key}:{value}" for key, value in data.items()).encode("utf-8")
    ).hexdigest()


def create_or_update_k8s_secret(
    service: str,
    secret_name: str,
    signature_name: str,
    get_secret_data: Callable[[], Dict[str, str]],
    secret_signature: str,
    kube_client: KubeClient,
    namespace: str,
) -> None:
    """
    :param get_secret_data: is a function to postpone fetching data in order to reduce service load, e.g. Vault API
    """
    # In order to prevent slamming the k8s API, add some artificial delay here
    delay = load_system_paasta_config().get_secret_sync_delay_seconds()
    if delay:
        time.sleep(delay)

    kubernetes_signature = get_secret_signature(
        kube_client=kube_client,
        signature_name=signature_name,
        namespace=namespace,
    )

    if not kubernetes_signature:
        log.info(f"{secret_name} for {service} in {namespace} not found, creating")
        try:
            create_secret(
                kube_client=kube_client,
                service_name=service,
                secret_name=secret_name,
                secret_data=get_secret_data(),
                namespace=namespace,
            )
        except ApiException as e:
            if e.status == 409:
                log.warning(
                    f"Secret {secret_name} for {service} already exists in {namespace} but no signature found. Updating secret and signature."
                )
                update_secret(
                    kube_client=kube_client,
                    secret_name=secret_name,
                    secret_data=get_secret_data(),
                    service_name=service,
                    namespace=namespace,
                )
            else:
                raise
        create_secret_signature(
            kube_client=kube_client,
            service_name=service,
            signature_name=signature_name,
            secret_signature=secret_signature,
            namespace=namespace,
        )
    elif secret_signature != kubernetes_signature:
        log.info(
            f"{secret_name} for {service} in {namespace} needs updating as signature changed"
        )
        update_secret(
            kube_client=kube_client,
            secret_name=secret_name,
            secret_data=get_secret_data(),
            service_name=service,
            namespace=namespace,
        )
        update_secret_signature(
            kube_client=kube_client,
            service_name=service,
            signature_name=signature_name,
            secret_signature=secret_signature,
            namespace=namespace,
        )
    else:
        log.info(f"{secret_name} for {service} in {namespace} up to date")


if __name__ == "__main__":
    main()
