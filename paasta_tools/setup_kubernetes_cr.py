#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
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
"""
Usage: ./setup_kubernetes_cr.py [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging
import os
import sys
from typing import Any
from typing import Mapping
from typing import NamedTuple
from typing import Sequence

import service_configuration_lib

from paasta_tools.kubernetes_tools import create_custom_resource
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeCustomResource
from paasta_tools.kubernetes_tools import KubeKind
from paasta_tools.kubernetes_tools import list_custom_resources
from paasta_tools.kubernetes_tools import update_custom_resource
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig

log = logging.getLogger(__name__)


class CustomResource(NamedTuple):
    file_prefix: str
    version: str
    kube_kind: KubeKind
    group: str


def load_custom_resources(system_paasta_config: SystemPaastaConfig) -> Sequence[CustomResource]:
    custom_resources = []
    for custom_resource_dict in system_paasta_config.get_kubernetes_custom_resources():
        kube_kind = KubeKind(**custom_resource_dict.pop('kube_kind'))  # type: ignore
        custom_resources.append(CustomResource(kube_kind=kube_kind, **custom_resource_dict))
    return custom_resources


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Creates custom_resources.')
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        dest="verbose", default=False,
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    kube_client = KubeClient()

    setup_kube_succeeded = setup_all_custom_resources(
        kube_client=kube_client,
        soa_dir=soa_dir,
        system_paasta_config=load_system_paasta_config(),
    )
    sys.exit(0 if setup_kube_succeeded else 1)


def setup_all_custom_resources(
    kube_client: KubeClient,
    soa_dir: str,
    system_paasta_config: SystemPaastaConfig,
) -> bool:
    cluster = system_paasta_config.get_cluster()
    cluster_crds = {
        crd.spec.names.kind
        for crd in
        kube_client.apiextensions.list_custom_resource_definition(
            label_selector="yelp.com/paasta_service",
        ).items
    }
    log.debug(f"CRDs found: {cluster_crds}")
    custom_resources = load_custom_resources(system_paasta_config)
    results = []
    for custom_resource in custom_resources:
        if custom_resource.kube_kind.singular not in cluster_crds:
            # TODO: kube_kind.singular seems to correspond to `crd.names.kind`
            # and not `crd.names.singular`
            log.warning(
                f"CRD {custom_resource.kube_kind.singular} "
                f"not found in {cluster}",
            )
            continue
        config_dicts = load_all_configs(
            cluster=cluster,
            file_prefix=custom_resource.file_prefix,
            soa_dir=soa_dir,
        )
        if not config_dicts:
            continue
        ensure_namespace(
            kube_client=kube_client,
            namespace=f'paasta-{custom_resource.kube_kind.plural}',
        )
        results.append(
            setup_custom_resources(
                kube_client=kube_client,
                kind=custom_resource.kube_kind,
                config_dicts=config_dicts,
                version=custom_resource.version,
                group=custom_resource.group,
                cluster=cluster,
            ),
        )
    return all(results) if results else True


def load_all_configs(cluster: str, file_prefix: str, soa_dir: str) -> Mapping[str, Mapping[str, Any]]:
    config_dicts = {}
    for service in os.listdir(soa_dir):
        config_dicts[service] = service_configuration_lib.read_extra_service_information(
            service,
            f"{file_prefix}-{cluster}",
            soa_dir=soa_dir,
        )
    return config_dicts


def setup_custom_resources(
    kube_client: KubeClient,
    kind: KubeKind,
    version: str,
    config_dicts: Mapping[str, Mapping[str, Any]],
    group: str,
    cluster: str,
) -> bool:
    succeded = True
    if config_dicts:
        crs = list_custom_resources(
            kube_client=kube_client,
            kind=kind,
            version=version,
            group=group,
        )
    for service, config in config_dicts.items():
        if not reconcile_kubernetes_resource(
            kube_client=kube_client,
            service=service,
            instance_configs=config,
            kind=kind,
            custom_resources=crs,
            version=version,
            group=group,
            cluster=cluster,
        ):
            succeded = False
    return succeded


def format_custom_resource(
    instance_config: Mapping[str, Any],
    service: str,
    instance: str,
    cluster: str,
    kind: str,
    version: str,
    group: str,
) -> Mapping[str, Any]:
    sanitised_service = service.replace('_', '--')
    sanitised_instance = instance.replace('_', '--')
    resource: Mapping[str, Any] = {
        'apiVersion': f'{group}/{version}',
        'kind': kind,
        'metadata': {
            'name': f'{sanitised_service}-{sanitised_instance}',
            'labels': {
                'yelp.com/paasta_service': service,
                'yelp.com/paasta_instance': instance,
                'yelp.com/paasta_cluster': cluster,
            },
            'annotations': {
                'yelp.com/desired_state': 'running',
            },
        },
        'spec': instance_config,
    }
    config_hash = get_config_hash(
        instance_config,
    )
    resource['metadata']['labels']['yelp.com/paasta_config_sha'] = config_hash
    return resource


def reconcile_kubernetes_resource(
    kube_client: KubeClient,
    service: str,
    instance_configs: Mapping[str, Any],
    custom_resources: Sequence[KubeCustomResource],
    kind: KubeKind,
    version: str,
    group: str,
    cluster: str,
) -> bool:

    results = []
    for instance, config in instance_configs.items():
        formatted_resource = format_custom_resource(
            instance_config=config,
            service=service,
            instance=instance,
            cluster=cluster,
            kind=kind.singular,
            version=version,
            group=group,
        )
        desired_resource = KubeCustomResource(
            service=service,
            instance=instance,
            config_sha=formatted_resource['metadata']['labels']['yelp.com/paasta_config_sha'],
            kind=kind.singular,
        )

        try:
            if not (service, instance, kind.singular) in [(c.service, c.instance, c.kind) for c in custom_resources]:
                log.info(f"{desired_resource} does not exist so creating")
                create_custom_resource(
                    kube_client=kube_client,
                    version=version,
                    kind=kind,
                    formatted_resource=formatted_resource,
                    group=group,
                )
            elif desired_resource not in custom_resources:
                sanitised_service = service.replace('_', '--')
                sanitised_instance = instance.replace('_', '--')
                log.info(f"{desired_resource} exists but config_sha doesn't match")
                update_custom_resource(
                    kube_client=kube_client,
                    name=f'{sanitised_service}-{sanitised_instance}',
                    version=version,
                    kind=kind,
                    formatted_resource=formatted_resource,
                    group=group,
                )
            else:
                log.info(f"{desired_resource} is up to date, no action taken")
        except Exception as e:
            log.error(str(e))
            results.append(False)
        results.append(True)
    return all(results) if results else True


if __name__ == "__main__":
    main()
