import argparse
import functools
import os
import tempfile
import traceback
from typing import List
from typing import Set
from typing import Tuple
import concurrent.futures

import mock
import json

from kubernetes.client.rest import ApiException

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes_tools import create_or_find_service_account_name
from paasta_tools.kubernetes_tools import get_kubernetes_app_name
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import make_namespace_object
from paasta_tools.kubernetes_tools import make_paasta_api_rolebinding_object
from paasta_tools.kubernetes_tools import make_paasta_namespace_limit_object
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.setup_prometheus_adapter_config import (
    create_prometheus_adapter_config,
)
from paasta_tools.setup_prometheus_adapter_config import (
    format_prometheus_adapter_configmap,
)
from paasta_tools.utils import list_clusters
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import DEFAULT_SOA_DIR

INSTANCE_TYPE_CLASSES = [
    KubernetesDeploymentConfig,
    EksDeploymentConfig,
]


def add_subparser(
    subparsers,
) -> None:
    parser = subparsers.add_parser(
        "yaml",
        help="Generate k8s yaml for services.",
    )

    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=".",
        help="Path to yelpsoa-configs checkout, defaults to current directory",
    )

    parser.add_argument(
        "--out-dir",
        dest="out_dir",
        help=(
            "Where output yamls should be written."
            " Defaults to a temporary directory."
            " One subdirectory will be created per cluster."
            " Inside each cluster directory, one directory per service will be created."
        ),
        default=None,
    )

    parser.add_argument(
        "services",
        help="Which services to generate k8s objects for.",
        nargs="+",
    )

    parser.set_defaults(command=paasta_yaml)


def write_yaml(out_dir, cluster, k8s_obj):
    k8s_dict = k8s_obj.to_dict()
    directory = os.path.join(
        out_dir,
        cluster,
        k8s_obj.metadata.namespace or "_unnamespaced",
        k8s_obj.kind,
    )
    filename = os.path.join(directory, f"{k8s_obj.metadata.name}.json")
    os.makedirs(directory, exist_ok=True)

    with open(os.path.join(directory, filename), "w+") as f:
        json.dump(k8s_dict, f)


_already_created_prometheus_adapter_clusters: Set[str] = set()


def maybe_write_prometheus_adapter_config(
    out_dir: str, cluster: str, soa_dir: str, services: List[str]
):
    if cluster in _already_created_prometheus_adapter_clusters:
        return
    _already_created_prometheus_adapter_clusters.add(cluster)
    config = create_prometheus_adapter_config(
        paasta_cluster=cluster,
        soa_dir=soa_dir,
        services=set(services),
    )
    configMap = format_prometheus_adapter_configmap(config)
    write_yaml(out_dir, cluster, configMap)


@functools.lru_cache(maxsize=None)
def maybe_write_namespace_objects(out_dir: str, cluster: str, namespace: str):
    write_yaml(out_dir, cluster, make_namespace_object(namespace))
    write_yaml(out_dir, cluster, make_paasta_api_rolebinding_object(namespace))
    write_yaml(out_dir, cluster, make_paasta_namespace_limit_object(namespace))


class FakeList:
    @property
    def items(self):
        return []


class FakeKubeClient:
    def __init__(self, cluster: str):
        self.cluster = cluster
        self.created_objects = []

    @property
    def core(self):
        return self

    def __getattr__(self, name):
        if name.startswith("list_"):
            return self._list

        if name.startswith("create_"):
            return self._create

        raise AttributeError(self, name)

    def _list(self, *args, **kwargs):
        return FakeList()

    def read_namespaced_config_map(self, *args, **kwargs):
        raise ApiException(status=404)

    def _create(self, body, **kwargs):
        self.created_objects.append(body)


def load_v2_deployments_json_fallback(*args, **kwargs):
    try:
        return load_v2_deployments_json(*args, **kwargs)
    except NoDeploymentsAvailable:
        kwargs["soa_dir"] = DEFAULT_SOA_DIR
        return load_v2_deployments_json(*args, **kwargs)


def generate_yamls_for_cluster(service: str, out_dir: str, args: argparse.Namespace, cluster: str):
    maybe_write_prometheus_adapter_config(
        out_dir, cluster, args.soa_dir, args.services
    )
    # print(cluster)

    pscl = PaastaServiceConfigLoader(
        service=service, soa_dir=args.soa_dir, load_deployments=True
    )

    fake_kube_client = FakeKubeClient(cluster)
    with mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        return_value=fake_kube_client,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_autoscaled_instances",
        return_value=None,
    ), mock.patch(
        "paasta_tools.paasta_service_config_loader.load_v2_deployments_json",
        side_effect=load_v2_deployments_json_fallback,
    ):
        for instance_type_class in INSTANCE_TYPE_CLASSES:
            # , mock.patch(
            #     "paasta_tools.kubernetes_tools.create_or_find_service_account_name",
            #     side_effect=lambda *args, **kwargs: create_or_find_service_account_name(*args, **kwargs, dry_run=True)
            # )
            # breakpoint()

            for job_config in pscl.instance_configs(
                cluster=cluster, instance_type_class=instance_type_class
            ):
                maybe_write_namespace_objects(
                    out_dir, cluster, job_config.get_namespace()
                )
                try:
                    out = job_config.format_kubernetes_app()
                except Exception:
                    traceback.print_exc()
                    continue

                write_yaml(out_dir, cluster, out)

                hpa = job_config.get_autoscaling_metric_spec(
                    name=get_kubernetes_app_name(service, job_config.instance),
                    cluster=cluster,
                    namespace=job_config.get_namespace(),
                )
                if hpa:
                    write_yaml(out_dir, cluster, hpa)

                pdr = job_config.get_pod_disruption_budget()
                write_yaml(out_dir, cluster, pdr)

            for k8s_obj in fake_kube_client.created_objects:
                write_yaml(out_dir, cluster, k8s_obj)


def paasta_yaml(args):
    out_dir = args.out_dir
    if out_dir is None:
        out_dir = tempfile.mkdtemp()
        print(f"Output will be in {out_dir}")

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for service in args.services:
            for cluster in list_clusters(service=service):
                futures.append(executor.submit(generate_yamls_for_cluster, service, out_dir, args, cluster))

        # there's gotta be a better way
        for future in concurrent.futures.as_completed(futures):
            future.result()
