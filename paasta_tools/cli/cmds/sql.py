#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import collections.abc
import concurrent.futures
import inspect
import json
import os
import sqlite3
import traceback
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import Iterable
from typing import Type
from typing import Union

from service_configuration_lib import DEFAULT_SOA_DIR  # type: ignore

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "sql",
        help="Produce a sqlite3 database of all paasta service.instances",
        description="Produce a sqlite3 database of all paasta service.instances",
    )
    parser.add_argument(
        "-y",
        "-d",
        "--yelpsoa-config-root",
        "--soa-dir",
        dest="soa_dir",
        help="A directory from which yelpsoa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    parser.add_argument(
        "-D",
        "--database",
        dest="database",
        help="Path to sqlite3 file which the data should be written into",
        default="paasta.sqlite3",
    )
    parser.add_argument(
        "-c",
        "--clusters",
        help="Clusters to include in the generated database (comma-separated). Defaults to all.",
        default=list_clusters(),
        type=lambda clusters: clusters.split(","),
    )

    parser.set_defaults(command=paasta_sql)


IGNORED_METHODS = {
    "get_desired_instances",
    "get_autoscaled_instances",
    "get_boto_secret_hash",
    "get_boto_volume",
    "get_container_env",
    "get_kubernetes_environment",
    "get_cap_drop",
    "get_cap_add",
    "get_docker_init",
}

EXTRA_METHODS = {"is_autoscaling_enabled"}

EXPAND_DICTS = {
    "get_autoscaling_params": "autoscaling",
    "get_monitoring": "monitoring",
}

PAASTA_K8S_INSTANCE_TYPE_CLASSES = (
    EksDeploymentConfig,
    KubernetesDeploymentConfig,
)


def removeprefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        return text[len(prefix) :]
    else:
        return text


# TODO: fully type this
def make_getter(method: Callable, required_params: Collection[str]) -> Callable:
    # TODO: fully type this
    def getter(instance_config: InstanceConfig, **kwargs: Any) -> Any:
        subset_kwargs = {
            key: value for key, value in kwargs.items() if key in required_params
        }
        try:
            return method(instance_config, **subset_kwargs)
        except (InvalidInstanceConfig):
            return None

    return getter


# TODO: fully type this
def make_dict_getter(
    method: Callable, required_params: Collection[str], key: Any
) -> Callable:
    # TODO: fully type this
    def dict_getter(instance_config: InstanceConfig, **kwargs: Any) -> Any:
        subset_kwargs = {
            key: value for key, value in kwargs.items() if key in required_params
        }
        try:
            d = method(instance_config, **subset_kwargs)
            return d.get(key)
        except (InvalidInstanceConfig):
            return None

    return dict_getter


def column_names_and_getters_for_class(
    cls: Type[InstanceConfig],
    allowed_params: Collection[str] = (
        "self",
        "system_paasta_config",
        "service_namespace_config",
        "soa_dir",
    ),
    # TODO: better type this Callable
) -> Dict[str, Callable]:

    column_names_and_getters = {}
    for method_name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
        if (
            method_name.startswith("get_")
            and method_name not in IGNORED_METHODS
            or method_name in EXTRA_METHODS
        ):
            sig = inspect.signature(method)
            required_params = [
                pn
                for pn, p in sig.parameters.items()
                if p.default == inspect.Parameter.empty
            ]

            if set(required_params).issubset(allowed_params):
                if method_name in EXPAND_DICTS:
                    column_name_prefix = EXPAND_DICTS[method_name]
                    # TODO: we should refactor this to use typing.get_type_hints()
                    retval_keys = sig._return_annotation.__annotations__.keys()  # type: ignore
                    for retval_key in retval_keys:
                        column_name = f"{column_name_prefix}_{retval_key}"
                        column_names_and_getters[column_name] = make_dict_getter(
                            method, required_params, retval_key
                        )
                else:
                    column_name = removeprefix(method_name, "get_")
                    column_names_and_getters[column_name] = make_getter(
                        method, required_params
                    )
            # else:
            #     print(f"ignoring {method_name} because it has extra parameters {set(required_params) - set(allowed_params)}")
    return column_names_and_getters


def create_table_for_class(
    cursor: sqlite3.Cursor,
    cls: Union[
        Type[KubernetesDeploymentConfig],
        Type[EksDeploymentConfig],
    ],
) -> None:
    column_names = list(column_names_and_getters_for_class(cls).keys())
    comma_newline = """,
            """
    query = f"""
        CREATE TABLE {cls.config_filename_prefix}_instance(
            {comma_newline.join(column_names)}
        )
    """
    cursor.execute(query)


# TODO: fully type this
def format_for_sqlite(value: Any) -> Any:
    if isinstance(value, collections.abc.Iterator):
        value = list(value)

    if hasattr(value, "to_dict"):
        # kubernetes client lib objects
        value = value.to_dict()

    if isinstance(value, (list, tuple)):
        value = [format_for_sqlite(x) for x in value]

    if isinstance(value, (dict)):
        value = {k: format_for_sqlite(v) for (k, v) in value.items()}

    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value)
    else:
        return value


def insert_instances(
    cursor: sqlite3.Cursor,
    instance_configs: Iterable[InstanceConfig],
    system_paasta_config: SystemPaastaConfig,
    soa_dir: str,
) -> None:
    instance_configs = list(instance_configs)
    if not instance_configs:
        return

    cls = type(instance_configs[0])

    column_names_and_getters = column_names_and_getters_for_class(cls)
    query = f"""
        INSERT INTO {cls.config_filename_prefix}_instance VALUES ({('?, ' * len(column_names_and_getters))[:-2]})
    """

    def calculate_values_for_instance(
        instance_config: Union[
            KubernetesDeploymentConfig,
            EksDeploymentConfig,
        ]
    ) -> Any:
        assert isinstance(instance_config, cls)
        values = []
        service_namespace_config = load_service_namespace_config(
            instance_config.service,
            instance_config.get_nerve_namespace(),  # TODO: refactor to use all of get_registrations()
        )

        for column_name, getter in column_names_and_getters.items():
            values.append(
                format_for_sqlite(
                    getter(
                        instance_config,
                        system_paasta_config=system_paasta_config,
                        soa_dir=soa_dir,
                        service_namespace_config=service_namespace_config,
                    )
                )
            )

        return values

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = [
            executor.submit(calculate_values_for_instance, x) for x in instance_configs
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                cursor.execute(query, future.result())
                print(result)
            except Exception:
                traceback.print_exc()


def paasta_sql(args: argparse.Namespace) -> None:
    """Print a list of Yelp services currently running
    :param args: argparse.Namespace obj created from sys.args by cli"""

    system_paasta_config = load_system_paasta_config()

    with sqlite3.connect(args.database) as conn:
        create_table_for_class(conn.cursor(), KubernetesDeploymentConfig)
        create_table_for_class(conn.cursor(), EksDeploymentConfig)

    os.environ["KUBECONFIG"] = "~/.kube/config"
    for cluster in args.clusters:
        for service in list_services(args.soa_dir):
            pscl = PaastaServiceConfigLoader(
                service=service,
                soa_dir=args.soa_dir,
                load_deployments=True,
            )

            os.environ["KUBECONTEXT"] = cluster
            with sqlite3.connect(args.database) as conn:
                for instance_type_class in PAASTA_K8S_INSTANCE_TYPE_CLASSES:
                    insert_instances(
                        conn.cursor(),
                        pscl.instance_configs(
                            cluster=cluster, instance_type_class=instance_type_class
                        ),
                        system_paasta_config=system_paasta_config,
                        soa_dir=args.soa_dir,
                    )
