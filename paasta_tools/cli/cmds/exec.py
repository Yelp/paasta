#!/usr/bin/env python
# Copyright 2015-2022 Yelp Inc.
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
"""PaaSTA exec for humans"""
import argparse
import random
import shlex
import subprocess
import sys

from kubernetes import client
from kubernetes import config

from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import guess_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import verify_instances
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers) -> None:
    exec_parser = subparsers.add_parser(
        "exec",
        help="Execs into a running PaaSTA service",
        description=(
            "'paasta exec' works by running some k8s commands for you "
            "in a human-friendly way."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    exec_parser.add_argument(
        "-s",
        "--service",
        help="The name of the service you wish to inspect. Defaults to autodetect.",
    ).completer = lazy_choices_completer(list_services)
    exec_parser.add_argument(
        "-c",
        "--cluster",
        help="The cluster to exec into.",
        type=str,
    ).completer = completer_clusters
    exec_parser.add_argument(
        "-i",
        "--instance",
        help="The instance to exec into.",
        type=str,
    ).completer = completer_clusters
    pod_help = "The pod to exec into. Defaults to a random running pod for the service."
    exec_parser.add_argument("-p", "--pod", help=pod_help)

    exec_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help=f"Define a different soa config directory. Defaults to %(default)s.",
    )

    exec_parser.set_defaults(command=paasta_exec)


def completer_clusters(prefix, parsed_args, **kwargs):
    service = parsed_args.service or guess_service_name()
    if service in list_services():
        return list_clusters(service)
    else:
        return list_clusters()


def authenticate_and_load_api_object(cluster):
    config.load_kube_config(config_file="/etc/kubernetes/paasta.conf", context=cluster)
    return client.CoreV1Api()


def get_pods_for_instance(k8s_v1, service, instance, namespace):
    return k8s_v1.list_namespaced_pod(
        namespace,
        watch=False,
        label_selector=f"paasta.yelp.com/service={service},paasta.yelp.com/instance={instance}",
        field_selector="status.phase==Running",
    )


def paasta_exec(args: argparse.Namespace) -> int:
    """Execs into a running Paasta service.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    soa_dir = args.soa_dir

    service = figure_out_service_name(args, soa_dir)
    namespace = (
        "paasta"  # keeping this here since we want to shard the paasta namespace
    )

    if (
        args.cluster is None
        or args.instance is None
        or len(args.instance.split(",")) > 2
    ):
        print(
            PaastaColors.red("You must specify one cluster and one instance."),
            file=sys.stderr,
        )
        return 1
    cluster = args.cluster
    if verify_instances(args.instance, service, [cluster]):
        return 1
    instance = args.instance
    k8s_v1 = authenticate_and_load_api_object(cluster)
    pods = get_pods_for_instance(k8s_v1, service, instance, namespace)
    pod_name = (
        args.pod
        if args.pod
        else None
        if not pods.items
        else random.choice(pods.items).metadata.name
    )
    if not pod_name or pod_name not in [pod.metadata.name for pod in pods.items]:
        print(
            PaastaColors.red("We can't find a running pod for your service."),
            file=sys.stderr,
        )
        return 1
    cmd = f"kubectl --kubeconfig /etc/kubernetes/paasta.conf --context {cluster} -i -t -n paasta exec {pod_name} -- sh -c 'clear; (bash || ash || sh)'"
    subprocess.check_call(shlex.split(cmd))
    return 0
