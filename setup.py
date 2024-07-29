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
import glob

from pkg_resources import yield_lines
from setuptools import find_packages
from setuptools import setup

from paasta_tools import __version__


def get_install_requires():
    with open("requirements-minimal.txt", "r") as f:
        minimal_reqs = list(yield_lines(f.read()))

    return minimal_reqs


setup(
    name="paasta-tools",
    version=__version__,
    provides=["paasta_tools"],
    author="Compute Infrastructure @ Yelp",
    author_email="compute-infra@yelp.com",
    description="Tools for Yelps SOA infrastructure",
    packages=find_packages(exclude=("tests*", "scripts*")),
    include_package_data=True,
    python_requires=">=3.8.0",
    install_requires=get_install_requires(),
    scripts=[
        "paasta_tools/apply_external_resources.py",
        "paasta_tools/check_autoscaler_max_instances.py",
        "paasta_tools/check_cassandracluster_services_replication.py",
        "paasta_tools/check_flink_services_health.py",
        "paasta_tools/check_kubernetes_api.py",
        "paasta_tools/check_kubernetes_services_replication.py",
        "paasta_tools/check_oom_events.py",
        "paasta_tools/check_spark_jobs.py",
        "paasta_tools/cleanup_kubernetes_cr.py",
        "paasta_tools/cleanup_kubernetes_crd.py",
        "paasta_tools/cleanup_kubernetes_jobs.py",
        "paasta_tools/cli/paasta_tabcomplete.sh",
        "paasta_tools/delete_kubernetes_deployments.py",
        "paasta_tools/generate_all_deployments",
        "paasta_tools/generate_deployments_for_service.py",
        "paasta_tools/generate_services_file.py",
        "paasta_tools/generate_services_yaml.py",
        "paasta_tools/generate_authenticating_services.py",
        "paasta_tools/kubernetes/bin/kubernetes_remove_evicted_pods.py",
        "paasta_tools/kubernetes/bin/paasta_cleanup_stale_nodes.py",
        "paasta_tools/kubernetes/bin/paasta_secrets_sync.py",
        "paasta_tools/log_task_lifecycle_events.py",
        "paasta_tools/monitoring/kill_orphaned_docker_containers.py",
        "paasta_tools/paasta_deploy_tron_jobs",
        "paasta_tools/paasta_execute_docker_command.py",
        "paasta_tools/paasta_remote_run.py",
        "paasta_tools/setup_istio_mesh.py",
        "paasta_tools/setup_kubernetes_cr.py",
        "paasta_tools/setup_kubernetes_crd.py",
        "paasta_tools/setup_kubernetes_internal_crd.py",
        "paasta_tools/setup_kubernetes_job.py",
        "paasta_tools/setup_prometheus_adapter_config.py",
        "paasta_tools/synapse_srv_namespaces_fact.py",
    ]
    + glob.glob("paasta_tools/contrib/*.sh")
    + glob.glob("paasta_tools/contrib/[!_]*.py"),
    entry_points={
        "console_scripts": [
            "paasta=paasta_tools.cli.cli:main",
            "paasta-api=paasta_tools.api.api:main",
            "paasta-deployd=paasta_tools.deployd.master:main",
            "paasta-fsm=paasta_tools.cli.fsm_cmd:main",
            "paasta_prune_completed_pods=paasta_tools.prune_completed_pods:main",
            "paasta_cleanup_tron_namespaces=paasta_tools.cleanup_tron_namespaces:main",
            "paasta_list_kubernetes_service_instances=paasta_tools.list_kubernetes_service_instances:main",
            "paasta_list_tron_namespaces=paasta_tools.list_tron_namespaces:main",
            "paasta_setup_tron_namespace=paasta_tools.setup_tron_namespace:main",
            "paasta_docker_wrapper=paasta_tools.docker_wrapper:main",
            "paasta_firewall_update=paasta_tools.firewall_update:main",
            "paasta_firewall_logging=paasta_tools.firewall_logging:main",
            "paasta_oom_logger=paasta_tools.oom_logger:main",
            "paasta_broadcast_log=paasta_tools.broadcast_log_to_services:main",
            "paasta_dump_locally_running_services=paasta_tools.dump_locally_running_services:main",
            "paasta_habitat_fixer=paasta_tools.contrib.habitat_fixer:main",
        ],
        "paste.app_factory": ["paasta-api-config=paasta_tools.api.api:make_app"],
    },
)
