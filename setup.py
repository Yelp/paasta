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

# Minimal setup.py to handle standalone scripts alongside pyproject.toml
# All other metadata, dependencies, and entry points are defined in pyproject.toml
import glob

from setuptools import setup

setup(
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
        "paasta_tools/kubernetes/bin/paasta_cleanup_remote_run_resources.py",
        "paasta_tools/kubernetes/bin/paasta_cleanup_stale_nodes.py",
        "paasta_tools/kubernetes/bin/paasta_secrets_sync.py",
        "paasta_tools/paasta_deploy_tron_jobs",
        "paasta_tools/paasta_execute_docker_command.py",
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
)
