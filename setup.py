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
    with open('requirements-minimal.txt', 'r') as f:
        minimal_reqs = list(yield_lines(f.read()))

    return minimal_reqs


setup(
    name='paasta-tools',
    version=__version__,
    provides=["paasta_tools"],
    author='Kyle Anderson',
    author_email='kwa@yelp.com',
    description='Tools for Yelps SOA infrastructure',
    packages=find_packages(exclude=("tests*", "scripts*")),
    include_package_data=True,
    install_requires=get_install_requires(),
    scripts=[
        'paasta_tools/am_i_mesos_leader.py',
        'paasta_tools/autoscale_all_services.py',
        'paasta_tools/check_marathon_services_replication.py',
        'paasta_tools/check_kubernetes_services_replication.py',
        'paasta_tools/check_oom_events.py',
        'paasta_tools/cleanup_marathon_jobs.py',
        'paasta_tools/paasta_deploy_chronos_jobs',
        'paasta_tools/deploy_marathon_services',
        'paasta_tools/paasta_deploy_tron_jobs',
        'paasta_tools/generate_all_deployments',
        'paasta_tools/generate_deployments_for_service.py',
        'paasta_tools/generate_services_file.py',
        'paasta_tools/generate_services_yaml.py',
        'paasta_tools/get_mesos_leader.py',
        'paasta_tools/list_marathon_service_instances.py',
        'paasta_tools/marathon_dashboard.py',
        'paasta_tools/monitoring/check_capacity.py',
        'paasta_tools/monitoring/check_chronos_has_jobs.py',
        'paasta_tools/monitoring/check_marathon_has_apps.py',
        'paasta_tools/monitoring/check_mesos_active_frameworks.py',
        'paasta_tools/monitoring/check_mesos_duplicate_frameworks.py',
        'paasta_tools/monitoring/check_mesos_quorum.py',
        'paasta_tools/monitoring/check_mesos_outdated_tasks.py',
        'paasta_tools/monitoring/check_synapse_replication.py',
        'paasta_tools/monitoring/kill_orphaned_docker_containers.py',
        'paasta_tools/cli/paasta_tabcomplete.sh',
        'paasta_tools/paasta_cluster_boost.py',
        'paasta_tools/paasta_execute_docker_command.py',
        'paasta_tools/paasta_maintenance.py',
        'paasta_tools/paasta_metastatus.py',
        'paasta_tools/paasta_remote_run.py',
        'paasta_tools/paasta_serviceinit.py',
        'paasta_tools/setup_kubernetes_job.py',
        'paasta_tools/setup_marathon_job.py',
        'paasta_tools/synapse_srv_namespaces_fact.py',
    ] + glob.glob('paasta_tools/contrib/*'),
    entry_points={
        'console_scripts': [
            'paasta=paasta_tools.cli.cli:main',
            'paasta-api=paasta_tools.api.api:main',
            'paasta-deployd=paasta_tools.deployd.master:main',
            'paasta_autoscale_cluster=paasta_tools.autoscale_cluster:main',
            'paasta_cleanup_chronos_jobs=paasta_tools.cleanup_chronos_jobs:main',
            'paasta_cleanup_tron_namespaces=paasta_tools.cleanup_tron_namespaces:main',
            'paasta_check_chronos_jobs=paasta_tools.check_chronos_jobs:main',
            'paasta_list_chronos_jobs=paasta_tools.list_chronos_jobs:main',
            'paasta_list_kubernetes_service_instances=paasta_tools.list_kubernetes_service_instances:main',
            'paasta_setup_chronos_job=paasta_tools.setup_chronos_job:main',
            'paasta_chronos_rerun=paasta_tools.chronos_rerun:main',
            'paasta_list_tron_namespaces=paasta_tools.list_tron_namespaces:main',
            'paasta_setup_tron_namespace=paasta_tools.setup_tron_namespace:main',
            'paasta_cleanup_maintenance=paasta_tools.cleanup_maintenance:main',
            'paasta_docker_wrapper=paasta_tools.docker_wrapper:main',
            'paasta_firewall_update=paasta_tools.firewall_update:main',
            'paasta_firewall_logging=paasta_tools.firewall_logging:main',
            'paasta_oom_logger=paasta_tools.oom_logger:main',
            'paasta_broadcast_log=paasta_tools.marathon_tools:broadcast_log_all_services_running_here_from_stdin',
        ],
        'paste.app_factory': [
            'paasta-api-config=paasta_tools.api.api:make_app',
        ],
    },
)
