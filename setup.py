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

from setuptools import find_packages
from setuptools import setup

from paasta_tools import __version__


setup(
    name='paasta-tools',
    version=__version__,
    provides=["paasta_tools"],
    author='Kyle Anderson',
    author_email='kwa@yelp.com',
    description='Tools for Yelps SOA infrastructure',
    packages=find_packages(exclude=("tests*", "scripts*")),
    include_package_data=True,
    install_requires=[
        # Make sure to modify requirements-minimal.txt as well!
        'a_sync >= 0.5.0',
        'argcomplete >= 0.8.1',
        'aiohttp >= 3.2.1',
        'boto3',
        'botocore',
        'bravado >= 8.4.0',
        'choice == 0.1',
        'chronos-python >= 1.2.0',
        'cookiecutter == 1.4.0',
        'croniter',
        # Don't update this unless you have confirmed the client works with
        # the Docker version deployed on PaaSTA servers
        'docker-py == 1.2.3',
        'dulwich >= 0.17.3',
        'ephemeral-port-reserve >= 1.0.1',
        'gevent == 1.1.1',
        'gunicorn >= 19.8.1',
        'humanize >= 0.5.1',
        'inotify >= 0.2.8',
        'isodate >= 0.5.0',
        'jsonschema[format]',
        'kazoo >= 2.0.0',
        'marathon >= 0.9.3',
        'mypy-extensions == 0.3.0',
        'progressbar2 >= 3.10.0',
        'pyramid >= 1.8',
        'pymesos >= 0.2.0',
        'pyramid-swagger >= 2.3.0',
        'pysensu-yelp >= 0.3.4',
        'pytimeparse >= 1.1.0',
        'pytz >= 2014.10',
        'python-crontab>=2.1.1',
        'python-dateutil >= 2.4.0',
        'python-iptables',
        'retry',
        'requests',
        'requests-cache >= 0.4.10,<= 0.5.0',
        'ruamel.yaml',
        'sensu-plugin',
        'service-configuration-lib >= 0.12.0',
        'syslogmp',
        'task-processing',
        'typing-extensions',
        'tzlocal',
        'ujson == 1.35',
        'wsgicors',
        'yelp-clog >= 2.7.2',
    ],
    scripts=[
        'paasta_tools/am_i_mesos_leader.py',
        'paasta_tools/autoscale_all_services.py',
        'paasta_tools/check_marathon_services_replication.py',
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
        'paasta_tools/monitoring/check_classic_service_replication.py',
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
