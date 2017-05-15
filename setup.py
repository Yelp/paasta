#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from __future__ import absolute_import
from __future__ import unicode_literals

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
    packages=find_packages(exclude=("tests*", "scripts*", "task_processing")),
    include_package_data=True,
    install_requires=[
        'argcomplete >= 0.8.1',
        'bravado == 8.4.0',
        'choice == 0.1',
        'chronos-python == 0.37.0',
        'cookiecutter == 1.4.0',
        # Don't update this unless you have confirmed the client works with
        # the Docker version deployed on PaaSTA servers
        'docker-py == 1.2.3',
        'dulwich',
        'ephemeral-port-reserve >= 1.0.1',
        'futures',
        'gevent == 1.1.1',
        'humanize >= 0.5.1',
        'httplib2 >= 0.9,<= 1.0',
        'inotify >= 0.2.8',
        'isodate >= 0.5.0',
        'jsonschema[format]',
        'kazoo >= 2.0.0',
        'marathon >= 0.8.1',
        'mesos.interface == 1.1.0',
        'path.py >= 8.1',
        'progressbar2 >= 3.10.0',
        'pyramid == 1.7',
        'pyramid-swagger == 2.2.3',
        'pysensu-yelp >= 0.2.2',
        'pytimeparse >= 1.1.0',
        'pytz >= 2014.10',
        'python-crontab>=2.1.1',
        'python-dateutil >= 2.4.0',
        'retry',
        'requests == 2.6.2',
        'requests-cache >= 0.4.10,<= 0.5.0',
        # We install this from git
        # 'sensu-plugin >= 0.2.0',
        'service-configuration-lib >= 0.12.0',
        'ujson == 1.35',
        'yelp-clog >= 2.7.2',
    ],
    scripts=[
        'paasta_tools/am_i_mesos_leader.py',
        'paasta_tools/autoscale_all_services.py',
        'paasta_tools/check_marathon_services_replication.py',
        'paasta_tools/cleanup_marathon_jobs.py',
        'paasta_tools/paasta_deploy_chronos_jobs',
        'paasta_tools/deploy_marathon_services',
        'paasta_tools/generate_all_deployments',
        'paasta_tools/generate_deployments_for_service.py',
        'paasta_tools/generate_services_file.py',
        'paasta_tools/generate_services_yaml.py',
        'paasta_tools/get_mesos_leader.py',
        'paasta_tools/list_marathon_service_instances.py',
        'paasta_tools/monitoring/check_classic_service_replication.py',
        'paasta_tools/monitoring/check_synapse_replication.py',
        'paasta_tools/monitoring/kill_orphaned_docker_containers.py',
        'paasta_tools/cli/paasta_tabcomplete.sh',
        'paasta_tools/paasta_execute_docker_command.py',
        'paasta_tools/paasta_maintenance.py',
        'paasta_tools/paasta_metastatus.py',
        'paasta_tools/paasta_remote_run.py',
        'paasta_tools/paasta_serviceinit.py',
        'paasta_tools/setup_marathon_job.py',
        'paasta_tools/synapse_srv_namespaces_fact.py',
    ] + glob.glob('paasta_tools/contrib/*'),
    package_data={str(''): [str('cli/fsm/template/*/*'), str('cli/schemas/*.json'), str('api/api_docs/*.json')]},
    entry_points={
        'console_scripts': [
            'paasta=paasta_tools.cli.cli:main',
            'paasta-api=paasta_tools.api.api:main',
            'paasta-deployd=paasta_tools.deployd.master:main',
            'paasta_autoscale_cluster=paasta_tools.autoscale_cluster:main',
            'paasta_cleanup_chronos_jobs=paasta_tools.cleanup_chronos_jobs:main',
            'paasta_check_chronos_jobs=paasta_tools.check_chronos_jobs:main',
            'paasta_list_chronos_jobs=paasta_tools.list_chronos_jobs:main',
            'paasta_setup_chronos_job=paasta_tools.setup_chronos_job:main',
            'paasta_chronos_rerun=paasta_tools.chronos_rerun:main',
            'paasta_cleanup_maintenance=paasta_tools.cleanup_maintenance:main',
            'paasta_docker_wrapper=paasta_tools.docker_wrapper:main',
            'paasta_firewall_update=paasta_tools.firewall_update:main',
        ],
        'paste.app_factory': [
            'paasta-api-config=paasta_tools.api.api:make_app'
        ],
    },
)
