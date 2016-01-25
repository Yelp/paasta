#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2015 Yelp Inc.
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


setup(
    name='paasta-tools',
    # Don't bump version manually. See `make release` docs in ./Makefile
    version='0.16.17',
    provides=["paasta_tools"],
    author='Kyle Anderson',
    author_email='kwa@yelp.com',
    description='Tools for Yelps SOA infrastructure',
    packages=find_packages(exclude=("tests*", "scripts*")),
    include_package_data=True,
    install_requires=[
        'argcomplete >= 0.8.1',
        # argparse is pinned to 1.2.1 since it comes in the core python2.7
        # libs and pip can't seem to override it
        'argparse == 1.2.1',
        'chronos-python == 0.34.0',
        # Don't update this unless you have confirmed the client works with
        # the Docker version deployed on PaaSTA servers
        'docker-py == 1.2.3',
        'dulwich == 0.10.0',
        'humanize >= 0.5.1',
        'httplib2 >= 0.9, <= 1.0',
        'isodate >= 0.5.0',
        'kazoo >= 2.0.0',
        'marathon >= 0.7.5',
        'mesos.cli == 0.1.3',
        'ordereddict >= 1.1',
        'path.py >= 8.1',
        'pysensu-yelp >= 0.2.2',
        'python-dateutil >= 2.4.0',
        'requests == 2.6.2',
        'requests-cache >= 0.4.10, <= 0.5.0',
        'sensu-plugin >= 0.1.0',
        'service-configuration-lib >= 0.9.2',
        'setuptools != 18.6',
        'tron == 0.6.1.1',
        'yelp_clog >= 2.2.0',
    ],
    scripts=[
        'paasta_tools/am_i_mesos_leader.py',
        'paasta_tools/check_marathon_services_replication.py',
        'paasta_tools/check_mesos_resource_utilization.py',
        'paasta_tools/cleanup_chronos_jobs.py',
        'paasta_tools/check_chronos_jobs.py',
        'paasta_tools/cleanup_marathon_jobs.py',
        'paasta_tools/deploy_chronos_jobs',
        'paasta_tools/deploy_marathon_services',
        'paasta_tools/generate_all_deployments',
        'paasta_tools/generate_deployments_for_service.py',
        'paasta_tools/generate_services_file.py',
        'paasta_tools/generate_services_yaml.py',
        'paasta_tools/list_chronos_jobs.py',
        'paasta_tools/list_marathon_service_instances.py',
        'paasta_tools/monitoring/check_classic_service_replication.py',
        'paasta_tools/monitoring/check_synapse_replication.py',
        'paasta_tools/cli/cli.py',
        'paasta_tools/cli/paasta_tabcomplete.sh',
        'paasta_tools/paasta_execute_docker_command.py',
        'paasta_tools/paasta_metastatus.py',
        'paasta_tools/paasta_serviceinit.py',
        'paasta_tools/setup_chronos_job.py',
        'paasta_tools/setup_marathon_job.py',
        'paasta_tools/synapse_srv_namespaces_fact.py',
    ] + glob.glob('paasta_tools/contrib/*'),
    package_data={'': ['cli/fsm/templates/*.tmpl']},
)
