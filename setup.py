#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name           = 'paasta-tools',
    version        = '0.9.9',
    provides       = ["paasta_tools"],
    author         = 'Kyle Anderson',
    author_email   = 'kwa@yelp.com',
    description    = 'Tools for Yelps SOA infrastructure',
    packages       = find_packages(exclude=["tests", "scripts"]),
    setup_requires = ['setuptools'],
    include_package_data=True,
    install_requires = [
        'isodate >= 0.5.0',
        'service-configuration-lib == 0.8.2',
        'marathon == 0.6.10',
        'argparse',
        'requests',
        'pysensu-yelp >= 0.1.5',
        'GitPython == 0.1.7',
        'kazoo >= 2.0.0',
        'sensu-plugin == 0.1.0',
        'argcomplete == 0.8.1',
        'mesos.cli == 0.1.3',
        'humanize == 0.5.1',
    ],
    scripts = [
        'paasta_tools/setup_marathon_job.py',
        'paasta_tools/setup_chronos_jobs.py',
        'paasta_tools/am_i_mesos_leader.py',
        'paasta_tools/synapse_srv_namespaces_fact.py',
        'paasta_tools/cleanup_marathon_jobs.py',
        'paasta_tools/check_marathon_services_frontends.py',
        'paasta_tools/list_marathon_service_instances.py',
        'paasta_tools/monitoring/check_synapse_replication.py',
        'paasta_tools/monitoring/check_classic_service_replication.py',
        'paasta_tools/deploy_marathon_services',
        'paasta_tools/generate_deployments_json.py',
        'paasta_tools/check_marathon_services_replication.py',
        'paasta_tools/check_mesos_resource_utilization.py',
        'paasta_tools/generate_services_yaml.py',
        'paasta_tools/cleanup_marathon_orphaned_containers.py',
        'paasta_tools/paasta_serviceinit.py',
        'paasta_tools/paasta_cli/paasta_cli.py',
        'paasta_tools/paasta_cli/paasta_tabcomplete.sh',
    ]
)
