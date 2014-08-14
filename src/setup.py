#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name           = 'service-deployment-tools',
    version        = '0.2.11',
    provides       = ["service_deployment_tools"],
    author         = 'Kyle Anderson',
    author_email   = 'kwa@yelp.com',
    description    = 'Tools for Yelps SOA infrastructure',
    packages       = find_packages(exclude=["tests", "scripts"]),
    setup_requires = ['setuptools'],
    include_package_data=True,
    install_requires = [
        'isodate >= 0.5.0',
        'service-configuration-lib >= 0.6.0',
        'marathon == 0.3.2',
        'argparse',
        'pycurl',
        'sensu-plugin == 0.1.0',
        'requests',
        'pysensu-yelp >= 0.1.5',
        'GitPython == 0.1.7',
        'kazoo == 1.3.1',
    ],
    scripts = [
        'service_deployment_tools/setup_marathon_job.py',
        'service_deployment_tools/setup_chronos_jobs.py',
        'service_deployment_tools/am_i_mesos_leader.py',
        'service_deployment_tools/synapse_srv_namespaces_fact.py',
        'service_deployment_tools/cleanup_marathon_jobs.py',
        'service_deployment_tools/check_marathon_services_frontends.py',
        'service_deployment_tools/list_marathon_service_instances.py',
        'service_deployment_tools/monitoring/check_synapse_replication.py',
        'service_deployment_tools/monitoring/check_classic_service_replication.py',
        'service_deployment_tools/deploy_marathon_services',
        'service_deployment_tools/generate_deployments_json.py',
        'service_deployment_tools/check_marathon_services_replication.py'
    ]
)
