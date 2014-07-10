#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name           = 'service-deployment-tools',
    version        = '0.0.2',
    provides       = ["service_deployment_tools"],
    author         = 'Kyle Anderson',
    author_email   = 'kwa@yelp.com',
    description    = 'Tools for Yelps SOA infrastructure',
    packages       = find_packages(exclude=["tests", "scripts"]),
    setup_requires = ['setuptools'],
    include_package_data=True,
    install_requires = [
        'isodate',
        'service-configuration-lib >= 0.5.0',
        'marathon',
        'argparse',
        'pycurl',
    ],
    scripts = [
        'service_deployment_tools/setup_marathon_job.py',
        'service_deployment_tools/setup_chronos_jobs.py',
	'service_deployment_tools/am_i_mesos_leader.py',
	'service_deployment_tools/synapse_srv_namespaces_fact.py'
    ]
)

