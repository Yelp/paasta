#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name           = 'service_deployment_tools',
    version        = '0.0.1',
    provides       = ["service_deployment_tools"],
    author         = 'Kyle Anderson',
    author_email   = 'kwa@yelp.com',
    description    = 'Tools for Yelps SOA infrastructure',
    packages       = find_packages(exclude=["tests"]),
    setup_requires = ['setuptools'],
    include_package_data=True,
    install_requires = [
    ],
    scripts = [
        'service_deployment_tools/setup_marathon_job.py',
        'service_deployment_tools/setup_chronos_jobs.py',
    ]
)

