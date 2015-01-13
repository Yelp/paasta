#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='service-wizard',
    version='0.0.1',
    provides=['service_wizard'],
    author='troscoe',
    author_email='troscoe@yelp.com',
    description='Interactive stuff for setting up services',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    include_package_data=True,
    install_requires=[
    ]
)

