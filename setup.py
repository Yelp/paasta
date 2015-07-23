#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
# http://stackoverflow.com/questions/14399534/how-can-i-reference-requirements-txt-for-the-install-requires-kwarg-in-setuptool/16624700#16624700
from pip.req import parse_requirements
install_reqs = parse_requirements('requirements.txt')
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name           = 'paasta-tools',
    # Don't bump version manually. See `make release` docs in ./Makefile
    version        = '0.12.27',
    provides       = ["paasta_tools"],
    author         = 'Kyle Anderson',
    author_email   = 'kwa@yelp.com',
    description    = 'Tools for Yelps SOA infrastructure',
    packages       = find_packages(exclude=["tests", "scripts"]),
    setup_requires = ['setuptools'],
    include_package_data=True,
    install_requires = reqs,
    scripts = [
        'paasta_tools/setup_marathon_job.py',
        'paasta_tools/setup_chronos_jobs.py',
        'paasta_tools/am_i_mesos_leader.py',
        'paasta_tools/paasta_execute_docker_command.py',
        'paasta_tools/synapse_srv_namespaces_fact.py',
        'paasta_tools/cleanup_marathon_jobs.py',
        'paasta_tools/list_marathon_service_instances.py',
        'paasta_tools/monitoring/check_synapse_replication.py',
        'paasta_tools/monitoring/check_classic_service_replication.py',
        'paasta_tools/deploy_marathon_services',
        'paasta_tools/generate_all_deployments',
        'paasta_tools/generate_deployments_for_service.py',
        'paasta_tools/check_marathon_services_replication.py',
        'paasta_tools/check_mesos_resource_utilization.py',
        'paasta_tools/generate_services_yaml.py',
        'paasta_tools/generate_services_file.py',
        'paasta_tools/paasta_serviceinit.py',
        'paasta_tools/paasta_metastatus.py',
        'paasta_tools/paasta_cli/paasta_cli.py',
        'paasta_tools/paasta_cli/paasta_tabcomplete.sh',
    ],
    package_data = {'': ['paasta_cli/fsm/templates/*.tmpl']},
)
