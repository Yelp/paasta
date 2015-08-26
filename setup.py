#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name           = 'paasta-tools',
    # Don't bump version manually. See `make release` docs in ./Makefile
    version        = '0.12.58',
    provides       = ["paasta_tools"],
    author         = 'Kyle Anderson',
    author_email   = 'kwa@yelp.com',
    description    = 'Tools for Yelps SOA infrastructure',
    packages       = find_packages(exclude=["tests", "scripts"]),
    setup_requires = ['setuptools'],
    include_package_data=True,
    install_requires = [
        'argcomplete >= 0.8.1',
        # argparse is pinned to 1.2.1 since it comes in the core python2.7 libs and pip can't seem to override it
        'argparse == 1.2.1',
        'chronos-python >= 0.32.1-yelp1, <= 1.0.0',
        # Don't update this unless you have confirmed the client works with the Docker version deployed on PaaSTA servers
        'docker-py == 1.2.3',
        'dulwich >= 0.9.8',
        'humanize >= 0.5.1',
        'httplib2 >= 0.9, <= 1.0',
        'isodate >= 0.5.0',
        'kazoo >= 2.0.0',
        'marathon >= 0.7.1-yelp2',
        'mesos.cli >= 0.1.3',
        'ordereddict >= 1.1',
        'pysensu-yelp >= 0.1.5',
        'python-dateutil >= 2.4.0',
        'requests >= 2.6.0',
        'requests-cache >= 0.4.10, <= 0.5.0',
        # scibrereader pins exact versions of yelp_clog which makes everything
        # sad, so we're going to just have an exact version of scribereader
        # To upgrade this you must also upgrade the yelp_clog pin
        'scribereader == 0.1.16',
        'sensu-plugin >= 0.1.0',
        'service-configuration-lib >= 0.9.2',
        # scribereader requires this _exact_ version
        'yelp_clog == 2.1.2',
    ],
    scripts = [
        'paasta_tools/am_i_mesos_leader.py',
        'paasta_tools/check_marathon_services_replication.py',
        'paasta_tools/check_mesos_resource_utilization.py',
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
        'paasta_tools/paasta_cli/paasta_cli.py',
        'paasta_tools/paasta_cli/paasta_tabcomplete.sh',
        'paasta_tools/paasta_execute_docker_command.py',
        'paasta_tools/paasta_metastatus.py',
        'paasta_tools/paasta_serviceinit.py',
        'paasta_tools/setup_chronos_job.py',
        'paasta_tools/setup_marathon_job.py',
        'paasta_tools/synapse_srv_namespaces_fact.py',
    ],
    package_data = {'': ['paasta_cli/fsm/templates/*.tmpl']},
)
