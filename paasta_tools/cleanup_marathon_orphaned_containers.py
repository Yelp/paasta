#!/usr/bin/env python
"""
Usage: cleanup_marathon_orphaned_containers.py [options]

Reaps containers that get lost in the shuffle when we restart Mesos slaves too
hard. See https://jira.yelpcorp.com/browse/MESOS-120.

Command line options:

- -n, --dry-run: Report what would be cleaned up but don't do it
- -m, --max-age: Containers older than this will be cleaned up
- -v, --verbose: Verbose output
"""

import argparse
import calendar
import datetime
import logging
import socket
import sys

import docker

from paasta_tools.generate_deployments_json import get_service_from_docker_image
from paasta_tools.marathon_tools import DeploymentsJson
from paasta_tools.marathon_tools import get_cluster
from paasta_tools.utils import _log

log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


def get_running_containers(client):
    """Given a docker-py Docker client, return the docker containers running on
    this machine (docker ps).
    """
    return client.containers()


def get_mesos_containers(containers):
    """Given a list of Docker containers as from get_running_containers(),
    return a list of the containers started by Mesos.
    """
    mesos_containers = []
    for image in containers:
        if any([name for name in image.get('Names', []) if name.startswith('/mesos-')]):
            mesos_containers.append(image)
    return mesos_containers


def get_old_containers(containers, max_age, now=None):
    """Given a list of Docker containers as from get_running_containers(),
    return a list of the containers started more than max_age minutes before
    now.
    """
    age_delta = datetime.timedelta(minutes=max_age)
    if now is None:
        now = datetime.datetime.utcnow()
    max_age_timestamp = calendar.timegm((now - age_delta).timetuple())
    log.info('Looking for containers older than %s minutes' % max_age)

    return [container for container in containers
            if container.get('Created') and container.get('Created') < max_age_timestamp]


def get_undeployed_containers(containers, deployed_images):
    """Given a list of Docker containers, as from get_running_containers(); and
    a set of images that are supposed/allowed to be deployed, as from
    marathon_tools.get_deployed_images(); return a list of containers that are
    not expected to be running.
    """
    undeployed_containers = []
    for container in containers:
        image = container.get('Image', 'NO IMAGE')
        # Strip out the registry url (everything before the first /). The
        # subsequent re-gluing is just in case someone has a / in their actual
        # image name.
        image = '/'.join(image.split('/')[1:])

        if image not in deployed_images:
            undeployed_containers.append(container)
    return undeployed_containers


def parse_args():
    parser = argparse.ArgumentParser(
        description='Stop Docker containers spawned by Mesos which are no longer supposed to be running')
    parser.add_argument(
        '-n', '--dry-run',
        action='store_true',
        dest='dry_run',
        default=False,
    )
    parser.add_argument(
        '-m', '--max-age',
        dest='max_age',
        help='Number of minutes old a docker container has to be before killing it',
        default=60,
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        dest='verbose',
        default=False,
    )
    args = parser.parse_args()
    args.max_age = int(args.max_age)
    return args


def kill_containers(containers, client, dry_run):
    cluster = get_cluster()
    hostname = socket.getfqdn()
    for container in containers:
        # The container object looks like this
        # {u'Status': u'Up About an hour', u'Created': 1424558722,
        #  u'Image': u'docker-paasta.yelpcorp.com:443/services-example_service:paasta-591ae8a7b.......',
        #  u'Ports': [{u'IP': u'0.0.0.0', u'Type': u'tcp', u'PublicPort': 31000, u'PrivatePort': 8888}],
        #  u'Command': u"/bin/sh -c 'cd /home/batch/example_service; ./run'",
        #  u'Names': [u'/mesos-aa62b345-c16c-4ab1-ba8b-8fa4a9461242'],
        #  u'Id': u'e7fc3bd3b27017c413b3ef624d98988e686d7920514f697b1c69e7264d3ef36d'}
        if not dry_run:
            # The docker-py docs are short on details about what kinds of
            # exceptions can be raised when something goes wrong. I see a bunch
            # of custom exceptions in docker.errors. Everything is done with
            # requests which has its own set of things it can throw.
            #
            # So: catch everything, log it, and move on.
            image_name = container['Image']
            service_name = get_service_from_docker_image(image_name)
            try:
                client.kill(container)
                client.remove_container(container)
                client.remove_image(image_name)
                message = "Killed orphaned container %s on %s. Not supposed to be deployed." % (image_name, hostname)
            except Exception as e:
                message = "Failed to kill orphaned docker container %s on %s. Error: %s" % (image_name, hostname, e)
            _log(service_name=service_name,
                 line=message,
                 component='deploy',
                 level='event',
                 cluster=cluster)


def main():
    args = parse_args()
    logging.basicConfig()
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    client = docker.Client()
    running_containers = get_running_containers(client)
    running_mesos_containers = get_mesos_containers(running_containers)
    running_mesos_old_containers = get_old_containers(running_mesos_containers, args.max_age)
    deployed_images = DeploymentsJson.load().get_deployed_images()
    # Blow up if something went wrong getting deployed images?
    assert deployed_images
    running_mesos_old_undeployed_containers = get_undeployed_containers(running_mesos_old_containers, deployed_images)
    kill_containers(running_mesos_old_undeployed_containers, client, args.dry_run)


if __name__ == '__main__':
    main()
