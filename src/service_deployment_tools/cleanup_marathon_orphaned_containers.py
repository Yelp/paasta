#!/usr/bin/env python
"""
Usage: cleanup_marathon_orphaned_containers.py [options]
"""

import argparse
import calendar
import datetime
import logging

import docker

from service_deployment_tools.marathon_tools import get_deployed_images

log = logging.getLogger('__main__')


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
    log.info('Looking for containers older than %s' % max_age_timestamp)

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
    deployed_images = get_deployed_images()
    running_mesos_old_undeployed_containers = get_undeployed_containers(running_mesos_old_containers, deployed_images)

    log.info("I found these containers running:")
    [log.info(container) for container in running_containers]

    for container in running_mesos_old_undeployed_containers:
        log.warning("Killing long-lived, undeployed Mesos container %s" % container)
        if not args.dry_run:
            client.kill(container)
            client.remove_image(container['Image'], force=True)


if __name__ == "__main__":
    main()
