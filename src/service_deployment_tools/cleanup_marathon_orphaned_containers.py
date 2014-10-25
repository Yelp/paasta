#!/usr/bin/env python
"""
Usage: cleanup_marathon_orphaned_images.py [options]
"""

import argparse
import logging

import docker

log = logging.getLogger('__main__')


def get_running_images(client):
    return client.containers()


def get_running_mesos_images(running_images):
    running_mesos_images = []
    for image in running_images:
        if any([name for name in image.get('Names', '') if name.startswith('/mesos-')]):
            running_mesos_images.append(image)
    return running_mesos_images


def parse_args():
    parser = argparse.ArgumentParser(
        description='Stop Docker images spawned by Mesos which are no longer supposed to be running')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        dest='verbose',
        default=False,
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    client = docker.Client()
    running_images = get_running_images(client)
    running_mesos_images = get_running_mesos_images(running_images)
    from pprint import pprint
    pprint(running_mesos_images)


if __name__ == "__main__":
    main()
