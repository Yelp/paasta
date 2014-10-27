#!/usr/bin/env python
"""
Usage: cleanup_marathon_orphaned_images.py [options]
"""

import argparse
import calendar
import datetime
import logging

import docker

log = logging.getLogger('__main__')


def get_running_images(client):
    return client.containers()


def get_mesos_images(images):
    mesos_images = []
    for image in images:
        if any([name for name in image.get('Names', []) if name.startswith('/mesos-')]):
            mesos_images.append(image)
    return mesos_images


def get_old_images(images, max_age=60, now=None):
    age_delta = datetime.timedelta(minutes=max_age)
    if now is None:
        now = datetime.datetime.now()
    max_age_timestamp = calendar.timegm((now + age_delta).timetuple())

    return [image for image in images if image.get('Created', 0) > max_age_timestamp]


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
    from pprint import pprint
    running_images = get_running_images(client)
    pprint(running_images)
    mesos_images = get_mesos_images(running_images)
    old_images = get_old_images(mesos_images)
    pprint(old_images)


if __name__ == "__main__":
    main()
