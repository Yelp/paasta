#!/usr/bin/env python
"""
Usage: cleanup_marathon_orphaned_images.py [options]
"""

import argparse
import logging

log = logging.getLogger('__main__')


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
    print args


if __name__ == "__main__":
    main()
