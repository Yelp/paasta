#!/usr/bin/env python
import argparse
import itertools

from paasta_tools.marathon_tools import get_list_of_marathon_clients
from paasta_tools.utils import paasta_print


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'Helper script to get the number of in-progress marathon deployments'
        ),
    )
    return parser.parse_args()


def get_deployments():
    clients = get_list_of_marathon_clients()
    return [deployment for deployment in itertools.chain.from_iterable(
        c.list_deployments() for c in clients
    )]


def main():
    parse_args()
    paasta_print(len(get_deployments()))


if __name__ == "__main__":
    main()
