#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse

from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.paasta_metastatus import get_marathon_client
from paasta_tools.utils import paasta_print


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'Helper script to get the number of in-progress marathon deployments'
        )
    )
    return parser.parse_args()


def get_deployments():
    marathon_config = load_marathon_config()
    marathon_client = get_marathon_client(marathon_config)
    deployments = marathon_client.list_deployments()
    return deployments


def main():
    parse_args()
    paasta_print(len(get_deployments()))


if __name__ == "__main__":
    main()
