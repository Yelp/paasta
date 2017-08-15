#!/usr/bin/env python
from paasta_tools import chronos_tools

if __name__ == '__main__':
    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)
    jobs = [job['name'] for job in client.list()]

    for job in jobs:
        client.delete(job)
