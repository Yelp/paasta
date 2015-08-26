#!/usr/bin/env python
"""
Usage: ./am_i_mesos_leader.py

Check if this host is the curret mesos-master leader.
This is done by simply calling mesos_tools.is_mesos_leader.
Exits 0 if this is the leader, and 1 if it isn't.
"""

from sys import exit

from paasta_tools.mesos_tools import is_mesos_leader


def main():
    if is_mesos_leader():
        print True
        exit(0)
    else:
        print False
        exit(1)


if __name__ == "__main__":
    main()
