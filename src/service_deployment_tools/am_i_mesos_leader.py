#!/usr/bin/env python

from sys import exit
# Why so specific? We don't want any unneeded kwargs getting parsed
# and loaded from a normal 'import marathon_tools'; we need this to
# be fast and return almost immediately.
from marathon_tools import is_mesos_leader


def main():
    if is_mesos_leader():
        print True
        exit(0)
    else:
        print False
        exit(1)


if __name__ == "__main__":
    main()
