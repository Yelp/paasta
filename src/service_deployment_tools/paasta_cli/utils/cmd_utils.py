#!/usr/bin/env python
"""
Contains helper functions common to all paasta commands or the client
"""


import glob
import os

# List of commands the paasta client can execute
CMDS = None


def paasta_commands():
    """
    Read the files names in the cmds directory to determine the various commands
    the paasta client is able to execute
    :return: a list of string such as ['list','check'] that correspond to a
    file in cmds
    """
    global CMDS

    if CMDS is None:

        CMDS = []

        path = "%s/cmds/*.py" % os.path.abspath('.')

        for file_name in glob.glob(path):
            start = file_name.rfind('/') + 1
            end = file_name.rfind('.')
            CMDS.append(file_name[start:end])

        # Remove __init__.py
        CMDS.sort()
        CMDS.pop(0)

    return CMDS
