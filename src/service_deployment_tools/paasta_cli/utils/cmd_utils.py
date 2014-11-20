#!/usr/bin/env python
"""
Contains helper functions common to all paasta commands or the client
"""


import glob
import os


def paasta_commands():
    """
    Read the files names in the cmds directory to determine the various commands
    the paasta client is able to execute
    :return: a list of string such as ['list','check'] that correspond to a
    file in cmds
    """
    path = '%s/cmds/*.py' % os.path.abspath('.')
    for file_name in glob.glob(path):
        basename = os.path.basename(file_name)
        root, _ = os.path.splitext(basename)
        if root == '__init__':
            continue
        yield root