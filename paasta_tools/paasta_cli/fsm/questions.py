"""This is a home for functions that calculate arguments based on various user
inputs.
"""


import sys


def ask_srvname(srvname, auto):
    if srvname is None:
        if auto:
            sys.exit("I'd Really Rather You Didn't Use --auto Without --service-name")
        while not srvname:
            srvname = raw_input('Service name? ')
    return srvname
