"""This is a home for functions shared between the wizard.py and fsm.py
frontends.
"""


def ask_srvname(srvname=None):
    if srvname is None:
        while not srvname:
            srvname = raw_input('Service name? ')
    return srvname
