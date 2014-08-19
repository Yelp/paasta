"""This is a home for functions that calculate arguments based on various user
inputs.
"""


import sys

from service_wizard.prompt import ask
from service_wizard.autosuggest import suggest_smartstack_proxy_port


def get_srvname(srvname, auto):
    if srvname is None:
        if auto:
            sys.exit("I'd Really Rather You Didn't Use --auto Without --service-name")
        while not srvname:
            srvname = ask('Service name?')
    return srvname


def get_smartstack_yaml(yelpsoa_config_root, auto, port):
    if port is None:
        suggested_port = suggest_smartstack_proxy_port(yelpsoa_config_root)
        if auto:
            port = suggested_port
        else:
            port = ask("Smartstack proxy_port?", suggested_port)
    smartstack_yaml = { 'proxy_port': port }
    return smartstack_yaml
