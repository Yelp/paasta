"""This is a home for functions that calculate arguments based on various user
inputs.
"""


import sys

import yaml

from service_wizard.prompt import ask
from service_wizard.autosuggest import suggest_smartstack_proxy_port


def _yamlize(contents):
    return yaml.dump(contents, explicit_start=True, default_flow_style=False)


def get_srvname(srvname, auto):
    if srvname is None:
        if auto:
            sys.exit("I'd Really Rather You Didn't Use --auto Without --service-name")
        while not srvname:
            srvname = ask('Service name?')
    return srvname


def get_smartstack_stanza(yelpsoa_config_root, auto, port, legacy_style=False):
    if port is None:
        suggested_port = suggest_smartstack_proxy_port(yelpsoa_config_root)
        if auto:
            port = suggested_port
        else:
            port = ask("Smartstack proxy_port?", suggested_port)
    smartstack_stanza = {}
    key_name = "main"
    if legacy_style:
        key_name = "smartstack"
    smartstack_stanza[key_name] = {
        "proxy_port": int(port),
    }
    return smartstack_stanza


def get_marathon_stanza():
    """We want to default to The Simplest Thing That Can Possibly Work. This
    allows new services to hit the ground running, but forces developers to
    think about their resource needs and tune as they move toward production.
    So:
    - 1 cpu
    - 100MB of memory.
        - kwa: There are plenty of workers on srv1 right now that are >100M
          RES, and there are *no* processes under 100M VSZ, and docker
          processes will not be sharing virt across containers.  I'm pretty
          sure in practice 100M going to be a crippling default for most
          services, but I think it is ok to start low, instead of start high.
    - 3 instances, including 1 canary. Why 2+1 instead of 1+1?
        - Three Of Everything (http://opsprincipl.es/principles/three_of_everything/)
        - If we reboot the dev box running that single main instance, the
          cluster is reduced to just a canary. This also may trigger (useless)
          alerts.
        - krall: In production, there will be multiple main instances and a single
          canary. With 1+1, this is less obvious.
    """
    stanza = {}
    stanza["main"] = {
        "cpu": 1,
        "mem": 100,
        "instances": 3,
    }
    stanza["canary"] = {
        "cpu": 1,
        "mem": 100,
        "nerve_ns": "main",
    }
    return stanza


def get_monitoring_stanza(team):
    stanza = {}
    stanza["team"] = team
    stanza["notification_email"] = "%s@yelp.com" % team
    return stanza
