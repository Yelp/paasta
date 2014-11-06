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


def get_smartstack_stanza(yelpsoa_config_root, auto, port):
    if port is None:
        suggested_port = suggest_smartstack_proxy_port(yelpsoa_config_root)
        if auto:
            port = suggested_port
        else:
            port = ask("Smartstack proxy_port?", suggested_port)
    smartstack_stanza = {}
    key_name = "main"
    smartstack_stanza[key_name] = {
        "proxy_port": int(port),
        # Default routes for AWSPROD-42
        # To be removed once AWSPROD has its own srv boxes
        "routes": [
            {'source': 'uswest1aprod',
             'destinations': ['sfo1', 'sfo2']},
            {'source': 'uswest1bprod',
             'destinations': ['sfo1', 'sfo2']},
        ]

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


def get_monitoring_stanza(auto, team, legacy_style=False):
    """Produce a monitoring.yaml a la
    https://trac.yelpcorp.com/wiki/HowToService/Monitoring/monitoring.yaml

    'team' is the critical key and is not calculable so it is required.

    legacy_style changes some behavior for use with wizard.py.
    """
    if team is None:
        if auto and not legacy_style:
            sys.exit("I'd Really Rather You Didn't Use --auto Without --team")
        while not team:
            team = ask("Team responsible for this service?")
    stanza = {}
    stanza["team"] = team
    stanza["service_type"] = "marathon"
    if legacy_style:
        stanza["service_type"] = "classic"
    return stanza
