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
    """Produce a smartstack.yaml a la http://y/cep319"""
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
            {'source': 'uswest1prod',
             'destinations': ['sfo1', 'sfo2']},
        ]

    }
    return smartstack_stanza


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

def get_deploy_stanza():
    """Produce a deploy.yaml a la http://y/cep319"""
    stanza = {}
    stanza["pipeline"] = [
        { "instancename": "itest", },
        { "instancename": "registry", },
        { "instancename": "pnw-stagea.canary", },
        { "instancename": "pnw-stagea.main", },
        { "instancename": "norcal-stageb.canary", },
        { "instancename": "norcal-stageb.main", },
        { "instancename": "norcal-devb.canary", },
        { "instancename": "norcal-devb.main", },
        { "instancename": "norcal-devc.canary", },
        { "instancename": "norcal-devc.main", "trigger_next_step_manually": True, },
        { "instancename": "norcal-prod.canary", },
        { "instancename": "nova-prod.canary", },
        { "instancename": "pnw-prod.canary", "trigger_next_step_manually": True,},
        { "instancename": "norcal-prod.main", },
        { "instancename": "nova-prod.main", },
        { "instancename": "pnw-prod.main", },
    ]
    return stanza


def get_clusternames_from_deploy_stanza(deploy_stanza):
    """Given a dictionary deploy_stanza, as from get_deploy_stanza(), return a
    set of the clusternames referenced in the pipeline.
    """
    clusternames = set()
    for entry in deploy_stanza.get("pipeline", []):
        instancename = entry["instancename"]
        if instancename in ("itest", "registry"):
            continue
        # Usually clustername will be instancename.namespace, so lop off
        # namespace. (If there's no namespace, this just returns clustername,
        # which is correct.)
        clustername = instancename.split(".")[0]
        clusternames.add(clustername)
    return clusternames


def get_marathon_stanza():
    """Produce a marathon-*.yaml a la
    http://servicedocs.yelpcorp.com/docs/paasta_tools/yelpsoa_configs.html#marathon-clustername-yaml

    We want to default to The Simplest Thing That Can Possibly Work. This
    allows new services to hit the ground running, but forces developers to
    think about their resource needs and tune as they move toward production.
    So:
    - .1 cpu
        - This is a good starting place. Most services use a very small
          percentage of a core.
    - 500MB of memory.
        - Docker processes will *not* be sharing virt across containers.
          I'm pretty sure in practice 500M going to be a sane starting point for
          most services. It is ok to start low and let them go high.
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
        "cpu": .1,
        "mem": 500,
        "instances": 3,
    }
    stanza["canary"] = {
        "cpu": .1,
        "mem": 500,
        "nerve_ns": "main",
        "instances": 1,
    }
    return stanza
