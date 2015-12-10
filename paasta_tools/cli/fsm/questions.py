# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This is a home for functions that calculate arguments based on various user
inputs.
"""


import sys

import yaml

from paasta_tools.cli.fsm.prompt import ask
from paasta_tools.cli.fsm.autosuggest import suggest_smartstack_proxy_port
from paasta_tools.cli.utils import list_teams
from paasta_tools.utils import DEPLOY_PIPELINE_NON_DEPLOY_STEPS


def _yamlize(contents):
    return yaml.dump(contents, explicit_start=True, default_flow_style=False)


def get_srvname(srvname, auto):
    if srvname is None:
        if auto:
            sys.exit("I'd Really Rather You Didn't Use --auto Without --service-name")
        while not srvname:
            srvname = ask('Service name?')
    return srvname


def get_description(description, auto):
    if description is None:
        if auto:
            description = "Please fill in a one-line description of this service"
        while not description:
            description = ask('One line description of this service?')
    return description


def get_external_link(link, auto):
    if link is None:
        if auto:
            link = "Please add a link to a reference doc for your service"
        while not link:
            link = ask('Link to a reference doc?')
    return link


def get_smartstack_stanza(yelpsoa_config_root, auto, port):
    """Produce a basic smartstack.yaml in a `format <yelpsoa_configs.html#smartstack-yaml>`_
    PaaSTA can use.
    """
    if port is None:
        suggested_port = suggest_smartstack_proxy_port(yelpsoa_config_root)
        if auto:
            port = suggested_port
        else:
            port = ask("Smartstack proxy_port?", suggested_port)
    smartstack_stanza = {}
    key_name = "main"
    smartstack_stanza[key_name] = {
        "advertise": ["superregion"],
        "discover": "superregion",
        "proxy_port": int(port),
        # Make the service available across all of testopia.
        # See SRV-1715 for more background.
        "extra_advertise": {
            "ecosystem:testopia": ["ecosystem:testopia"]
        }
    }
    return smartstack_stanza


def get_service_stanza(description, external_link, auto):
    stanza = {}
    stanza["description"] = get_description(description, auto)
    stanza["external_link"] = get_external_link(external_link, auto)
    return stanza


def get_monitoring_stanza(auto, team):
    """Produce a monitoring.yaml in a `format <yelpsoa_configs.html#monitoring-yaml>`_
    that PaaSTA can read.

    'team' is the critical key and is not calculable so it is required.
    """
    all_teams = list_teams()
    if team is None:
        if auto:
            sys.exit("I'd Really Rather You Didn't Use --auto Without --team")
        while not team:
            print "Here are the existing teams:"
            print ", ".join(sorted(all_teams))
            team = ask("Team responsible for this service?")
    if not all_teams:
        sys.stderr.write("Warning: No sensu teams are defined on disk, cannot perform validation on this team name")
    elif team not in all_teams:
        print "I Don't See Your Team '%s' In The List Of Valid Teams:" % team
        sys.exit(", ".join(sorted(all_teams)))

    stanza = {}
    stanza["team"] = team
    stanza["service_type"] = "marathon"
    return stanza


def get_deploy_stanza():
    """Produce a deploy.yaml a la http://y/cep319"""
    stanza = {}
    stanza["pipeline"] = [
        {"instancename": "itest", },
        {"instancename": "security-check", },
        {"instancename": "push-to-registry", },
        {"instancename": "performance-check", },
        {"instancename": "pnw-stagea.canary", },
        {"instancename": "pnw-stagea.main", },
        {"instancename": "norcal-stageb.canary", },
        {"instancename": "norcal-stageb.main", },
        {"instancename": "norcal-devb.canary", },
        {"instancename": "norcal-devb.main", },
        {"instancename": "norcal-devc.canary", },
        {"instancename": "norcal-devc.main", "trigger_next_step_manually": True, },
        {"instancename": "norcal-prod.canary", },
        {"instancename": "nova-prod.canary", "trigger_next_step_manually": True, },
        {"instancename": "norcal-prod.main", },
        {"instancename": "nova-prod.main", },
    ]
    return stanza


def get_clusternames_from_deploy_stanza(deploy_stanza):
    """Given a dictionary deploy_stanza, as from get_deploy_stanza(), return a
    set of the clusternames referenced in the pipeline.
    """
    clusternames = set()
    for entry in deploy_stanza.get("pipeline", []):
        instancename = entry["instancename"]
        if instancename in DEPLOY_PIPELINE_NON_DEPLOY_STEPS:
            continue
        # Usually clustername will be instancename.namespace, so lop off
        # namespace. (If there's no namespace, this just returns clustername,
        # which is correct.)
        clustername = instancename.split(".")[0]
        clusternames.add(clustername)
    return clusternames


def get_marathon_stanza():
    """Produce a ``marathon-*.yaml`` a la
    `the docs <yelpsoa_configs.html#marathon-clustername-yaml>`_

    We want to default to The Simplest Thing That Can Possibly Work. This
    allows new services to hit the ground running, but forces developers to
    think about their resource needs and tune as they move toward production.
    So:

    - .1 cpus
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
        "cpus": .1,
        "mem": 500,
        "instances": 3,
    }
    stanza["canary"] = {
        "cpus": .1,
        "mem": 500,
        "nerve_ns": "main",
        "instances": 1,
    }
    return stanza
