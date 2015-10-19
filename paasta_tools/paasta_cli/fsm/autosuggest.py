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

import operator
import os
import os.path

import yaml

from paasta_tools.paasta_cli.fsm import config
from paasta_tools.paasta_cli.fsm import service_configuration


def _get_port_from_file(root, file):
    """Given a root and file (as from os.walk), attempt to return a port
    number (int) from that file. Returns 0 if file is empty."""
    with open(os.path.join(root, file)) as f:
        port = f.read().strip()
        port = int(port) if port else 0
    return port


def suggest_port():
    """Pick the next highest port from the 13000-14000 block"""
    max_port = 0
    for root, dirs, files in os.walk(config.YELPSOA_CONFIG_ROOT):
        for f in files:
            if f.endswith("port"):
                port = _get_port_from_file(root, f)
                if not 14000 > port > 13000:
                    port = 0
                max_port = max(port, max_port)
    return max_port + 1


def _get_smartstack_proxy_port_from_file(root, file):
    """Given a root and file (as from os.walk), attempt to return the highest
    smartstack proxy port number (int) from that file. Returns 0 if there is no
    smartstack proxy_port.
    """
    port = 0
    with open(os.path.join(root, file)) as f:
        data = yaml.load(f)

    if file.endswith('service.yaml') and 'smartstack' in data:
        # Specifying this in service.yaml is old and deprecated and doesn't
        # support multiple namespaces.
        port = data['smartstack'].get('proxy_port', 0)
    elif file.endswith('smartstack.yaml'):
        for namespace in data.keys():
            port = max(port, data[namespace].get('proxy_port', 0))

    return int(port)


def suggest_smartstack_proxy_port(yelpsoa_config_root):
    """Pick the next highest smartstack proxy port from the 20000-21000 block"""
    max_proxy_port = 0
    for root, dirs, files in os.walk(yelpsoa_config_root):
        for f in files:
            if f.endswith('service.yaml') or f.endswith('smartstack.yaml'):
                proxy_port = _get_smartstack_proxy_port_from_file(root, f)
                if not 20000 < proxy_port < 21000:
                    proxy_port = 0
                max_proxy_port = max(proxy_port, max_proxy_port)
    return max_proxy_port + 1


def is_stage_habitat(habitat):
    return habitat.startswith("stage")


PROD_HABITATS = (
    "sfo1",
    "iad1",
    "sfo2",
)


def is_prod_habitat(habitat):
    return habitat in PROD_HABITATS


def is_dev_habitat(habitat):
    return habitat.startswith("dev")


def discover_habitats(collated_service_yamls):
    """Given a dictionary as returned by collate_service_yamls(), return a list
    of default habitats from the ones in use.
    """
    habitats = []

    # stage
    stages = [habitat for habitat in collated_service_yamls.keys() if is_stage_habitat(habitat)]
    habitats.extend(stages)

    # prod
    habitats.extend(PROD_HABITATS)

    # dev
    devs = [habitat for habitat in collated_service_yamls.keys() if is_dev_habitat(habitat)]
    habitats.extend(devs)

    return habitats


def is_srv_machine(host, habitat):
    """Returns True if 'host' is eligible to run services in 'habitat'. If
    'habitat' is not known, return True (i.e. any machine is valid by default).
    """
    if is_stage_habitat(habitat):
        if host.startswith("%sservices" % habitat):
            return True
        return False
    elif is_prod_habitat(habitat):
        if host.startswith("srv"):
            return True
        return False
    elif is_dev_habitat(habitat):
        if host.startswith("srv") or host.startswith("devservices"):
            return True
        return False
    else:
        return True


def is_general_prod_srv_machine(host):
    """Returns True if 'host' is considered a general production srv machine that
    is eligible to run general service code.
    """
    if host.startswith('srv'):
        if any((
            'batch' in host,
            'pcde' in host,
            'uswest' in host,
        )):
            return False
        else:
            return True
    return False


def get_prod_srv_hosts(host_histogram):
    hosts = [host for host in host_histogram.keys() if is_general_prod_srv_machine(host)]
    return ",".join(hosts)


def get_least_used_host(host_histogram, habitat=None):
    """'habitat' is used to determine an eligible host, e.g. service machines
    in stage are called stageXservicesN. We need this so we don't suggest
    running on a random search or dsu box.
    """
    eligible_host_histogram = dict(
        (host, count) for host, count in host_histogram.iteritems() if is_srv_machine(host, habitat)
    )
    least_used_host = min(eligible_host_histogram.items(), key=operator.itemgetter(1))
    return least_used_host[0]


def suggest_hosts_for_habitat(collated_service_yamls, habitat):
    host_histogram = collated_service_yamls.get(habitat)
    if host_histogram is None:
        print "WARNING: Habitat %s not in collated_service_yamls. Typo?" % habitat
        print "Not suggesting hosts for this habitat."
        return ""

    if is_prod_habitat(habitat):
        return get_prod_srv_hosts(host_histogram)
    else:
        return get_least_used_host(host_histogram, habitat)


def suggest_all_hosts(collated_service_yamls):
    suggested_hosts = []
    for habitat in discover_habitats(collated_service_yamls):
        suggested_hosts.append(suggest_hosts_for_habitat(collated_service_yamls, habitat))
    return ",".join(suggested_hosts)


def suggest_runs_on(runs_on=None):
    """Suggest a set of machines for the service to run on.

    'runs_on' is any existing --runs-on value provided by the user. This could
    be a comma-separated list of ready-to-go hostnames, an all-caps HABITAT
    to transform into appropriate defaults for that habitat, or the string
    'AUTO' to transform into appropriate defaults for the default set of
    habitats.

    While doing all of that, try not to go read a bunch of yaml off disk if we
    don't have to. We don't want the dependencies or the overhead (user warned
    about inocmpatible options even though we don't get that far; loading
    hundreds of yaml files just to throw them away because the user provided a
    list of hosts).

    Returns the (possibly munged) 'runs_on' as a string of comma-separated
    hostnames.
    """
    if runs_on is None:
        runs_on = "AUTO"

    collated_service_yamls = None

    def _get(collated_service_yamls):
        """A silly method to implement a memoized singleton."""
        if collated_service_yamls is None:
            all_service_yamls = service_configuration.load_service_yamls()
            return service_configuration.collate_service_yamls(all_service_yamls)
        return collated_service_yamls

    munged_runs_on = []
    for thing in runs_on.split(","):
        if thing == "AUTO":
            collated_service_yamls = _get(collated_service_yamls)
            munged_runs_on.append(suggest_all_hosts(collated_service_yamls))
        elif thing == thing.upper():
            collated_service_yamls = _get(collated_service_yamls)
            munged_runs_on.append(suggest_hosts_for_habitat(collated_service_yamls, thing.lower()))
        else:
            munged_runs_on.append(thing)

    return ",".join(munged_runs_on)


# vim: expandtab tabstop=4 sts=4 shiftwidth=4:
