import operator
import os
import os.path

from service_wizard import config
from service_wizard import service_configuration


class NoVipError(Exception):
    pass


def suggest_vip():
    """Suggest the most under-utilized vip"""
    vip_counts = {}
    for root, dirs, files in os.walk(config.YELPSOA_CONFIG_ROOT):
        if 'vip' in files:
            with open(os.path.join(root, 'vip')) as f:
                vip = f.read().strip()
                if vip:
                    vip_counts[vip] = vip_counts.get(vip, 0) + 1
    if not vip_counts:
        raise NoVipError("Could not find any vips. Bad YELPSOA_CONFIG_ROOT %s?" % (config.YELPSOA_CONFIG_ROOT))
    least_vip = min(vip_counts.items(), key=operator.itemgetter(1))
    return least_vip[0]

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

def is_stage_habitat(habitat):
    return habitat.startswith("stage")

def is_dev_habitat(habitat):
    return habitat.startswith("dev")

def discover_habitats(collated_service_yamls):
    """Given a dictionary as returned by collate_service_yamls(), return a list
    of default habitats (in ALL CAPS as if passed in via command line) in which
    hosts should be assigned by default.
    """
    habitats = []

    # stage
    stages = [habitat.upper() for habitat in collated_service_yamls.keys() if is_stage_habitat(habitat)]
    habitats.extend(stages)

    # prod
    habitats.append("SFO1")
    habitats.append("SFO2")
    habitats.append("IAD1")

    # dev
    devs = [habitat.upper() for habitat in collated_service_yamls.keys() if is_dev_habitat(habitat)]
    habitats.extend(devs)

    return habitats

def suggest_hosts_for_habitat(collated_service_yamls, habitat):
    return "srv.%s" % habitat

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
    about --puppet-root even though it isn't actually needed; loading hundreds
    of yaml files just to throw them away because the user provided a list of
    hosts).

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
            munged_runs_on.append(suggest_hosts_for_habitat(collated_service_yamls, thing))
        else:
            munged_runs_on.append(thing)

    return ",".join(munged_runs_on)


# vim: expandtab tabstop=4 sts=4 shiftwidth=4:
