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

def suggest_runs_on():
    if not config.PUPPET_ROOT:
        print "INFO: Can't suggest runs_on because --puppet-root is not set."
        return ""

    all_service_yamls = service_configuration.load_service_yamls()
    collated_service_yamls = service_configuration.collate_service_yamls(all_service_yamls)

    ### tmp until this returns something usable by our caller
    from pprint import pprint
    pprint(collated_service_yamls)
    return ""


# vim: expandtab tabstop=4 sts=4 shiftwidth=4:
