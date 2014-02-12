import operator
import os
import os.path

from service_setup import config
from service_setup import paths

def suggest_vip():
    """Suggest the most under-utilized vip"""
    vip_counts = {}
    for root, dirs, files in os.walk(
            os.path.join(config.PUPPET_ROOT, paths.SERVICE_FILES)):
        if 'vip' in files:
            with open(os.path.join(root, 'vip')) as f:
                vip = f.read().strip()
                if vip:
                    vip_counts[vip] = vip_counts.get(vip, 0) + 1
    least_vip = min(vip_counts.items(), key=operator.itemgetter(1))
    return least_vip[0]

def suggest_port():
    """Pick the next highest port from the 13000-14000 block"""
    max_port = 0
    for root, dirs, files in os.walk(
            os.path.join(config.PUPPET_ROOT, paths.SERVICE_FILES)):
        for portfile in ('port','status_port'):
            if portfile in files:
                with open(os.path.join(root, portfile)) as f:
                    port = f.read().strip()
                    port = int(port) if port else 0
                    if not 14000 > port > 13000:
                        port = 0
                    max_port = max(port, max_port)
    return max_port + 1

# vim: expandtab tabstop=4 sts=4 shiftwidth=4:
