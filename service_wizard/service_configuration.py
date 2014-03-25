"""
Tools for interacting with the data model represented by
service_configuration_lib and /nail/etc/services/*/service.yaml.
"""

from collections import defaultdict
import os.path
import re
import sys

from service_wizard import config


PROD_RE = re.compile(r"-(sfo\d|iad\d)$")
# Capture names like stagea and stagez but not stagespam, which is different.
STAGE_RE = re.compile(r"^(stage[a-z])(?!pam)")
DEV_RE = re.compile(r"-(dev[a-z])$")
def get_habitat_from_fqdn(fqdn):
    """Tries to calculate a habitat given a fully qualified domain name.
    Returns None and prints a warning if it can't guess a habitat.
    """
    try:
        (hostname, subdomain, _yelpcorp, _com) = fqdn.split(".")
    except (AttributeError, ValueError):
        print "WARNING: %s doesn't appear to be a well-formed fqdn (host.subdomain.yelpcorp.com)." % fqdn
        return None

    m = PROD_RE.search(hostname)
    if m:
        return m.group(1)

    m = STAGE_RE.search(hostname)
    if m:
        return m.group(1)

    m = DEV_RE.search(hostname)
    if m:
        return m.group(1)

    if subdomain == "365":
        return "sfo1"

    if hostname.endswith("sv") and subdomain == "sldev":
        return subdomain

    if hostname.endswith("sw") and subdomain == "slwdc":
        return subdomain

    if hostname.endswith("sj") and subdomain == "sjc":
        return subdomain

    print "WARNING: Could not find habitat for fqdn %s" % fqdn
    return None

def collate_hosts_by_habitat(fqdns):
    """Given a list of fqdns, return a dictionary where the value is the short
    hostname of the fqdn and the key is the habitat calculated from the fqdn.

    If a habitat cannot be calculated for an fqdn, that fqdn is dropped from
    the returned dictionary.
    """
    host_by_habitat = defaultdict(list)
    for fqdn in fqdns:
        # Some service.yamls have a line " - " which gets loaded as [None].
        # We'll just throw these lines out.
        if fqdn is None:
            continue
        host = fqdn.split(".")[0]
        habitat = get_habitat_from_fqdn(fqdn)
        if not habitat:
            print "WARNING: Skipping habitatless host %s" % fqdn
        else:
            host_by_habitat[habitat].append(host)
    return host_by_habitat

def load_service_yamls():
    """Walks config.YELPSOA_CONFIG_ROOT looking for service.yaml files. Returns
    a list of dicts representing the contents of those files.

    Requires service_configuration_lib from config.PUPPET_ROOT. Raises
    ImportError if that doesn't work out. This happens down here because we
    only need this (slightly complicated) import logic if we're asked to
    suggest runs_on.
    """
    if not config.YELPSOA_CONFIG_ROOT:
        print "INFO: Can't suggest runs_on because --yelpsoa-config-root is not set."
        return []

    if not config.PUPPET_ROOT:
        print "INFO: Can't suggest runs_on because --puppet-root is not set."
        return []

    sys.path.append(
        os.path.join(
            config.PUPPET_ROOT, "modules", "deployment", "files",
            "services", "nail", "sys", "srv-deploy", "lib"))
    try:
        import service_configuration_lib
    except ImportError:
        print "ERROR: You asked me to calculate 'runs_on' but I couldn't import"
        print "service_configuration_lib. Bad PUPPET_ROOT %s?" % config.PUPPET_ROOT
        raise

    all_service_yamls = []
    for root, dirs, files in os.walk(config.YELPSOA_CONFIG_ROOT):
        if "service.yaml" in files:
            all_service_yamls.append(
                service_configuration_lib.read_service_information(
                    os.path.join(root, "service.yaml")))
    return all_service_yamls

def collate_service_yamls(all_service_yamls):
    """Given a list containing dictionaries representing the contents of
    service.yaml files, return a dict-of-dicts. The outer dict has habitats for
    keys and dictionaries for values. The inner dict has hostnames for keys and
    the number of time this hostname has been seen (i.e. the number of services
    running on this host) for values. Example:

    {
        'sfo1': {
            'app1.365.yelpcorp.com': 2,
            'app2.365.yelpcorp.com': 2,
        },
        'stageb': {
            'stagebmon1.sjc.yelpcorp.com': 7,
            'stagebmon2.sjc.yelpcorp.com': 3,
            'stagebservices1.sjc.yelpcorp.com': 24,
            'stagebservices2.sjc.yelpcorp.com': 21,
            'stagebservices3.sjc.yelpcorp.com': 7,
        },
    }
    """
    all_hosts_by_habitat = {}
    for service_yaml in all_service_yamls:
        fqdns = service_yaml.get("runs_on", [])
        for fqdn in fqdns:
            habitat = get_habitat_from_fqdn(fqdn)
            if not habitat:
                continue
            previously_seen_hosts = all_hosts_by_habitat.get(habitat, {})
            num_services_previously_assigned = previously_seen_hosts.get(fqdn, 0)
            num_services_previously_assigned += 1
            all_hosts_by_habitat[habitat] = previously_seen_hosts
            all_hosts_by_habitat[habitat][fqdn] = num_services_previously_assigned
    return all_hosts_by_habitat
