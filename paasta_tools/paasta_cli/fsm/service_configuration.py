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

"""
Tools for interacting with the data model represented by
service_configuration_lib and ``/nail/etc/services/*/service.yaml``.
"""

import os.path
import re

import service_configuration_lib

from paasta_tools.paasta_cli.fsm import config


PROD_RE = re.compile(r"-(sfo\d|iad\d)$")
# Capture names like stagea and stagez but not stagespam, which is different.
STAGE_RE = re.compile(r"^(stage[a-z])(?!pam)")
DEV_RE = re.compile(r"-(dev[a-z])$")


def get_habitat_from_fqdn(fqdn):
    """Tries to calculate a habitat given a fully qualified domain name.
    Returns None and prints a warning if it can't guess a habitat.
    """
    if fqdn is None:
        return None
    try:
        (hostname, subdomain, _yelpcorp, _com) = fqdn.split(".")
    except (AttributeError, ValueError):
        print "WARNING: %s doesn't appear to be a well-formed fqdn (host.subdomain.yelpcorp.com)." % fqdn
        return None

    # Handle any special cases up front to avoid early exit due to a false
    # positive match (e.g. snowflake-devc.dev.yelpcorp.com is in the
    # 'snowflake' habitat by special exception, not the 'devc' habitat as its
    # name would otherwise imply.
    if fqdn == "relengsrv1-sjc.dev.yelpcorp.com":
        return "testopia"

    m = PROD_RE.search(hostname)
    if m:
        return m.group(1)

    # Some sfo1 hostnames are not compliant with the standard naming convention
    # If we come accross such a host, guess that it is in sfo1
    if subdomain == "365" or subdomain == "prod":
        return "sfo1"

    m = STAGE_RE.search(hostname)
    if m:
        return m.group(1)

    m = DEV_RE.search(hostname)
    if m:
        return m.group(1)
    if hostname.endswith("sv") and subdomain == "sldev":
        return subdomain

    if hostname.endswith("sw") and subdomain == "slwdc":
        return subdomain

    if hostname.endswith("sj") and subdomain == "sjc":
        return subdomain

    print "WARNING: Could not find habitat for fqdn %s" % fqdn
    return None


def load_service_yamls():
    """Walks config.YELPSOA_CONFIG_ROOT looking for service.yaml files. Returns
    a list of dicts representing the contents of those files.
    """
    if not config.YELPSOA_CONFIG_ROOT:
        print "INFO: Can't suggest runs_on because --yelpsoa-config-root is not set."
        return []

    return _load_service_yamls_from_disk()


def _load_service_yamls_from_disk():
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
    running on this host) for values. Example::

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
