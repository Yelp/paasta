# Copyright 2015-2016 Yelp Inc.
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
import csv

import requests


def retrieve_haproxy_csv(synapse_host, synapse_port, synapse_haproxy_url_format):
    """Retrieves the haproxy csv from the haproxy web interface

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns reader: a csv.DictReader object
    """
    synapse_uri = synapse_haproxy_url_format.format(host=synapse_host, port=synapse_port)

    # timeout after 1 second and retry 3 times
    haproxy_request = requests.Session()
    haproxy_request.mount(
        'http://',
        requests.adapters.HTTPAdapter(max_retries=3))
    haproxy_request.mount(
        'https://',
        requests.adapters.HTTPAdapter(max_retries=3))
    haproxy_response = haproxy_request.get(synapse_uri, timeout=1)
    haproxy_data = haproxy_response.text
    reader = csv.DictReader(haproxy_data.splitlines())
    return reader


def get_backends(service, synapse_host, synapse_port, synapse_haproxy_url_format):
    """Fetches the CSV from haproxy and returns a list of backends,
    regardless of their state.

    :param service: If None, return backends for all services, otherwise only return backends for this particular
                    service.
    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """
    if service:
        services = [service]
    else:
        services = None
    return get_multiple_backends(services, synapse_host=synapse_host, synapse_port=synapse_port,
                                 synapse_haproxy_url_format=synapse_haproxy_url_format)


def get_multiple_backends(services, synapse_host, synapse_port, synapse_haproxy_url_format):
    """Fetches the CSV from haproxy and returns a list of backends,
    regardless of their state.

    :param services: If None, return backends for all services, otherwise only return backends for these particular
                     services.
    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """

    reader = retrieve_haproxy_csv(synapse_host, synapse_port, synapse_haproxy_url_format=synapse_haproxy_url_format)
    backends = []

    for line in reader:
        # clean up two irregularities of the CSV output, relative to
        # DictReader's behavior there's a leading "# " for no good reason:
        line['pxname'] = line.pop('# pxname')
        # and there's a trailing comma on every line:
        line.pop('')

        # Look for the service in question and ignore the fictional
        # FRONTEND/BACKEND hosts, use starts_with so that hosts that are UP
        # with 1/X healthchecks to go before going down get counted as UP:
        ha_slave, ha_service = line['svname'], line['pxname']
        if (services is None or ha_service in services) and ha_slave not in ('FRONTEND', 'BACKEND'):
            backends.append(line)

    return backends
