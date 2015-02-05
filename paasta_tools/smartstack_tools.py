import csv
import requests

SYNAPSE_HAPROXY_PATH = "http://{0}/;csv;norefresh"


def retrieve_haproxy_csv(synapse_host_port='localhost:3212'):
    """Retrieves the haproxy csv from the haproxy web interface

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns reader: a csv.DictReader object
    """
    synapse_uri = SYNAPSE_HAPROXY_PATH.format(synapse_host_port)

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


def get_backends(service=None, synapse_host_port='localhost:3212'):
    """Fetches the CSV from haproxy and returns a list of backends,
    regardless of their state.

    :param service: If specified, only return backends for this particular
                    service
    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """

    reader = retrieve_haproxy_csv(synapse_host_port)
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
        if (service is None or service == ha_service) and ha_slave not in ('FRONTEND', 'BACKEND'):
            backends.append(line)

    return backends
