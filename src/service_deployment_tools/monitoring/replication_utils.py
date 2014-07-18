import csv

import requests


SYNAPSE_HAPROXY_PATH = "http://{0}/;csv;norefresh"


def get_replication_for_services(synapse_host_port, service_names):
    """Returns the replication level for the provided services

    This check is intended to be used with an haproxy load balancer, and
    relies on the implementation details of that choice.

    :param synapse_host_port: A string in host:port format that this check
                              should contact for replication information.
    :param service_names: A list of strings that are the service names
                          that should be checked for replication.

    :returns available_instances: A dictionary mapping the service names
                                  to an integer number of available
                                  replicas
    """
    synapse_uri = SYNAPSE_HAPROXY_PATH.format(synapse_host_port)

    # timeout after 1 second
    haproxy_response = requests.get(synapse_uri, timeout=1)
    haproxy_data = haproxy_response.text
    reader = csv.DictReader(haproxy_data.splitlines())

    available_instances = dict([(service_name, 0) for
                                service_name in service_names])
    for line in reader:
        # clean up two irregularities of the CSV output, relative to
        # DictReader's behavior there's a leading "# " for no good reason:
        line['pxname'] = line.pop('# pxname')
        # and there's a trailing comma on every line:
        line.pop('')

        # Look for the service in question and ignore
        # the fictional FRONTEND/BACKEND hosts:
        slave, service = line['svname'], line['pxname']
        if (service in service_names and
                slave not in ('FRONTEND', 'BACKEND') and
                line['status'] == 'UP'):
            available_instances[service] += 1

    return available_instances
