#!/usr/bin/env python3
import yelp_meteorite

from paasta_tools import utils
from paasta_tools.smartstack_tools import retrieve_haproxy_csv


GUAGES = ['ereq', 'rate']
COUNTERS = ['hrsp_2xx', 'hrsp_3xx', 'hrsp_4xx', 'hrsp_5xx']


def parse_haproxy_backend_name(backend):
    # Currently a hack, the backend doesn't always match service.instance,
    # but at least the data will be reported.
    return backend.split('.')[0], backend.split('.')[1]


def report_metric_to_meteorite(backend, metric, value, paasta_cluster):
    try:
        paasta_service, paasta_instance = parse_haproxy_backend_name(backend)
    except IndexError:
        return

    meteorite_dims = {
        'paasta_service': paasta_service,
        'paasta_cluster': paasta_cluster,
        'paasta_instance': paasta_instance,
    }
    path = f'paasta.service.requests.{metric}'
    if metric in GUAGES:
        guage = yelp_meteorite.create_gauge(path, meteorite_dims)
        guage.set(value)
    elif metric in COUNTERS:
        counter = yelp_meteorite.create_counter(path, meteorite_dims)
        counter.count(value)
    else:
        raise ValueError(f"{metric} hasn't been configured as a guage or counter")
    print(f"Sent {path}: {value} to meteorite")


def report_all_metrics_to_meteorite(csv, paasta_cluster):
    for row in csv:
        if row['svname'] == 'BACKEND':
            for metric in GUAGES + COUNTERS:
                report_metric_to_meteorite(
                    backend=row['# pxname'],
                    metric=metric,
                    value=row[metric],
                    paasta_cluster=paasta_cluster,
                )


if __name__ == '__main__':
    system_paasta_config = utils.load_system_paasta_config()
    csv = retrieve_haproxy_csv(
        synapse_host=system_paasta_config.get_default_synapse_host(),
        synapse_port=system_paasta_config.get_synapse_port(),
        synapse_haproxy_url_format=system_paasta_config.get_synapse_haproxy_url_format(),
    )
    report_all_metrics_to_meteorite(
        csv=csv,
        paasta_cluster=system_paasta_config.get_local_run_config().get('default_cluster'),
    )
