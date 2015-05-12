# A Diamond collector that scrapes the /status/metrics endpoint of all services
# that run on localhost. If the service is a java service, the structure of
# the json returned by /status/metrics is expected to conform to the java
# dropwizard format. Otherwise, no special effort is made to extract metadata
# to enrich the metric information (translation: pyramid uwsgi metrics as emitted
# as key/value pairs of the leaf nodes with the metric_type set to GAUGE).
import json
import os
import sys
import urllib2

import diamond.collector

# TODO: This is a hack to let this module be imported by diamond
# see PAASTA-691 for details
directory_path = os.path.dirname(os.path.realpath(__file__))
package_path = os.path.abspath(os.path.join(directory_path, '../../'))
sys.path.insert(0, package_path)

from paasta_tools import marathon_tools
import service_configuration_lib


METRIC_TYPE_COUNTER = 'COUNTER'
METRIC_TYPE_GAUGE = 'GAUGE'


def sanitize(metric_segment):
    """
    Graphite metric segments cannot contain a dot or a space for obvious reasons.

    :param metric_segment: individual segment that makes up a graphite metric name.
        e.g. the 'foo' in server.foo.load.short is a metric_segment.
    :return: metric_segment with verboten chars replaced with an underscore.
    """
    return metric_segment.replace('.', '_').replace(' ', '_')


def add_metric(bucket, metric_segments, metric_value, metric_type=None):
    """
    Helper to collect metrics into a single bucket. Right now the bucket
    is a list of tuples but that will probably change. This is to insulate
    the callers from such a change.

    :param bucket: list
    :param metric_segments: list of str
    :param metric_value: raw metric value e.g. 10, 0.5, 99999
    :param metric_type: GAUGE or COUNTER
    """
    bucket.append((metric_segments, metric_value, metric_type))


def collect_gauge(metric_segments, node, bucket):
    """
    Matches metrics in json structure that look like:

    "percent-idle": {
      "value": 0.985,
      "type": "gauge"
    },

    :param metric_segments: list of segments that make up the metric name
    :param node: dict representing node in json structure to collect metrics from
    :param bucket: bucket of collected metrics
    :return: True if metrics collected, False otherwise
    """
    if node.get('type') in ('gauge',) and 'value' in node:
        add_metric(bucket, metric_segments, node['value'], METRIC_TYPE_GAUGE)
        return True
    return False


def collect_histogram(metric_segments, node, bucket):
    """
    Matches metrics in json structure that look like:

    "prefix-length": {
        "type": "histogram",
        "count": 1,
        "min": 2,
        "max": 2,
        "mean": 2,
        "std_dev": 0,
        "median": 2,
        "p75": 2,
        "p95": 2,
        "p98": 2,
        "p99": 2,
        "p999": 2
    },

    :param metric_segments: list of segments that make up the metric name
    :param node: dict representing node in json structure to collect metrics from
    :param bucket: bucket of collected metrics
    :return: True if metrics collected, False otherwise
    """
    if node.get('type') in ('histogram',) and 'count' in node:
        for key, value in node.items():
            if key in ('type',):
                continue
            metric_type = METRIC_TYPE_COUNTER if key in ('count',) else METRIC_TYPE_GAUGE
            add_metric(bucket, metric_segments + [sanitize(key)], value, metric_type)
        return True
    return False


def collect_counter(metric_segments, node, bucket):
    """
    Matches metrics in json structure that look like:

    "active-suspended-requests": {
       "count": 0,
       "type": "counter"
    }

    :param metric_segments: list of segments that make up the metric name
    :param node: dict representing node in json structure to collect metrics from
    :param bucket: bucket of collected metrics
    :return: True if metrics collected, False otherwise
    """
    if node.get('type') in ('counter',) and 'count' in node:
        add_metric(bucket, metric_segments, node['count'], METRIC_TYPE_COUNTER)
        return True
    return False


def collect_rate(metric_segments, node, bucket):
    """
    Matches metrics in json structure that look like:

    "rate": {
      "m15": 0,
      "m5": 0,
      "m1": 0,
      "mean": 0,
      "count": 0,
      "unit": "seconds"
    },
    :param metric_segments: list of segments that make up the metric name
    :param node: dict representing node in json structure to collect metrics from
    :param bucket: bucket of collected metrics
    :return: True if metrics collected, False otherwise
    """
    if node.get('unit') in ('seconds', 'milliseconds',):
        for key, value in node.items():
            if key == 'unit':
                continue
            metric_type = METRIC_TYPE_COUNTER if key == 'count' else METRIC_TYPE_GAUGE
            add_metric(bucket, metric_segments + [sanitize(key)], value, metric_type)
        return True
    return False


def collect_meter(metric_segments, node, bucket):
    """
    Collects metrics from a json structure that looks like:

    "suspends": {
      "m15": 0,
      "m5": 0,
      "m1": 0,
      "mean": 0,
      "count": 0,
      "unit": "seconds",
      "event_type": "requests",
      "type": "meter"
    },

    :param metric_segments: list of segments that make up the metric name
    :param node: dict representing node in json structure to collect metrics from
    :param bucket: bucket of collected metrics
    :return: True if metrics collected, False otherwise
    """
    # Don't impose any restrictions on the value of 'event_type'
    if node.get('type') in ('meter',) and \
            'event_type' in node and \
            node.get('unit') in ('seconds', 'milliseconds', 'minutes',):

        for key, value in node.items():
            if key in ('unit', 'event_type', 'type'):
                continue
            metric_type = METRIC_TYPE_COUNTER if key == 'count' else METRIC_TYPE_GAUGE
            add_metric(bucket, metric_segments + [sanitize(key)], value, metric_type)
        return True
    return False


def drop_jvm_node(metric_segments, node, bucket):
    """
    Identifies the 'jvm' node in a java metrics json structure and
    explicitly ignores it.

    "vm": {
      "version": "1.7.0_15-b03",
      "name": "Java HotSpot(TM) 64-Bit Server VM"
    },
    """
    if 'version' in node and 'name' in node and metric_segments and metric_segments[-1] in ('vm',):
        return True
    return False


def drop_timer_entry(metric_segments, node, bucket):
    """
    Identifies the 'timer' entry in the java metrics json structure
    and explicitly ignores it.

    "org.eclipse.jetty.servlet.ServletContextHandler": {
      "trace-requests": {
        "rate": {
          "m15": 0,
          "m5": 0,
          "m1": 0,
          "mean": 0,
          "count": 0,
          "unit": "seconds"
        },
        "duration": {
          "p999": 0,
          "p99": 0,
          "p98": 0,
          "unit": "milliseconds",
          "min": 0,
          "max": 0,
          "mean": 0,
          "std_dev": 0,
          "median": 0,
          "p75": 0,
          "p95": 0
        },
        "type": "timer"    <-- useless, so drop
      },
    """
    if node.get('type') in ('timer',):
        for key, value in node.items():
            if key != 'type':
                if not isinstance(value, dict):
                    return False
        # drop entry iff other peers are dicts
        del node['type']

    # allow subnodes to be processed
    return False


# Collectors identify and extract metrics from the json structure.
# Make sure to order from specific -> general.
collectors = [
    collect_meter,
    drop_timer_entry,
    drop_jvm_node,
    collect_histogram,
    collect_gauge,
    collect_counter,
    collect_rate,
]


def is_number(s):
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


def json_to_metrics(json_dict):
    """
    Ingest json dict representing metrics and turn into diamond metrics that can be emitted

    :param json_dict: dict of metrics from an endpoint such as /status/metrics
    :returns: list of diamond metrics
    :rtype: list of tuple(metric_segments, value, metric_type)
    """
    # Work queue of tuple(metric_segments, dict representing json node)
    stack = [([], json_dict)]

    # Collected metrics
    bucket = []

    while stack:
        metric_segments, node = stack.pop()

        for collect in collectors:
            # These collectors are specifically for java dropwizard metrics
            if collect(metric_segments, node, bucket):
                break
        else:
            # no collectors matched - probably python uwsgi metrics
            for key, value in node.items():
                if isinstance(value, dict):
                    # sub-nodes are queued up to be processed
                    stack.append((metric_segments + [sanitize(key)], value))
                elif is_number(value):
                    # treat everything that is not a sub-node as a metric datapoint
                    add_metric(bucket, metric_segments + [sanitize(key)], value, metric_type=None)

    return bucket


def get_json_metrics(url, log, service_name):
    """
    Read json response from the given url and return as a dict.
    Any deviation from a successful retrieval and parsing will log the error and return an empty dict.

    :param url: url to retrieve metrics from
    :param log: diamond's logger
    :param service_name: used for context in logging errors
    :returns: json response as a dict or an empty dict if any errors occur.
    """
    result = {}
    try:
        json_response = urllib2.urlopen(url).read()
    except Exception, e:
        # Not all services publish metrics. Warn for the time being..
        log.warn('Failed to retrieve %s metrics from %s: %s' % (service_name, url, str(e)))
    else:
        result = json.loads(json_response)
    return result


class SOACollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(SOACollector, self).__init__(*args, **kwargs)
        # We don't want to cache YAML beacuse diamond is a long running process.
        # In doing so, we'd never pick up changes to the yaml files on disk.
        service_configuration_lib.disable_yaml_cache()

    def get_default_config(self):
        """
        :return: Default collector settings as a dict
        """
        config = super(SOACollector, self).get_default_config()
        config.update({})
        return config

    def collect(self):
        services = marathon_tools.get_services_running_here_for_nerve()

        for service_name, service_data in services:
            port = service_data.get('port')
            if not port:
                continue

            if service_name.endswith('.main'):
                # strip off .main for backwards compatibility with old metrics
                service_name = service_name.partition('.')[0]

            url = 'http://localhost:%s/status/metrics' % port
            json_response = get_json_metrics(url, self.log, service_name)
            json_metrics = json_to_metrics(json_response)

            for metric_segments, metric_value, metric_type in json_metrics:
                if metric_type is None:
                    metric_type = METRIC_TYPE_GAUGE
                metric_name = '.'.join([sanitize(service_name)] + metric_segments)
                try:
                    self.publish(
                        metric_name,
                        metric_value,
                        raw_value=metric_value,
                        precision=4,
                        metric_type=metric_type
                    )
                except Exception, e:
                    # Don't let one bad metric stop us from publishing the good ones
                    self.log.error('[%s] Error publishing metric %s/%s/%s from %s: %s' %
                                   (service_name, metric_name, metric_value, metric_type, url, str(e)))
