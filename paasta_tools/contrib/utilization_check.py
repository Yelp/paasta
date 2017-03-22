#!/usr/bin/env python
"""Reads a list of hosts to stdin and produces
a utilization report for those hosts.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import sys

from paasta_tools.mesos.exceptions import MasterNotAvailableException
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.metrics.metastatus_lib import calculate_resource_utilization_for_slaves
from paasta_tools.metrics.metastatus_lib import filter_tasks_for_slaves
from paasta_tools.metrics.metastatus_lib import get_all_tasks_from_state
from paasta_tools.metrics.metastatus_lib import resource_utillizations_from_resource_info
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors


def main(hostnames):
    master = get_mesos_master()
    try:
        mesos_state = master.state
    except MasterNotAvailableException as e:
        paasta_print(PaastaColors.red("CRITICAL:  %s" % e.message))
        sys.exit(2)
    slaves = [slave for slave in mesos_state.get('slaves', []) if slave['hostname'] in hostnames]
    tasks = get_all_tasks_from_state(mesos_state, include_orphans=True)
    filtered_tasks = filter_tasks_for_slaves(slaves, tasks)
    resource_info_dict = calculate_resource_utilization_for_slaves(slaves, filtered_tasks)
    resource_utilizations = resource_utillizations_from_resource_info(
        total=resource_info_dict['total'],
        free=resource_info_dict['free'],
    )
    output = {}
    for metric in resource_utilizations:
        utilization = metric.total - metric.free
        if int(metric.total) == 0:
            utilization_perc = 100
        else:
            utilization_perc = utilization / float(metric.total) * 100
        output[metric.metric] = {
            'total': metric.total,
            'used': utilization,
            'perc': utilization_perc,
        }
    print(json.dumps(output))


if __name__ == "__main__":
    hostnames = reduce(lambda x, y: x + [y.strip()], sys.stdin, [])
    main(hostnames)
