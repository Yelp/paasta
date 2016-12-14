#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import sys

from paasta_tools.autoscaling.autoscaling_cluster_lib import get_sfr
from paasta_tools.autoscaling.autoscaling_cluster_lib import get_sfr_slaves
from paasta_tools.autoscaling.autoscaling_cluster_lib import get_spot_fleet_instances
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import load_system_paasta_config


def check_registration(threshold_percentage):
    mesos_state = get_mesos_master().state
    autoscaling_resources = load_system_paasta_config().get_cluster_autoscaling_resources()
    for resource in autoscaling_resources.values():
        if resource['type'] == 'aws_spot_fleet_request':
            resource['sfr'] = get_sfr(resource['id'], region=resource['region'])
            instances = get_spot_fleet_instances(resource['id'], region=resource['region'])
            resource['sfr']['ActiveInstances'] = instances
            slaves = get_sfr_slaves(resource, mesos_state)
            if len(instances) == 0:
                continue
            else:
                percent_registered = float(float(len(slaves)) / float(len(instances))) * 100
                if percent_registered < float(threshold_percentage):
                    print "CRIT: Only found {0}% of instances in {1} registered in mesos. "\
                          "Please check for puppet or AMI baking problems!".format(percent_registered,
                                                                                   resource['id'])
                    return False
    print "OK: Found more than {0}% of instances registered for all paasta resources in this "\
          "superregion".format(threshold_percentage)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--threshold", help="percentage threshold for registered instances",
                        default="75")
    threshold = parser.parse_args().threshold
    if check_registration(threshold):
        sys.exit(0)
    sys.exit(2)


if __name__ == "__main__":
    main()
