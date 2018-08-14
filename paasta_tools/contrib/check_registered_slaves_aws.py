#!/usr/bin/env python
import argparse
import sys

from a_sync import block

from paasta_tools.autoscaling.autoscaling_cluster_lib import get_scaler
from paasta_tools.mesos.exceptions import MasterNotAvailableException
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import load_system_paasta_config


def check_registration(threshold_percentage):
    try:
        mesos_state = block(get_mesos_master().state)
    except MasterNotAvailableException as e:
        print("Could not find Mesos Master: %s" % e.message)
        sys.exit(1)

    config = load_system_paasta_config()
    autoscaling_resources = config.get_cluster_autoscaling_resources()
    for resource in autoscaling_resources.values():
        print("Checking %s" % resource['id'])
        try:
            scaler = get_scaler(resource['type'])(
                resource=resource,
                pool_settings=None,
                config_folder=None,
                dry_run=True,
                utilization_error=0.0,
                max_increase=0.0,
                max_decrease=0.0,
            )
        except KeyError:
            print("Couldn't find a metric provider for resource of type: {}".format(resource['type']))
            continue
        if len(scaler.instances) == 0:
            print("No instances for this resource")
            continue
        elif scaler.is_new_autoscaling_resource():
            # See OPS-13784
            threshold = config.get_monitoring_config().get('check_registered_slave_threshold')
            print(
                f"Autoscaling resource was created within last {threshold}"
                " seconds and would probably fail this check",
            )
            continue
        else:
            slaves = scaler.get_aws_slaves(mesos_state)
            percent_registered = float(float(len(slaves)) / float(len(scaler.instances))) * 100
            if percent_registered < float(threshold_percentage):
                print("CRIT: Only found {}% of instances in {} registered in mesos. "
                      "Please check for puppet or AMI baking problems!".format(
                          percent_registered,
                          resource['id'],
                      ))
                return False
    print("OK: Found more than {}% of instances registered for all paasta resources in this "
          "superregion".format(threshold_percentage))
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--threshold", help="percentage threshold for registered instances",
        default="75",
    )
    threshold = parser.parse_args().threshold
    if check_registration(threshold):
        sys.exit(0)
    sys.exit(2)


if __name__ == "__main__":
    main()
