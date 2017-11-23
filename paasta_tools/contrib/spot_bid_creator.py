#!/usr/bin/env python
import argparse
import csv
import json


def parse_ecu_per_vcpu(s):
    s = s.strip(' units')
    try:
        s = float(s)
    except ValueError:
        s = 0
    return s


def parse_instance_storage(s):
    s = s.split(' ')[0]
    try:
        s = int(s)
    except ValueError:
        s = 0
    return s


def parse_vcpus(s):
    s = s.strip(' vCPUs')
    try:
        s = int(s)
    except ValueError:
        s = 0
    return s


def parse_cost(s):
    s = s.strip('$').strip(' hourly')
    try:
        s = float(s)
    except ValueError:
        s = 0
    return s


parsers = {
    'Memory': lambda s: float(s.strip(' GiB')),
    'vCPUs': parse_vcpus,
    'ECU per vCPU': parse_ecu_per_vcpu,
    'Instance Storage': parse_instance_storage,
}

for h in [
    'Linux On Demand cost', 'Linux Reserved cost', 'RHEL On Demand cost', 'RHEL Reserved cost',
    'SLES On Demand cost', 'SLES Reserved cost', 'Windows On Demand cost', 'Windows Reserved cost',
    'Windows SQL Web On Demand cost', 'Windows SQL Web Reserved cost', 'Windows SQL Std On Demand cost',
    'Windows SQL Std Reserved cost', 'Windows SQL Ent On Demand cost', 'Windows SQL Ent Reserved cost',
]:
    parsers[h] = parse_cost


def parse_headers_and_lines(headers, lines):
    instances = []
    for line in lines:
        d = {}
        zipped = zip(headers, line)
        for header, value in zipped:
            if header in parsers.keys():
                d[header] = parsers[header](value)
            else:
                d[header] = value
        instances.append(d)

    return instances


def check_cpu_cores(args, instance):
    return instance['vCPUs'] >= args.min_cores


def check_mem(args, instance):
    return instance['Memory'] >= args.min_memory


def check_ecu_per_core(args, instance):
    return instance['ECU per vCPU'] >= args.min_ecu_per_core


def check_instance_type(args, instance):
    if args.skip_instance_types is None:
        return True
    skip = args.skip_instance_types.split(",")
    for s in skip:
        if instance['API Name'].startswith(s):
            return False
    return True


instance_checks = [check_cpu_cores, check_mem, check_ecu_per_core, check_instance_type]


def calculate_root_ebs_size(args, instance):
    if instance['Instance Storage'] >= args.min_instance_storage:
        return args.small_ebs
    else:
        return args.big_ebs


def calculate_bid(args, instance):
    on_demand_cost_per_core = instance['Linux On Demand cost'] / instance['vCPUs']
    bid_per_core = on_demand_cost_per_core * 2
    if bid_per_core > args.max_bid_per_core:
        return args.max_bid_per_core * instance['vCPUs']
    else:
        return 2 * on_demand_cost_per_core * instance['vCPUs']


def make_one_instance_data(args, instance):
    data = {}
    data['ebs_size'] = calculate_root_ebs_size(args, instance)
    data['price'] = calculate_bid(args, instance)
    data['type'] = instance['API Name']
    data['weight'] = instance['vCPUs'] / 100.0

    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--instance-data", help="csv file of instance data, as returned by ec2instances.info",
        required=True,
    )
    parser.add_argument(
        "--min-memory", help="minimum memory, in GiB", type=float, required=True,
    )
    parser.add_argument(
        "--min-cores", help="minimum cores aka vCPUs", type=int, required=True,
    )
    parser.add_argument(
        "--min-ecu-per-core", help="minimum ecu per core", type=float, required=True,
    )
    parser.add_argument(
        "--max-bid-per-core", help="maximum bid per core, in $", type=float, required=True,
    )
    parser.add_argument(
        "--min-instance-storage", help="minimum instance storage to use small ebs root, in GiB",
        type=float, required=True,
    )
    parser.add_argument(
        "--small-ebs", help="size of root ebs when instance storage is enough, in GiB",
        type=float, required=True,
    )
    parser.add_argument(
        "--big-ebs", help="size of root ebs when instance storage is not enough, in GiB",
        type=float, required=True,
    )
    parser.add_argument(
        "--max-returned", help="maximum number of instances to return", type=int,
    )
    parser.add_argument(
        "--skip-instance-types", help="comma separated list of instance types to skip",
        type=str,
    )

    args = parser.parse_args()

    headers = []
    lines = []
    with open(args.instance_data, 'r', newline='') as f:
        reader = csv.reader(f)
        headers += reader.__next__()
        for row in reader:
            lines.append(row)

    instances = parse_headers_and_lines(headers, lines)

    good_instances = []
    for instance in instances:
        if all([f(args, instance) for f in instance_checks]):
            good_instances.append(instance)

    good_instances = sorted(good_instances, key=lambda i: i['ECU per vCPU'], reverse=True)

    if args.max_returned:
        good_instances = good_instances[:args.max_returned]

    instance_datas = []
    for instance in good_instances:
        instance_datas.append(make_one_instance_data(args, instance))

    print(json.dumps({'instance_data': instance_datas}))


if __name__ == '__main__':
    main()
