#!/usr/bin/env python
import argparse
import json
import time
from collections import defaultdict
from datetime import datetime

import boto3
import matplotlib.pyplot as plt


region_names = {
    'us-east-1': 'US East (N. Virginia)',
    'us-east-2': 'US East (Ohio)',
    'us-west-1': 'US West (N. California)',
    'us-west-2': 'US West (Oregon)',
}


def get_on_demand_price(pricing, region, instance_type):
    r = pricing.get_products(
        ServiceCode='AmazonEC2',
        Filters=[
            {
                'Type': 'TERM_MATCH',
                'Field': 'instanceType',
                'Value': instance_type,
            }, {
                'Type': "TERM_MATCH",
                'Field': "operatingSystem",
                'Value': "Linux",
            }, {
                'Type': 'TERM_MATCH',
                'Field': 'location',
                'Value': region_names[region],
            }, {
                'Type': "TERM_MATCH",
                'Field': "tenancy",
                "Value": "Shared",
            },
        ],
    )
    print(instance_type)
    if len(r['PriceList']) == 0:
        return None
    assert len(r['PriceList']) == 1, r
    p = json.loads(r['PriceList'][0])
    price = float(list(list(p['terms']['OnDemand'].values())[0]['priceDimensions'].values())[0]['pricePerUnit']['USD'])
    vcpu = int(p['product']['attributes']['vcpu'])
    return {'price': price, 'vcpu': vcpu}


def get_spot_price_history(ec2, instance_types, ts, bid_per_core, on_demand_prices, azs):
    ret = ec2.describe_spot_price_history(
        InstanceTypes=instance_types,
        ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
        StartTime=ts,
        EndTime=ts,
    )
    history = ret['SpotPriceHistory']
    price_history = defaultdict(dict)

    for item in history:
        item['Timestamp'] = str(item['Timestamp'])
        item['SpotPrice'] = float(item['SpotPrice'])
        item['Bid'] = bid_per_core * on_demand_prices[item['InstanceType']]['vcpu']
        item['Price'] = on_demand_prices[item['InstanceType']]['price']
        if item['AvailabilityZone'] not in azs:
            continue
        price_history[item['InstanceType']][item['AvailabilityZone']] = item

    return price_history


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--availability-zones", type=str, required=True)
    parser.add_argument("--instance-types", type=str, required=True)
    parser.add_argument("--bid-per-core", type=float, required=True)
    parser.add_argument("--days", type=int, default=5)

    args = parser.parse_args()
    azs = args.availability_zones.split(',')
    region = azs[0][:-1]
    instance_types = args.instance_types.split(',')
    bid_per_core = args.bid_per_core

    pricing = boto3.client('pricing', region_name='us-east-1')

    on_demand_prices = {i_type: get_on_demand_price(pricing, region, i_type) for i_type in instance_types}

    ec2 = boto3.client('ec2', region_name=region)

    spot_histories = {}
    ts = int(time.time())
    end = ts - 60 * 60 * 24 * args.days
    while ts > end:
        print(ts)
        spot_histories[ts] = get_spot_price_history(ec2, instance_types, ts, bid_per_core, on_demand_prices, azs)
        ts -= 3600

    x = []
    oks = []
    no_launches = []
    terminates = []
    for ts, item in spot_histories.items():
        x.append(datetime.fromtimestamp(ts))
        ok = 0
        no_launch = 0
        terminate = 0
        for itype, az_data in item.items():
            for az, data in az_data.items():
                if data['SpotPrice'] < data['Price'] and data['SpotPrice'] < data['Bid']:
                    ok += 1
                elif data['SpotPrice'] < data['Bid']:
                    no_launch += 1
                else:
                    terminate += 1
        oks.append(ok)
        no_launches.append(no_launch)
        terminates.append(terminate)

    fig, ax = plt.subplots()
    ax.stackplot(
        x, oks, no_launches, terminates,
        baseline='zero',
        labels=['OK', 'No launch', 'Terminate'],
        colors=['green', 'yellow', 'red'],
        step='mid',
    )
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    plt.title("%s bid status" % region)
    plt.ylabel("Number of instances in each state")
    plt.show()


if __name__ == '__main__':
    main()
