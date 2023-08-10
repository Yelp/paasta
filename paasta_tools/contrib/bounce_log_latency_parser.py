#!/usr/bin/env python3.8
import itertools
import json
import sys
from collections import defaultdict
from datetime import datetime


def get_datetime_from_ts(ts):
    tformat = "%Y-%m-%dT%H:%M:%S.%f"
    return datetime.strptime(ts, tformat)


def get_deploy_durations_from_file(filename):
    """
    filename: path to a file to be parsed for datetime data
    The expected input is a paasta service log for the deploy events
    The way I've been fetching them is by running 'internal logreader command' | grep deploy | grep event > filename
    """
    file_object = open(filename, "r")
    data = sorted(
        [json.loads(line.rstrip("\n")) for line in file_object],
        key=lambda x: get_datetime_from_ts(x["timestamp"]),
    )

    timedeltas = defaultdict(list)
    last_time = dict()
    instance_bitvector = defaultdict(bool)  # defaults to False

    for datum in data:
        time = get_datetime_from_ts(datum["timestamp"])
        instance = datum["instance"]
        if "in progress" in datum["message"] and not instance_bitvector[instance]:
            instance_bitvector[instance] = True
            last_time[instance] = time
        elif "finishing" in datum["message"]:
            instance_bitvector[instance] = False
            timedeltas[instance].append(time - last_time[instance])

    return timedeltas


def display_bounce_info(timedeltas):
    """
    timedeltas: iterable of timedelta objects
    """
    std = list(sorted(timedeltas))
    print("Median time to bounce: {} seconds".format(std[len(std) / 2]))
    print("10% time to bounce: {}".format(std[len(std) / 10]))
    print("90% time to bounce: {}".format(std[len(std) * 9 / 10]))


def main(filenames):
    for filename in filenames:
        print(filename)
        print("=========================")
        timedeltas = get_deploy_durations_from_file(filename)
        for instance, tdlist in timedeltas.items():
            if timedeltas:
                print("Instance: %s" % instance)
                display_bounce_info(tdlist)
        print("Overall:")
        display_bounce_info(itertools.chain.from_iterable(timedeltas.values()))
        print("=========================")


if __name__ == "__main__":
    main(filenames=sys.argv[1:])
