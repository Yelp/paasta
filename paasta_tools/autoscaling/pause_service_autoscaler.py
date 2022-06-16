import os
import time
from datetime import datetime

import pytz
from tzlocal import get_localzone as tzlocal_get_localzone

import paasta_tools.paastaapi.models as paastamodels
from paasta_tools.api import client


def get_localzone():
    if "TZ" in os.environ:
        return pytz.timezone(os.environ["TZ"])
    else:
        return tzlocal_get_localzone()


def print_paused_message(pause_time):
    local_tz = get_localzone()
    paused_readable = local_tz.localize(datetime.fromtimestamp(pause_time)).strftime(
        "%F %H:%M:%S %Z"
    )
    print(f"Service autoscaler is paused until {paused_readable}")


def get_service_autoscale_pause_time(cluster):
    api = client.get_paasta_oapi_client(cluster=cluster, http_res=True)
    if not api:
        print("Could not connect to paasta api. Maybe you misspelled the cluster?")
        return 1
    pause_time, status, _ = api.default.get_service_autoscaler_pause(
        _return_http_data_only=False
    )
    if status == 500:
        print("Could not connect to zookeeper server")
        return 2

    pause_time = float(pause_time)
    if pause_time < time.time():
        print("Service autoscaler is not paused")
    else:
        print_paused_message(pause_time)

    return 0


def update_service_autoscale_pause_time(cluster, mins):
    api = client.get_paasta_oapi_client(cluster=cluster, http_res=True)
    if not api:
        print("Could not connect to paasta api. Maybe you misspelled the cluster?")
        return 1
    res, status, _ = api.default.update_service_autoscaler_pause(
        paastamodels.InlineObject(minutes=int(mins)), _return_http_data_only=False
    )
    if status == 500:
        print("Could not connect to zookeeper server")
        return 2

    print(f"Service autoscaler is paused for {mins} minutes")
    return 0


def delete_service_autoscale_pause_time(cluster):
    api = client.get_paasta_oapi_client(cluster=cluster, http_res=True)
    if not api:
        print("Could not connect to paasta api. Maybe you misspelled the cluster?")
        return 1
    res, status, _ = api.default.delete_service_autoscaler_pause(
        _return_http_data_only=False
    )
    if status == 500:
        print("Could not connect to zookeeper server")
        return 2

    print("Service autoscaler is unpaused")
    return 0
