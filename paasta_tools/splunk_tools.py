import json
import logging

import requests

requests_log = logging.getLogger("requests.packages.urllib3")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


def get_report_from_splunk(creds, app, filename, criteria_filter):
    """ Expect a table containing at least the following fields:
    criteria (<service> [marathon|kubernetes]-<cluster_name> <instance>)
    service_owner (Optional)
    project (Required to create tickets)
    estimated_monthly_savings (Optional)
    search_time (Unix time)
    one of the following pairs:
    - current_cpus
    suggested_cpus
    - current_mem
    suggested_mem
    - current_disk
    suggested_disk
    """
    url = f"https://splunk-api.yelpcorp.com/servicesNS/nobody/{app}/search/jobs/export"
    search = (
        '| inputlookup {filename} | search criteria="{criteria_filter}"'
        '| eval _time = search_time | where _time > relative_time(now(),"-7d")'
    ).format(filename=filename, criteria_filter=criteria_filter)
    log.debug(f"Sending this query to Splunk: {search}\n")
    data = {"output_mode": "json", "search": search}
    creds = creds.split(":")
    resp = requests.post(url, data=data, auth=(creds[0], creds[1]))
    resp_text = resp.text.split("\n")
    log.info("Found {} services to rightsize".format(len(resp_text) - 1))
    resp_text = [x for x in resp_text if x]
    resp_text = [json.loads(x) for x in resp_text]
    services_to_update = {}
    for d in resp_text:
        if "result" not in d:
            raise ValueError(f"Splunk request didn't return any results: {resp_text}")
        criteria = d["result"]["criteria"]
        serv = {}
        serv["service"] = criteria.split(" ")[0]
        serv["cluster"] = criteria.split(" ")[1]
        serv["instance"] = criteria.split(" ")[2]
        serv["owner"] = d["result"].get("service_owner", "Unavailable")
        serv["date"] = d["result"]["_time"].split(" ")[0]
        serv["money"] = d["result"].get("estimated_monthly_savings", 0)
        serv["project"] = d["result"].get("project", "Unavailable")
        serv["cpus"] = d["result"].get("suggested_cpus")
        serv["old_cpus"] = d["result"].get("current_cpus")
        serv["mem"] = d["result"].get("suggested_mem")
        serv["old_mem"] = d["result"].get("current_mem")
        serv["disk"] = d["result"].get("suggested_disk")
        serv["old_disk"] = d["result"].get("current_disk")
        serv["hacheck_cpus"] = d["result"].get("suggested_hacheck_cpus")
        services_to_update[criteria] = serv

    return {
        "search": search,
        "results": services_to_update,
    }
