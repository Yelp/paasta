import argparse
import logging
from collections import defaultdict

from paasta_tools.config_utils import AutoConfigUpdater
from paasta_tools.contrib.paasta_update_soa_memcpu import get_report_from_splunk


NULL = "null"


def parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-s",
        "--splunk-creds",
        help="Service credentials for Splunk API, user:pass",
        dest="splunk_creds",
        required=True,
    )
    parser.add_argument(
        "-f",
        "--criteria-filter",
        help="Filter Splunk search results criteria field. Default: *",
        dest="criteria_filter",
        required=False,
        default="*",
    )
    parser.add_argument(
        "-c",
        "--csv-report",
        help="Splunk csv file from which to pull data.",
        required=True,
        dest="csv_report",
    )
    parser.add_argument(
        "--app",
        help="Splunk app of the CSV file",
        default="yelp_computeinfra",
        required=False,
        dest="splunk_app",
    )
    parser.add_argument(
        "--git-remote",
        help="Master git repo for soaconfigs",
        required=True,
        dest="git_remote",
    )
    parser.add_argument(
        "--branch",
        help="Branch name to push to. Defaults to master",
        default="master",
        required=False,
        dest="branch",
    )
    parser.add_argument(
        "--push-to-remote",
        help="Actually push to remote. Otherwise files will only be modified and validated.",
        action="store_true",
        dest="push_to_remote",
    )
    parser.add_argument(
        "--local-dir",
        help="Act on configs in the local directory rather than cloning the git_remote",
        required=False,
        default=None,
        dest="local_dir",
    )
    parser.add_argument(
        "--source-id",
        help="String to attribute the changes in the commit message. Defaults to csv report name",
        required=False,
        default=None,
        dest="source_id",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Logging verbosity",
        action="store_true",
        dest="verbose",
    )
    return parser.parse_args()


def get_recommendation_from_result(result):
    rec = {}
    cpus = result.get("cpus")
    if cpus and cpus != NULL:
        rec["cpus"] = float(cpus)
    mem = result.get("mem")
    if mem and mem != NULL:
        rec["mem"] = max(128, round(float(mem)))
    disk = result.get("disk")
    if disk and disk != NULL:
        rec["disk"] = max(128, round(float(disk)))
    return rec


def get_recommendations_by_service_file(results):
    results_by_service_file = defaultdict(dict)
    for result in results.values():
        key = (
            result["service"],
            result["cluster"],
        )  # e.g. (foo, marathon-norcal-stagef)
        rec = get_recommendation_from_result(result)
        if not rec:
            continue
        results_by_service_file[key][result["instance"]] = rec
    return results_by_service_file


def get_extra_message(splunk_search_string):
    return f"""This review is based on results from the following Splunk search:\n
    {splunk_search_string}
    """


def main(args):
    report = get_report_from_splunk(
        args.splunk_creds, args.splunk_app, args.csv_report, args.criteria_filter
    )
    extra_message = get_extra_message(report["search"])
    config_source = args.source_id or args.csv_report

    results = get_recommendations_by_service_file(report["results"])
    updater = AutoConfigUpdater(
        config_source=config_source,
        git_remote=args.git_remote,
        branch=args.branch,
        working_dir=args.local_dir or "/nail/tmp",
        do_clone=args.local_dir is None,
    )
    with updater:
        for (service, extra_info), instance_recommendations in results.items():
            existing_recommendations = updater.get_existing_configs(service, extra_info)
            for instance_name, recommendation in instance_recommendations.items():
                existing_recommendations.setdefault(instance_name, {})
                existing_recommendations[instance_name].update(recommendation)
            updater.write_configs(service, extra_info, existing_recommendations)

        if args.push_to_remote:
            updater.commit_to_remote(extra_message=extra_message)
        else:
            updater.validate()


if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    main(args)
