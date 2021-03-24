#!/usr/bin/env python3
import argparse
import contextlib
import json
import logging
import os
import subprocess
import tempfile
import time
from http.client import HTTPConnection

import requests
import ruamel.yaml as yaml

from paasta_tools.utils import DEFAULT_SOA_CONFIGS_GIT_URL
from paasta_tools.utils import format_git_url
from paasta_tools.utils import load_system_paasta_config

requests_log = logging.getLogger("requests.packages.urllib3")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


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
        "-j",
        "--jira-creds",
        help="Service credentials for JIRA API, user:pass",
        dest="jira_creds",
        required=False,
    )
    parser.add_argument(
        "-t",
        "--ticket",
        help="Create JIRA tickets for every service in corresponding project (not available in bulk mode)",
        action="store_true",
        dest="ticket",
        default=False,
    )
    parser.add_argument(
        "-r",
        "--reviews",
        help="Guess owners of each service and create reviews automatically",
        action="store_true",
        dest="create_reviews",
        default=False,
    )
    parser.add_argument(
        "-p",
        "--publish-reviews",
        help="Guess owners of each service and publish reviews automatically",
        action="store_true",
        dest="publish_reviews",
        default=False,
    )
    parser.add_argument(
        "-b",
        "--bulk",
        help="Patch all services in the report with only one code review",
        action="store_true",
        dest="bulk",
        default=False,
    )
    parser.add_argument(
        "--app",
        help="Splunk app of the CSV file",
        default="yelp_performance",
        required=False,
        dest="splunk_app",
    )
    parser.add_argument(
        "-c",
        "--csv-report",
        help="Splunk csv file from which to pull data.",
        required=True,
        dest="csv_report",
    )
    parser.add_argument(
        "-y",
        "--yelpsoa-configs-dir",
        help="Use provided existing yelpsoa-configs instead of cloning the repo in a temporary dir. Only avail with -b option",
        dest="YELPSOA_DIR",
        required=False,
    )
    parser.add_argument(
        "-l",
        "--local",
        help="Do not create a branch. Implies -y and -b.",
        action="store_true",
        dest="no_branch",
        default=False,
    )
    parser.add_argument(
        "-v", "--verbose", help="Debug mode.", action="store_true", dest="verbose",
    )

    return parser.parse_args()


def tempdir():
    tmp = tempfile.TemporaryDirectory(prefix="repo", dir="/nail/tmp")
    log.debug(f"Created temp directory: {tmp.name}")
    return tmp


@contextlib.contextmanager
def cwd(path):
    pwd = os.getcwd()
    os.chdir(path)
    log.debug(f"Switching from directory {pwd} to {path}")
    try:
        yield
    finally:
        log.debug(f"Switching back from directory {path} to {pwd}")
        os.chdir(pwd)


def get_report_from_splunk(creds, app, filename, criteria_filter):
    """ Expect a table containing at least the following fields:
    criteria (<service> [marathon|kubernetes]-<cluster_name> <instance>)
    service_owner (Optional)
    project (Required to create tickets)
    estimated_monthly_savings (Optional)
    search_time (Unix time)
    one of the following pairs:
    - current_cpus and suggested_cpus
    - current_mem and suggested_mem
    - current_disk and suggested_disk
    - suggested_hacheck_cpus
    - suggested_cpu_burst_add
    - suggested_min_instances
    - suggested_max_instances
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
        serv["min_instances"] = d["result"].get("suggested_min_instances")
        serv["max_instances"] = d["result"].get("suggested_max_instances")
        serv["hacheck_cpus"] = d["result"].get("suggested_hacheck_cpus")
        serv["cpu_burst_add"] = d["result"].get("suggested_cpu_burst_add")
        services_to_update[criteria] = serv

    return {
        "search": search,
        "results": services_to_update,
    }


def clone_in(target_dir, system_paasta_config=None):
    if not system_paasta_config:
        system_paasta_config = load_system_paasta_config()
    repo_config = system_paasta_config.get_git_repo_config("yelpsoa-configs")

    remote = format_git_url(
        system_paasta_config.get_git_config()["git_user"],
        repo_config.get("git_server", DEFAULT_SOA_CONFIGS_GIT_URL),
        repo_config["repo_name"],
    )
    subprocess.check_call(("git", "clone", remote, target_dir))


def create_branch(branch_name):
    subprocess.check_call(("git", "checkout", "master"))
    subprocess.check_call(("git", "checkout", "-b", branch_name))


def bulk_commit(filenames, originating_search):
    message = f"Rightsizer bulk update\n\nSplunk search:\n{originating_search}"
    subprocess.check_call(["git", "add"] + filenames)
    subprocess.check_call(("git", "commit", "-n", "-m", message))


def bulk_review(filenames, originating_search, publish=False):
    reviewers = set(get_reviewers_in_group("right-sizer"))
    for filename in filenames:
        reviewers = reviewers.union(get_reviewers(filename))

    reviewers_arg = " ".join(list(reviewers))
    summary = "Rightsizer bulk update"
    description = (
        "This is an automated bulk review. It will be shipped automatically if a primary reviewer gives a shipit. If you think this should not be shipped, talk to one of the primary reviewers. \n\n"
        "This review is based on results from the following Splunk search:\n"
        f"{originating_search}"
    )
    review_cmd = [
        "review-branch",
        f"--summary={summary}",
        f"--description={description}",
        "--reviewers",
        reviewers_arg,
        "--server",
        "https://reviewboard.yelpcorp.com",
    ]
    if publish:
        review_cmd.append("-p")

    subprocess.check_call(review_cmd)


def commit(filename, serv):
    message = "Updating {} for {}provisioned cpu from {} to {} cpus".format(
        filename, serv["state"], serv["old_cpus"], serv["cpus"]
    )
    log.debug(f"Commit {filename} with the following message: {message}")
    subprocess.check_call(("git", "add", filename))
    subprocess.check_call(("git", "commit", "-n", "-m", message))


def get_reviewers_in_group(group_name):
    """Using rbt's target-groups argument overrides our configured default review groups.
    So we'll expand the group into usernames and pass those users in the group individually.
    """
    rightsizer_reviewers = json.loads(
        subprocess.check_output(
            (
                "rbt",
                "api-get",
                "--server",
                "https://reviewboard.yelpcorp.com",
                f"groups/{group_name}/users/",
            )
        ).decode("UTF-8")
    )
    return [user.get("username", "") for user in rightsizer_reviewers.get("users", {})]


def get_reviewers(filename):
    recent_authors = set()
    authors = (
        subprocess.check_output(("git", "log", "--format=%ae", "--", filename))
        .decode("UTF-8")
        .splitlines()
    )

    authors = [x.split("@")[0] for x in authors]
    for author in authors:
        if "no-reply" in author:
            continue
        recent_authors.add(author)
        if len(recent_authors) >= 3:
            break
    return recent_authors


def review(filename, summary, description, publish_review):
    all_reviewers = get_reviewers(filename).union(get_reviewers_in_group("right-sizer"))
    reviewers_arg = " ".join(all_reviewers)
    publish_arg = "-p" if publish_review is True else "-d"
    subprocess.check_call(
        (
            "review-branch",
            f"--summary={summary}",
            f"--description={description}",
            publish_arg,
            "--reviewers",
            reviewers_arg,
            "--server",
            "https://reviewboard.yelpcorp.com",
        )
    )


def edit_soa_configs(filename, instance, cpu, mem, disk):
    if not os.path.exists(filename):
        filename = filename.replace("marathon", "kubernetes")
    if os.path.islink(filename):
        real_filename = os.path.realpath(filename)
        os.remove(filename)
    else:
        real_filename = filename
    try:
        with open(real_filename, "r") as fi:
            yams = fi.read()
            yams = yams.replace("cpus: .", "cpus: 0.")
            data = yaml.round_trip_load(yams, preserve_quotes=True)

        instdict = data[instance]
        if cpu:
            instdict["cpus"] = float(cpu)
        if mem:
            mem = max(128, round(float(mem)))
            instdict["mem"] = mem
        if disk:
            instdict["disk"] = round(float(disk))
        out = yaml.round_trip_dump(data, width=120)

        with open(filename, "w") as fi:
            fi.write(out)
    except FileNotFoundError:
        log.exception(f"Could not find {filename}")
    except KeyError:
        log.exception(f"Error in {filename}. Will continue")


def create_jira_ticket(serv, creds, description, JIRA):
    creds = creds.split(":")
    options = {"server": "https://jira.yelpcorp.com"}
    jira_cli = JIRA(options=options, basic_auth=(creds[0], creds[1]))  # noqa: F821
    jira_ticket = {}
    # Sometimes a project has required fields we can't predict
    try:
        jira_ticket = {
            "project": {"key": serv["project"]},
            "description": description,
            "issuetype": {"name": "Improvement"},
            "labels": ["perf-watching", "paasta-rightsizer"],
            "summary": "{s}.{i} in {c} may be {o}provisioned".format(
                s=serv["service"],
                i=serv["instance"],
                c=serv["cluster"],
                o=serv["state"],
            ),
        }
        tick = jira_cli.create_issue(fields=jira_ticket)
    except Exception:
        jira_ticket["project"] = {"key": "PEOBS"}
        jira_ticket["labels"].append(serv["service"])
        tick = jira_cli.create_issue(fields=jira_ticket)
    return tick.key


def _get_dashboard_qs_param(param, value):
    # Some dashboards may ask for query string params like param=value, but not this provider.
    return f"variables%5B%5D={param}%3D{param}:{value}"


def generate_ticket_content(serv):
    cpus = float(serv["cpus"])
    provisioned_state = "over"
    if cpus > float(serv["old_cpus"]):
        provisioned_state = "under"

    serv["state"] = provisioned_state
    ticket_desc = (
        "This ticket and CR have been auto-generated to help keep PaaSTA right-sized."
        "\nPEOBS will review this CR and give a shipit. Then an ops deputy from your team can merge"
        " if these values look good for your service after review."
        "\nOpen an issue with any concerns and someone from PEOBS will respond."
        "\nWe suspect that {s}.{i} in {c} may have been {o}-provisioned"
        " during the 1 week prior to {d}. It initially had {x} cpus, but based on the below dashboard,"
        " we recommend {y} cpus."
        "\n- Dashboard: https://y.yelpcorp.com/{o}provisioned?{cluster_param}&{service_param}&{instance_param}"
        "\n- Service owner: {n}"
        "\n- Estimated monthly excess cost: ${m}"
        "\n\nFor more information and sizing examples for larger services:"
        "\n- Runbook: https://y.yelpcorp.com/rb-provisioning-alert"
        "\n- Alert owner: pe-observability@yelp.com"
    ).format(
        s=serv["service"],
        c=serv["cluster"],
        i=serv["instance"],
        o=provisioned_state,
        d=serv["date"],
        n=serv["owner"],
        m=serv["money"],
        x=serv["old_cpus"],
        y=serv["cpus"],
        cluster_param=_get_dashboard_qs_param(
            "paasta_cluster", serv["cluster"].replace("marathon-", "")
        ),
        service_param=_get_dashboard_qs_param("paasta_service", serv["service"]),
        instance_param=_get_dashboard_qs_param("paasta_instance", serv["instance"]),
    )
    summary = f"Rightsizing {serv['service']}.{serv['instance']} in {serv['cluster']} to make it not have {provisioned_state}-provisioned cpu"  # noqa: E501
    return (summary, ticket_desc)


def bulk_rightsize(report, create_code_review, publish_code_review, create_new_branch):
    if create_new_branch:
        branch = "rightsize-bulk-{}".format(int(time.time()))
        create_branch(branch)

    filenames = []
    for _, serv in report["results"].items():
        filename = "{}/{}.yaml".format(serv["service"], serv["cluster"])
        filenames.append(filename)
        cpus = serv.get("cpus", None)
        mem = serv.get("mem", None)
        disk = serv.get("disk", None)
        edit_soa_configs(filename, serv["instance"], cpus, mem, disk)
    if create_code_review:
        bulk_commit(filenames, report["search"])
        bulk_review(filenames, report["search"], publish_code_review)


def individual_rightsize(
    report, create_tickets, jira_creds, create_review, publish_review, JIRA
):
    for _, serv in report["results"].items():
        filename = "{}/{}.yaml".format(serv["service"], serv["cluster"])
        summary, ticket_desc = generate_ticket_content(serv)

        if create_tickets is True:
            branch = create_jira_ticket(serv, jira_creds, ticket_desc, JIRA)
        else:
            branch = "rightsize-{}".format(int(time.time() * 1000))

        create_branch(branch)
        cpus = serv.get("cpus", None)
        mem = serv.get("mem", None)
        disk = serv.get("disk", None)
        edit_soa_configs(filename, serv["instance"], cpus, mem, disk)
        try:
            commit(filename, serv)
            if create_review:
                review(filename, summary, ticket_desc, publish_review)
        except Exception:
            log.exception(
                (
                    "\nUnable to push changes to {f}. Check if {f} conforms to"
                    "yelpsoa-configs yaml rules. No review created. To see the"
                    "cpu suggestion for this service check {t}."
                ).format(f=filename, t=branch)
            )
            continue


def main():
    args = parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
        requests_log.setLevel(logging.DEBUG)
        HTTPConnection.debuglevel = 2
        requests_log.propagate = True

    # Safety checks
    if args.no_branch and not args.YELPSOA_DIR:
        log.error(
            "You must specify --yelpsoa-configs-dir to work on if you use the --local option"
        )
        return False

    if args.ticket:
        if not args.jira_creds:
            raise ValueError("No JIRA creds specified")
        # Only import the jira module if we need too
        from jira.client import JIRA  # noqa: F401
    else:
        JIRA = None

    report = get_report_from_splunk(
        args.splunk_creds, args.splunk_app, args.csv_report, args.criteria_filter
    )

    tmpdir = tempdir()  # Create a tmp dir even if we are not using it

    working_dir = args.YELPSOA_DIR
    system_paasta_config = load_system_paasta_config()
    if working_dir is None:
        # Working in a temporary directory
        working_dir = os.path.join("rightsizer", tmpdir.name)
        clone_in(working_dir, system_paasta_config=system_paasta_config)

    with cwd(working_dir):
        if args.bulk or args.no_branch:
            log.info("Running in bulk mode")
            bulk_rightsize(
                report, args.create_reviews, args.publish_reviews, not args.no_branch
            )
        else:
            individual_rightsize(
                report,
                args.ticket,
                args.jira_creds,
                args.create_reviews,
                args.publish_reviews,
                JIRA,
            )

    tmpdir.cleanup()  # Cleanup any tmpdire used


if __name__ == "__main__":
    main()
