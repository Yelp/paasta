#!/usr/bin/env python3
import argparse
import contextlib
import json
import os
import subprocess
import tempfile
import time

import requests
import ruamel.yaml as yaml
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def parse_args(argv):
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-s",
        "--splunk-creds",
        help="Service credentials for Splunk API, user:pass",
        dest="splunk_creds",
        required=True,
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
        "-v",
        "--verbose",
        help="Debug mode.",
        action="store_true",
        dest="verbose",
    )

    return parser.parse_args(argv)


@contextlib.contextmanager
def cwd(path):
    pwd = os.getcwd()
    os.chdir(path)
    log.debug("Switching from directory {} to {}".format(pwd, path))
    try:
        yield
    finally:
        log.debug("Switching back from directory {} to {}".format(path, pwd))
        os.chdir(pwd)


@contextlib.contextmanager
def in_tempdir():
    with tempfile.TemporaryDirectory(prefix="repo", dir="/nail/tmp") as tmp:
        log.info("Created temp directory: {}".format(tmp))
        with cwd(tmp):
            yield


def get_report_from_splunk(creds, filename):
    """ Expect a table containing at least the following fields:
    criteria (<service> [marathon|kubernetes]-<cluster_name> <instance>)
    service_owner
    project (Required to create tickets)
    estimated_monthly_savings (Optional)
    search_time (Unix time)
    current_cpus (Optional if current_mem is specified)
    suggested_cpus (Optional if suggested_mem is specified)
    current_mem (Optional if current_cpus is specified)
    suggested_mem (Optional if suggested_cpus is specified)
    """
    url = "https://splunk-api.yelpcorp.com/servicesNS/nobody/yelp_performance/search/jobs/export"
    search = (
        "| inputlookup {} |"
        ' eval _time = search_time | where _time > relative_time(now(),"-7d")'
    ).format(filename)
    data = {"output_mode": "json", "search": search}
    creds = creds.split(":")
    resp = requests.post(url, data=data, auth=(creds[0], creds[1]))
    resp_text = resp.text.split("\n")
    resp_text = [x for x in resp_text if x]
    resp_text = [json.loads(x) for x in resp_text]
    services_to_update = {}
    for d in resp_text:
        if not "result" in d:
            raise ValueError("Splunk request didn't return any results: {}".format(resp_text))
        criteria = d["result"]["criteria"]
        serv = {}
        serv["service"] = criteria.split(" ")[0]
        serv["cluster"] = criteria.split(" ")[1]
        serv["instance"] = criteria.split(" ")[2]
        serv["owner"] = d["result"]["service_owner"]
        serv["date"] = d["result"]["_time"].split(" ")[0]
        serv["money"] = d["result"].get("estimated_monthly_savings", 0)
        serv["project"] = d["result"].get("project", "Unavailable")
        serv["cpus"] = d["result"].get("suggested_cpus")
        serv["old_cpus"] = d["result"].get("current_cpus")
        serv["mem"] = d["result"].get("suggested_mem")
        serv["old_mem"] = d["result"].get("current_mem")
        services_to_update[criteria]=serv

    return services_to_update


def clone(target_dir):
    remote = "git@sysgit.yelpcorp.com:yelpsoa-configs"
    subprocess.check_call(("git", "clone", remote, target_dir))


def create_branch(branch_name):
    subprocess.check_call(("git", "checkout", "-b", branch_name))


def bulk_commit(filenames):
    message = "Rightsizer bulk update"
    subprocess.check_call(["git", "add"] + filenames)
    subprocess.check_call(("git", "commit", "-n", "-m", message))


def bulk_review(filenames, publish=False):
    reviewers = set(get_reviewers_in_group("right-sizer"))
    for filename in filenames:
        reviewers = reviewers.union(get_reviewers(filename))

    reviewers_arg = " ".join(list(reviewers))
    summary = "Rightsizer bulk update"
    description = "This is an automated bulk review. It will be shipped automatically if a primary reviewer gives a shipit. If you think this should not be shipped, talk to one of the primary reviewers."
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


def review(filename, summary, description, publish_reviews):
    all_reviewers = get_reviewers(filename).union(get_reviewers_in_group("right-sizer"))
    reviewers_arg = " ".join(all_reviewers)
    publish_arg = "-p" if publish_reviews is True else " "
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


def edit_soa_configs(filename, instance, cpu, mem):
    if not os.path.exists(filename):
        filename=filename.replace("marathon", "kubernetes")
    if os.path.islink(filename):
        real_filename=os.path.realpath(filename)
        os.remove(filename)
    else:
        real_filename=filename
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
        out = yaml.round_trip_dump(data, width=120)

        with open(filename, "w") as fi:
            fi.write(out)
    except FileNotFoundError:
        log.warning("Could not find {}".format(filename))
    except KeyError:
        log.warning("Error in {}".format(filename))


def create_jira_ticket(serv, creds, description):
    creds = creds.split(":")
    options = {"server": "https://jira.yelpcorp.com"}
    jira_cli = JIRA(options=options, basic_auth=(creds[0], creds[1]))
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
    summary = f"Rightsizing {serv["service"]}.{serv["instance"]} in {serv["cluster"]} to make it not have {provisioned_state}-provisioned cpu"  # noqa: E501
    return (summary, ticket_desc)


def bulk_rightsize(report, working_dir, create_code_review, publish_code_review):
    with cwd(working_dir):
        branch = "rightsize-bulk-{}".format(int(time.time()))
        create_branch(branch)
        filenames=[]
        for _, serv in report.items():
            filename = "{}/{}.yaml".format(serv["service"], serv["cluster"])
            filenames.append(filename)
            cpus=serv.get("cpus", None)
            mem=serv.get("mem", None)
            edit_soa_configs(filename, serv["instance"], cpus, mem)
        if create_code_review:
            bulk_commit(filenames)
            bulk_review(filenames, publish_code_review)


def individual_rightsize(report):
    for serv in cpu_report:
        filename = "{}/{}.yaml".format(serv["service"], serv["cluster"])
        summary, ticket_desc = generate_ticket_content(serv)

        if args.ticket:
            branch = create_jira_ticket(serv, args.jira_creds, ticket_desc)
        else:
            branch = "rightsize-{}".format(int(time.time()))

        with in_tempdir():
            clone(branch)
            cpus=serv.get("cpus", None)
            mem=serv.get("mem", None)
            edit_soa_configs(filename, serv["instance"], cpus, mem)
            try:
                commit(filename, serv)
                review(filename, summary, ticket_desc, args.publish_reviews)
            except Exception:
                log.warning(
                    (
                        "\nUnable to push changes to {f}. Check if {f} conforms to"
                        "yelpsoa-configs yaml rules. No review created. To see the"
                        "cpu suggestion for this service check {t}."
                    ).format(f=filename, t=branch)
                )
                continue


def main(argv=None):
    args = parse_args(argv)
    if args.verbose:
        log.setLevel(logging.DEBUG)

    if args.ticket:
        if not args.jira_creds:
            raise ValueError("No JIRA creds specified")
        # Only import the jira module if we need too
        from jira.client import JIRA

    report = get_report_from_splunk(args.splunk_creds, args.csv_report)

    if args.bulk:
        log.info("Running in bulk mode")
        working_dir = args.YELPSOA_DIR
        if working_dir is not None:
            log.info("Using provided yelpsoa dir: {}".format(working_dir))
            bulk_rightsize(report, working_dir, args.create_reviews, args.publish_reviews)
        else:
            with in_tempdir():
                working_dir="bulk_rightsize"
                clone(working_dir)
                create_code_review=True
                bulk_rightsize(report, working_dir, create_code_review, args.publish_reviews)

    else:
        individual_rightsize(report)


if __name__ == "__main__":
    main()
