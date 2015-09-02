#!/usr/bin/env python
import logging
import sys

import isodate
import requests_cache

import chronos_tools
from paasta_tools.utils import PaastaColors


log = logging.getLogger("__main__")
log.addHandler(logging.StreamHandler(sys.stdout))


def format_chronos_job_status(job):
    """Given a job, returns a pretty-printed human readable output regarding
    the status of the job.
    Currently only reports whether the job is disabled or enabled

    :param job: dictionary of the job status
    """
    status = PaastaColors.red("UNKNOWN")
    if job.get("disabled", False):
        status = PaastaColors.red("Disabled")
    else:
        status = PaastaColors.green("Enabled")

    last_result = PaastaColors.red("UNKNOWN")
    fail_result = PaastaColors.red("Fail")
    ok_result = PaastaColors.green("OK")
    last_error = job.get("lastError", "")
    last_success = job.get("lastSuccess", "")
    if not last_error and not last_success:
        last_result = PaastaColors.yellow("New")
    elif not last_error:
        last_result = ok_result
    elif not last_success:
        last_result = fail_result
    else:
        fail_dt = isodate.parse_datetime(last_error)
        ok_dt = isodate.parse_datetime(last_success)
        if ok_dt > fail_dt:
            last_result = ok_result
        else:
            last_result = fail_result

    return "Status: %s Last: %s" % (status, last_result)


def status_chronos_job(job_id, all_jobs):
    """Returns a formatted string of the status of a chronos job

    :param job_id: the idenfier of the job (beginning of its name) in the
    chronos api
    :param all_jobs: list of all the jobs from chronos
    """
    # The actual job name will contain a git<hash> and config<hash>, so we'll
    # look for things that start with our job_id. We add SPACER to the end as
    # an anchor to prevent catching "my_service my_job_extra" when looking for
    # "my_service my_job".
    job_id_pattern = "%s%s" % (job_id, chronos_tools.SPACER)
    our_jobs = [job for job in all_jobs if job["name"].startswith(job_id_pattern)]
    if our_jobs == []:
        return "%s: %s is not setup yet" % (PaastaColors.yellow("Warning"), job_id)
    else:
        output = [format_chronos_job_status(job) for job in our_jobs]
        return "\n".join(output)


def perform_command(command, service, instance):
    job_id = chronos_tools.compose_job_id(service, instance)
    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)

    if command == "status":
        # Setting up transparent cache for http API calls
        requests_cache.install_cache("paasta_serviceinit", backend="memory")

        all_jobs = client.list()
        print status_chronos_job(job_id, all_jobs)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    sys.exit(0)


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
