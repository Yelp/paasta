#!/usr/bin/env python
import logging
import sys

import isodate
import requests_cache

import chronos_tools
from paasta_tools.utils import PaastaColors


log = logging.getLogger("__main__")
log.addHandler(logging.StreamHandler(sys.stdout))


def _get_disabled(job):
    status = PaastaColors.red("UNKNOWN")
    if job.get("disabled", False):
        status = PaastaColors.red("Disabled")
    else:
        status = PaastaColors.green("Enabled")
    return status


def _get_last_result(job):
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
    return last_result


def format_chronos_job_status(job, desired_state):
    """Given a job, returns a pretty-printed human readable output regarding
    the status of the job.

    :param job: dictionary of the job status
    :param desired_state: a pretty-formatted string representing the
    job's started/stopped state as set with paast emergency-[stop|start], e.g.
    the result of get_desired_state_human()
    """
    is_disabled = _get_disabled(job)
    is_stopped = desired_state
    last_result = _get_last_result(job)
    return "Status: %s, %s Last: %s" % (is_disabled, is_stopped, last_result)


def status_chronos_job(jobs, complete_job_config):
    """Returns a formatted string of the status of a list of chronos jobs

    :param jobs: list of dicts of chronos job info as returned by the chronos
    client
    """
    if jobs == []:
        return "%s: chronos job is not setup yet" % PaastaColors.yellow("Warning")
    else:
        desired_state = complete_job_config.get_desired_state_human()
        output = [format_chronos_job_status(job, desired_state) for job in jobs]
        return "\n".join(output)


def perform_command(command, service, instance, cluster, verbose, soa_dir):
    chronos_config = chronos_tools.load_chronos_config()
    complete_job_config = chronos_tools.load_chronos_job_config(service, instance, cluster, soa_dir=soa_dir)
    client = chronos_tools.get_chronos_client(chronos_config)
    job_id = chronos_tools.compose_job_id(service, instance)

    if command == "status":
        # Setting up transparent cache for http API calls
        requests_cache.install_cache("paasta_serviceinit", backend="memory")

        # We add SPACER to the end as an anchor to prevent catching
        # "my_service my_job_extra" when looking for "my_service my_job".
        job_pattern = "%s%s" % (job_id, chronos_tools.SPACER)
        jobs = chronos_tools.lookup_chronos_jobs(job_pattern, client, include_disabled=True)
        print "Job id: %s" % job_id
        print status_chronos_job(jobs, complete_job_config)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    return 0


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
