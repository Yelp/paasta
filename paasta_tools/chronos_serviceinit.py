#!/usr/bin/env python
import datetime
import logging
import sys

import humanize
import isodate
import requests_cache

import chronos_tools
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import _log
from paasta_tools.utils import PaastaColors


log = logging.getLogger("__main__")
log.addHandler(logging.StreamHandler(sys.stdout))


# Calls the 'manual start' endpoint in Chronos (https://mesos.github.io/chronos/docs/api.html#manually-starting-a-job),
# running the job now regardless of its 'schedule' and 'disabled' settings. The job's 'schedule' is left unmodified.
def start_chronos_job(service, instance, job_id, client, cluster, job_config, emergency=False):
    name = PaastaColors.cyan(job_id)
    log_reason = PaastaColors.red("EmergencyStart") if emergency else "Brutal bounce"
    log_immediate_run = " and running it immediately" if not job_config['disabled'] else ""
    _log(
        service_name=service,
        line="%s: Sending job %s to Chronos%s" % (log_reason, name, log_immediate_run),
        component='deploy',
        level='event',
        cluster=cluster,
        instance=instance
    )
    client.update(job_config)
    # TODO fail or give some output/feedback to user that the job won't run immediately if disabled (PAASTA-1244)
    if not job_config['disabled']:
        client.run(job_id)


def stop_chronos_job(service, instance, client, cluster, existing_jobs, emergency=False):
    log_reason = PaastaColors.red("EmergencyStop") if emergency else "Brutal bounce"
    for job in existing_jobs:
        name = PaastaColors.cyan(job['name'])
        _log(
            service_name=service,
            line="%s: Killing all tasks for job %s" % (log_reason, name),
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance
        )
        job['disabled'] = True
        client.update(job)
        client.delete_tasks(job['name'])


def restart_chronos_job(service, instance, job_id, client, cluster, matching_jobs, job_config, emergency=False):
    stop_chronos_job(service, instance, client, cluster, matching_jobs, emergency)
    start_chronos_job(service, instance, job_id, client, cluster, job_config, emergency)


def _get_disabled_status(job):
    status = PaastaColors.red("UNKNOWN")
    if job.get("disabled", False):
        status = PaastaColors.red("Disabled")
    else:
        status = PaastaColors.green("Enabled")
    return status


def _prettify_datetime(dt):
    """Prettify datetime objects further. Ignore hardcoded values like 'never'."""
    pretty_dt = dt
    if isinstance(pretty_dt, datetime.datetime):
        dt_localtime = datetime_from_utc_to_local(dt)
        pretty_dt = "%s, %s" % (
            dt_localtime.strftime("%Y-%m-%dT%H:%M"),
            humanize.naturaltime(dt_localtime),
        )
    return pretty_dt


def _get_last_result(job):
    last_result = PaastaColors.red("UNKNOWN")
    last_result_when = PaastaColors.red("UNKNOWN")
    fail_result = PaastaColors.red("Fail")
    ok_result = PaastaColors.green("OK")
    last_error = job.get("lastError")
    last_success = job.get("lastSuccess")

    if not last_error and not last_success:
        last_result = PaastaColors.yellow("New")
        last_result_when = "never"
    elif not last_error:
        last_result = ok_result
        last_result_when = isodate.parse_datetime(last_success)
    elif not last_success:
        last_result = fail_result
        last_result_when = isodate.parse_datetime(last_error)
    else:
        fail_dt = isodate.parse_datetime(last_error)
        ok_dt = isodate.parse_datetime(last_success)
        if ok_dt > fail_dt:
            last_result = ok_result
            last_result_when = ok_dt
        else:
            last_result = fail_result
            last_result_when = fail_dt

    pretty_last_result_when = _prettify_datetime(last_result_when)
    return (last_result, pretty_last_result_when)


def format_chronos_job_status(job, desired_state):
    """Given a job, returns a pretty-printed human readable output regarding
    the status of the job.

    :param job: dictionary of the job status
    :param desired_state: a pretty-formatted string representing the
    job's started/stopped state as set with paasta emergency-[stop|start], e.g.
    the result of get_desired_state_human()
    """
    disabled_state = _get_disabled_status(job)
    (last_result, last_result_when) = _get_last_result(job)
    return (
        "Status: %(disabled_state)s, %(desired_state)s\n"
        "Last: %(last_result)s (%(last_result_when)s)" % {
            "disabled_state": disabled_state,
            "desired_state": desired_state,
            "last_result": last_result,
            "last_result_when": last_result_when,
        }
    )


def status_chronos_jobs(jobs, job_config):
    """Returns a formatted string of the status of a list of chronos jobs

    :param jobs: list of dicts of chronos job info as returned by the chronos
        client
    :param job_config: dict containing configuration about these jobs as
        provided by chronos_tools.load_chronos_job_config().
    """
    if jobs == []:
        return "%s: chronos job is not set up yet" % PaastaColors.yellow("Warning")
    else:
        desired_state = job_config.get_desired_state_human()
        output = [format_chronos_job_status(job, desired_state) for job in jobs]
        return "\n".join(output)


def perform_command(command, service, instance, cluster, verbose, soa_dir):
    chronos_config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(chronos_config)
    complete_job_config = chronos_tools.create_complete_config(service, instance, soa_dir=soa_dir)
    job_id = complete_job_config['name']
    # We add SPACER to the end as an anchor to prevent catching
    # "my_service my_job_extra" when looking for "my_service my_job".
    job_pattern = r"^%s%s" % (chronos_tools.compose_job_id(service, instance), chronos_tools.SPACER)
    matching_jobs = chronos_tools.lookup_chronos_jobs(job_pattern, client, include_disabled=True)

    if command == "start":
        start_chronos_job(service, instance, job_id, client, cluster, complete_job_config, emergency=True)
    elif command == "stop":
        stop_chronos_job(service, instance, client, cluster, matching_jobs, emergency=True)
    elif command == "restart":
        restart_chronos_job(
            service,
            instance,
            job_id,
            client,
            cluster,
            matching_jobs,
            complete_job_config,
            emergency=True,
        )
    elif command == "status":
        # Setting up transparent cache for http API calls
        requests_cache.install_cache("paasta_serviceinit", backend="memory")
        job_config = chronos_tools.load_chronos_job_config(service, instance, cluster, soa_dir=soa_dir)
        print "Job id: %s" % job_id
        print status_chronos_jobs(matching_jobs, job_config)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    return 0

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
