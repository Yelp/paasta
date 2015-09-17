#!/usr/bin/env python
import logging
import sys

import isodate
import requests_cache

import chronos_tools
from paasta_tools.utils import _log
from paasta_tools.utils import PaastaColors


log = logging.getLogger("__main__")
log.addHandler(logging.StreamHandler(sys.stdout))


# Calls the 'manual start' endpoint in Chronos (https://mesos.github.io/chronos/docs/api.html#manually-starting-a-job),
# running the job now regardless of its 'schedule' and 'disabled' settings. The job's 'schedule' is left unmodified.
def start_chronos_job(service, instance, job_id, client, cluster, job_config):
    name = PaastaColors.cyan(job_id)
    _log(
        service_name=service,
        line="EmergencyStart: sending job %s to Chronos" % name,
        component='deploy',
        level='event',
        cluster=cluster,
        instance=instance
    )
    client.update(job_config)
    client.run(job_id)


def stop_chronos_job(service, instance, client, cluster, existing_jobs):
    for job in existing_jobs:
        name = PaastaColors.cyan(job['name'])
        _log(
            service_name=service,
            line="EmergencyStop: killing all tasks for job %s" % name,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance
        )
        job['disabled'] = True
        client.update(job)
        client.delete_tasks(job['name'])


def restart_chronos_job(service, instance, job_id, client, cluster, matching_jobs, job_config, immediate_start):
    stop_chronos_job(service, instance, client, cluster, matching_jobs)
    start_chronos_job(service, instance, job_id, client, cluster, job_config)


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


def status_chronos_job(jobs):
    """Returns a formatted string of the status of a list of chronos jobs

    :param jobs: list of dicts of chronos job info as returned by the chronos
        client
    """
    if jobs == []:
        return "%s: chronos job is not setup yet" % PaastaColors.yellow("Warning")
    else:
        output = [format_chronos_job_status(job) for job in jobs]
        return "\n".join(output)


def perform_command(command, service, instance, cluster, verbose, soa_dir):
    job_prefix = chronos_tools.compose_job_id(service, instance)
    job_config = chronos_tools.create_complete_config(service, instance, soa_dir=soa_dir)
    job_id = job_config['name']
    chronos_config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(chronos_config)
    # We add SPACER to the end as an anchor to prevent catching
    # "my_service my_job_extra" when looking for "my_service my_job".
    matching_jobs = chronos_tools.lookup_chronos_jobs(r'^%s%s' % (job_prefix, chronos_tools.SPACER),
                                                      client,
                                                      include_disabled=True)

    if command == "start":
        start_chronos_job(service, instance, job_id, client, cluster, job_config)
    elif command == "stop":
        stop_chronos_job(service, instance, client, cluster, matching_jobs)
    elif command == "restart":
        restart_chronos_job(service, instance, job_id, client, cluster, matching_jobs, job_config)
    elif command == "status":
        # Setting up transparent cache for http API calls
        requests_cache.install_cache("paasta_serviceinit", backend="memory")
        print "Job Id: %s" % job_id
        print status_chronos_job(matching_jobs)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    return 0

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
