#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import humanize
import isodate
import requests_cache

import chronos_tools
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.mesos_tools import status_mesos_tasks_verbose
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import _log
from paasta_tools.utils import PaastaColors


log = logging.getLogger('__main__')
logging.basicConfig()


# Calls the 'manual start' endpoint in Chronos (https://mesos.github.io/chronos/docs/api.html#manually-starting-a-job),
# running the job now regardless of its 'schedule' and 'disabled' settings. The job's 'schedule' is left unmodified.
def start_chronos_job(service, instance, job_id, client, cluster, job_config, emergency=False):
    name = PaastaColors.cyan(job_id)
    log_reason = PaastaColors.red("EmergencyStart") if emergency else "Brutal bounce"
    log_immediate_run = " and running it immediately" if not job_config["disabled"] else ""
    _log(
        service=service,
        line="%s: Sending job %s to Chronos%s" % (log_reason, name, log_immediate_run),
        component="deploy",
        level="event",
        cluster=cluster,
        instance=instance
    )
    client.update(job_config)
    # TODO fail or give some output/feedback to user that the job won't run immediately if disabled (PAASTA-1244)
    if not job_config["disabled"]:
        client.run(job_id)


def stop_chronos_job(service, instance, client, cluster, existing_jobs, emergency=False):
    log_reason = PaastaColors.red("EmergencyStop") if emergency else "Brutal bounce"
    for job in existing_jobs:
        name = PaastaColors.cyan(job["name"])
        _log(
            service=service,
            line="%s: Killing all tasks for job %s" % (log_reason, name),
            component="deploy",
            level="event",
            cluster=cluster,
            instance=instance
        )
        job["disabled"] = True
        client.update(job)
        client.delete_tasks(job["name"])


def restart_chronos_job(service, instance, job_id, client, cluster, matching_jobs, job_config, emergency=False):
    stop_chronos_job(service, instance, client, cluster, matching_jobs, emergency)
    start_chronos_job(service, instance, job_id, client, cluster, job_config, emergency)


def get_short_task_id(task_id):
    """Return just the Chronos-generated timestamp section of a Mesos task id."""
    return task_id.split(chronos_tools.MESOS_TASK_SPACER)[1]


def _format_config_hash(job):
    job_id = job.get("name", PaastaColors.red("UNKNOWN"))
    return job_id


def _format_disabled_status(job):
    status = PaastaColors.red("UNKNOWN")
    if job.get("disabled", False):
        status = PaastaColors.grey("Not scheduled")
    else:
        status = PaastaColors.green("Scheduled")
    return status


def _prettify_time(time):
    """Given a time, return a formatted representation of that time"""
    try:
        dt = isodate.parse_datetime(time)
    except isodate.isoerror.ISO8601Error:
        print "unable to parse datetime %s" % time
        raise
    dt_localtime = datetime_from_utc_to_local(dt)
    pretty_dt = "%s, %s" % (
        dt_localtime.strftime("%Y-%m-%dT%H:%M"),
        humanize.naturaltime(dt_localtime),
    )
    return pretty_dt


def _prettify_status(status):
    if status not in (
        chronos_tools.LastRunState.Fail,
        chronos_tools.LastRunState.Success,
        chronos_tools.LastRunState.NotRun,
    ):
        raise ValueError("Expected valid state, got %s" % status)
    if status == chronos_tools.LastRunState.Fail:
        return PaastaColors.red("Failed")
    elif status == chronos_tools.LastRunState.Success:
        return PaastaColors.green("OK")
    elif status == chronos_tools.LastRunState.NotRun:
        return PaastaColors.yellow("New")


def _format_last_result(job):
    time, status = chronos_tools.get_status_last_run(job)
    if status is chronos_tools.LastRunState.NotRun:
        formatted_time = "never"
    else:
        formatted_time = _prettify_time(time)
    return _prettify_status(status), formatted_time


def _format_schedule(job):
    schedule = job.get("schedule", PaastaColors.red("UNKNOWN"))
    epsilon = job.get("epsilon", PaastaColors.red("UNKNOWN"))
    formatted_schedule = "%s Epsilon: %s" % (schedule, epsilon)
    return formatted_schedule


def _format_command(job):
    command = job.get("command", PaastaColors.red("UNKNOWN"))
    return command


def _format_mesos_status(job, running_tasks):
    mesos_status = PaastaColors.red("UNKNOWN")
    num_tasks = len(running_tasks)
    if num_tasks == 0:
        mesos_status = PaastaColors.grey("Not running")
    elif num_tasks == 1:
        mesos_status = PaastaColors.yellow("Running")
    else:
        mesos_status = PaastaColors.red("Critical - %d tasks running (expected 1)" % num_tasks)
    return mesos_status


def format_chronos_job_status(job, running_tasks, verbose):
    """Given a job, returns a pretty-printed human readable output regarding
    the status of the job.

    :param job: dictionary of the job status
    :param running_tasks: a list of Mesos tasks associated with ``job``, e.g. the
                          result of ``mesos_tools.get_running_tasks_from_active_frameworks()``.

    """
    config_hash = _format_config_hash(job)
    disabled_state = _format_disabled_status(job)
    (last_result, formatted_time) = _format_last_result(job)
    schedule = _format_schedule(job)
    command = _format_command(job)
    mesos_status = _format_mesos_status(job, running_tasks)
    if verbose:
        mesos_status_verbose = status_mesos_tasks_verbose(job["name"], get_short_task_id)
        mesos_status = "%s\n%s" % (mesos_status, mesos_status_verbose)
    return (
        "Config:     %(config_hash)s\n"
        "  Status:   %(disabled_state)s\n"
        "  Last:     %(last_result)s (%(formatted_time)s)\n"
        "  Schedule: %(schedule)s\n"
        "  Command:  %(command)s\n"
        "  Mesos:    %(mesos_status)s" % {
            "config_hash": config_hash,
            "disabled_state": disabled_state,
            "last_result": last_result,
            "formatted_time": formatted_time,
            "schedule": schedule,
            "command": command,
            "mesos_status": mesos_status,
        }
    )


def status_chronos_jobs(jobs, job_config, verbose):
    """Returns a formatted string of the status of a list of chronos jobs

    :param jobs: list of dicts of chronos job info as returned by the chronos
        client
    :param job_config: dict containing configuration about these jobs as
        provided by chronos_tools.load_chronos_job_config().
    """
    if jobs == []:
        return "%s: chronos job is not set up yet" % PaastaColors.yellow("Warning")
    else:
        output = []
        desired_state = job_config.get_desired_state_human()
        output.append("Desired:    %s" % desired_state)
        for job in jobs:
            running_tasks = get_running_tasks_from_active_frameworks(job["name"])
            output.append(format_chronos_job_status(job, running_tasks, verbose))
        return "\n".join(output)


def perform_command(command, service, instance, cluster, verbose, soa_dir):
    chronos_config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(chronos_config)
    complete_job_config = chronos_tools.create_complete_config(service, instance, soa_dir=soa_dir)
    job_id = complete_job_config["name"]

    if command == "start":
        start_chronos_job(service, instance, job_id, client, cluster, complete_job_config, emergency=True)
    elif command == "stop":
        matching_jobs = chronos_tools.lookup_chronos_jobs(
            service=service,
            instance=instance,
            client=client,
            include_disabled=True,
        )
        stop_chronos_job(service, instance, client, cluster, matching_jobs, emergency=True)
    elif command == "restart":
        matching_jobs = chronos_tools.lookup_chronos_jobs(
            service=service,
            instance=instance,
            client=client,
            include_disabled=True,
        )
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
        # Verbose mode shows previous versions.
        if verbose:
            git_hash = None
            config_hash = None
        # Non-verbose shows only the version specified via
        # create_complete_config.
        else:
            (_, __, git_hash, config_hash) = chronos_tools.decompose_job_id(job_id)
        matching_jobs = chronos_tools.lookup_chronos_jobs(
            service=service,
            instance=instance,
            git_hash=git_hash,
            config_hash=config_hash,
            client=client,
            include_disabled=True,
        )
        sorted_matching_jobs = chronos_tools.sort_jobs(matching_jobs)
        job_config = chronos_tools.load_chronos_job_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
        )
        print status_chronos_jobs(sorted_matching_jobs, job_config, verbose)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    return 0

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
