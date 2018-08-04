#!/usr/bin/env python
"""Usage: ./check_chronos_jobs.py [options]

Check the status of chronos jobs. If the last run of the job was a failure, then
a CRITICAL event to sensu.

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
"""
import argparse
import logging
import sys
from datetime import datetime
from datetime import timedelta

import chronos
import isodate
import pysensu_yelp
import pytz

from paasta_tools import chronos_tools
from paasta_tools import monitoring_tools
from paasta_tools import utils
from paasta_tools.chronos_tools import compose_check_name_for_service_instance
from paasta_tools.chronos_tools import DEFAULT_SOA_DIR
from paasta_tools.chronos_tools import load_chronos_job_config


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description=(
        'Check the status of Chronos jobs, and report'
        'their status to Sensu.'
    ))
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    args = parser.parse_args()
    return args


def guess_realert_every(chronos_job_config):
    """Returns the job interval (in minutes) or None if the job has no interval.

    As we run check_chronos_jobs once a minute, we are going to skip
    all re-alert attempts until the next scheduled run of the job.

    This value is clamped by 1 day (1440 minutes) because we want a batch
    that has a big interval to realert once per day anyway.
    """
    interval = chronos_job_config.get_schedule_interval_in_seconds()
    return min(interval // 60, 24 * 60) if interval is not None else None


def compose_monitoring_overrides_for_service(chronos_job_config):
    """ Compose a group of monitoring overrides """
    monitoring_overrides = chronos_job_config.get_monitoring()
    if 'alert_after' not in monitoring_overrides:
        monitoring_overrides['alert_after'] = '15m'
    monitoring_overrides['check_every'] = '1m'
    monitoring_overrides['runbook'] = monitoring_tools.get_runbook(
        monitoring_overrides, chronos_job_config.service, soa_dir=chronos_job_config.soa_dir,
    )

    if 'realert_every' not in monitoring_overrides:
        guessed_realert_every = guess_realert_every(chronos_job_config)
        if guessed_realert_every is not None:
            monitoring_overrides['realert_every'] = monitoring_tools.get_realert_every(
                monitoring_overrides, chronos_job_config.service, soa_dir=chronos_job_config.soa_dir,
                monitoring_defaults=lambda x: {'realert_every': guessed_realert_every}.get(x),
            )
        else:
            monitoring_overrides['realert_every'] = monitoring_tools.get_realert_every(
                monitoring_overrides, chronos_job_config.service, soa_dir=chronos_job_config.soa_dir,
            )
    return monitoring_overrides


def check_chronos_job_name(service, instance):
    return compose_check_name_for_service_instance('check_chronos_jobs', service, instance)


def send_event(chronos_job_config, status_code, output):
    """Compose monitoring overrides and send the event to sensu.

    :param chronos_job_config: an instance of ChronosJobConfig
    :param status_code: Sensu status code
    :param output: An event message
    """
    monitoring_overrides = compose_monitoring_overrides_for_service(chronos_job_config)

    return monitoring_tools.send_event(
        service=chronos_job_config.service,
        check_name=check_chronos_job_name(
            chronos_job_config.service,
            chronos_job_config.instance,
        ),
        overrides=monitoring_overrides,
        status=status_code,
        output=add_realert_status(output, monitoring_overrides.get('realert_every')),
        soa_dir=chronos_job_config.soa_dir,
    )


def human_readable_time_interval(minutes):
    hours = minutes // 60
    minutes = minutes % 60
    return (hours > 0) * ('%sh' % hours) + (minutes > 0) * ('%sm' % minutes)


def add_realert_status(sensu_output, realert_every_in_minutes):
    if realert_every_in_minutes is None or realert_every_in_minutes == -1:
        return sensu_output
    else:
        interval_string = human_readable_time_interval(realert_every_in_minutes)
        return ("{}\n\nThis check realerts every {}."
                .format(sensu_output, interval_string))


def sensu_event_for_last_run_state(state):
    """
    Given a LastRunState, return a corresponding sensu event type.
    Will return None in the case that the job has not run yet,
    indicating that no Sensu alert should be sent.
    Raises ValueError in the case that the state is not valid.
    """
    if state is chronos_tools.LastRunState.Fail:
        return pysensu_yelp.Status.CRITICAL
    elif state is chronos_tools.LastRunState.Success:
        return pysensu_yelp.Status.OK
    elif state is chronos_tools.LastRunState.NotRun:
        return None
    else:
        raise ValueError('Expected valid LastRunState. Found %s' % state)


def build_service_job_mapping(client, configured_jobs):
    """
    :param client: A Chronos client used for getting the list of running jobs
    :param configured_jobs: A list of jobs configured in Paasta, i.e. jobs we
        expect to be able to find
    :returns: A dict of {(service, instance): last_chronos_job}
        where last_chronos_job is the latest job matching (service, instance)
        or None if there is no such job
    """
    service_job_mapping = {}
    all_chronos_jobs = client.list()
    for job in configured_jobs:
        # find all the jobs belonging to each service
        matching_jobs = chronos_tools.filter_chronos_jobs(
            jobs=all_chronos_jobs,
            service=job[0],
            instance=job[1],
            include_disabled=True,
            include_temporary=True,
        )
        matching_jobs = chronos_tools.sort_jobs(matching_jobs)
        # Only consider the most recent one
        service_job_mapping[job] = matching_jobs[0] if len(matching_jobs) > 0 else None
    return service_job_mapping


def message_for_status(status, chronos_job_config):
    """
    :param chronos_job_config: an instance of ChronosJobConfig
    """
    if status == pysensu_yelp.Status.CRITICAL:
        return (
            "Last run of job %(service)s%(separator)s%(instance)s failed.\n"
            "You can view the logs for the job with:\n"
            "\n"
            "    paasta logs -s %(service)s -i %(instance)s -c %(cluster)s\n"
            "\n"
            "If your job didn't manage to start up, you can view the stdout and stderr of your job using:\n"
            "\n"
            "    paasta status -s %(service)s -i %(instance)s -c %(cluster)s -vv\n"
            "\n"
            "If you need to rerun your job for the datetime it was started, you can do so with:\n"
            "\n"
            "    paasta rerun -s %(service)s -i %(instance)s -c %(cluster)s -d {datetime}\n"
            "\n"
            "See the docs on paasta rerun here:\n"
            "https://paasta.readthedocs.io/en/latest/workflow.html#re-running-failed-jobs for more details."
        ) % {
            'service': chronos_job_config.service,
            'instance': chronos_job_config.instance,
            'cluster': chronos_job_config.cluster,
            'separator': utils.SPACER,
        }
    elif status == pysensu_yelp.Status.UNKNOWN:
        return ('Last run of job %s%s%s Unknown' %
                (chronos_job_config.service, utils.SPACER, chronos_job_config.instance))
    elif status == pysensu_yelp.Status.OK:
        return ('Last run of job %s%s%s Succeeded' %
                (chronos_job_config.service, utils.SPACER, chronos_job_config.instance))
    elif status is None:
        return None
    else:
        raise ValueError('unknown sensu status: %s' % status)


def job_is_stuck(last_run_iso_time, interval_in_seconds, client, job_name):
    """Considers that the job is stuck when it hasn't run on time

    :param last_run_iso_time: ISO date and time of the last job run as a string
    :param interval_in_seconds: the job interval in seconds
    :param client: configured Chronos client
    :param job_name: Chronos job name
    :returns: True or False
    """
    if last_run_iso_time is None or interval_in_seconds is None:
        return False
    dt_next_run = isodate.parse_datetime(last_run_iso_time) + timedelta(seconds=interval_in_seconds)
    dt_now_utc = datetime.now(pytz.utc)
    if dt_next_run >= dt_now_utc:
        return False
    try:
        expected_runtime = min(int(client.job_stat(job_name)['histogram']['99thPercentile']), interval_in_seconds)
    except KeyError:
        log.debug("Can't get 99thPercentile for %s. "
                  "Assuming a runtime of %d seconds." % (job_name, interval_in_seconds))
        expected_runtime = interval_in_seconds
    return (dt_next_run + timedelta(seconds=expected_runtime) < dt_now_utc)


def message_for_stuck_job(
    service, instance, cluster, last_run_iso_time,
    interval_in_seconds, schedule, schedule_timezone,
):
    last_run_utc = isodate.parse_datetime(last_run_iso_time)
    if schedule_timezone is None:
        last_run_local = last_run_utc
        schedule_timezone = 'UTC'
    else:
        last_run_local = last_run_utc.astimezone(pytz.timezone(schedule_timezone)).isoformat()

    return (
        "Job %(service)s%(separator)s%(instance)s with schedule %(schedule)s (%(timezone)s) "
        "hasn't run since %(last_run)s, and is configured to run every "
        "%(interval)s.\n\n"
        "You can view the logs for the job with:\n"
        "\n"
        "    paasta logs -s %(service)s -i %(instance)s -c %(cluster)s\n"
        "\n"
    ) % {
        'service': service,
        'instance': instance,
        'cluster': cluster,
        'separator': utils.SPACER,
        'interval': human_readable_time_interval(interval_in_seconds // 60),
        'last_run': last_run_local,
        'schedule': schedule,
        'timezone': schedule_timezone,
    }


def sensu_message_status_for_jobs(chronos_job_config, chronos_job, client):
    """
    :param chronos_job_config: an instance of ChronosJobConfig
    :param client: configured Chronos client
    """
    if not chronos_job:
        if chronos_job_config.get_disabled():
            sensu_status = pysensu_yelp.Status.OK
            output = ("Job %s%s%s is disabled - ignoring status."
                      % (chronos_job_config.service, utils.SPACER, chronos_job_config.instance))
        else:
            sensu_status = pysensu_yelp.Status.WARNING
            output = ("Warning: %s%s%s isn't in chronos at all, "
                      "which means it may not be deployed yet"
                      % (chronos_job_config.service, utils.SPACER, chronos_job_config.instance))
    else:
        if chronos_job.get('disabled') and not chronos_tools.is_temporary_job(chronos_job):
            sensu_status = pysensu_yelp.Status.OK
            output = ("Job %s%s%s is disabled - ignoring status." %
                      (chronos_job_config.service, utils.SPACER, chronos_job_config.instance))
        else:
            last_run_time, state = chronos_tools.get_status_last_run(chronos_job)
            interval_in_seconds = chronos_job_config.get_schedule_interval_in_seconds()
            if job_is_stuck(last_run_time, interval_in_seconds, client, chronos_job['name']):
                sensu_status = pysensu_yelp.Status.CRITICAL
                output = message_for_stuck_job(
                    service=chronos_job_config.service,
                    instance=chronos_job_config.instance,
                    cluster=chronos_job_config.cluster,
                    last_run_iso_time=last_run_time,
                    interval_in_seconds=interval_in_seconds,
                    schedule=chronos_job_config.get_schedule(),
                    schedule_timezone=chronos_job_config.get_schedule_time_zone(),
                )
            else:
                sensu_status = sensu_event_for_last_run_state(state)
                output = message_for_status(sensu_status, chronos_job_config)
    return output, sensu_status


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)
    system_paasta_config = utils.load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    configured_jobs = chronos_tools.get_chronos_jobs_for_cluster(cluster, soa_dir=soa_dir)

    try:
        service_job_mapping = build_service_job_mapping(client, configured_jobs)

        for service_instance, chronos_job in service_job_mapping.items():
            service, instance = service_instance[0], service_instance[1]
            try:
                chronos_job_config = load_chronos_job_config(
                    service=service,
                    instance=instance,
                    cluster=cluster,
                    soa_dir=soa_dir,
                )
            except utils.NoDeploymentsAvailable:
                log.info("Skipping %s because no deployments are available" % service)
                continue
            sensu_output, sensu_status = sensu_message_status_for_jobs(
                chronos_job_config=chronos_job_config,
                chronos_job=chronos_job,
                client=client,
            )
            if sensu_status is not None:
                send_event(chronos_job_config, sensu_status, sensu_output)
    except (chronos.ChronosAPIError) as e:
        log.error("CRITICAL: Unable to contact Chronos! Error: %s" % e)
        sys.exit(2)


if __name__ == '__main__':
    main()
