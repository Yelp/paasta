#!/usr/bin/env python

"""Usage: ./check_chronos_jobs.py [options]

Check the status of chronos jobs. If the last run of the job was a failure, then
a CRITICAL event to sensu.

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
"""

import argparse

import service_configuration_lib
import pysensu_yelp

from paasta_tools import monitoring_tools
from paasta_tools import chronos_tools
from paasta_tools import utils


def parse_args():
    parser = argparse.ArgumentParser(description='balh')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    args = parser.parse_args()
    return args


def compose_monitoring_overrides_for_service(cluster, service, instance, soa_dir):
    """ Compose a group of monitoring overrides """
    monitoring_overrides = chronos_tools.load_chronos_job_config(
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir
    ).get_monitoring()
    monitoring_overrides['alert_after'] = '2m'
    monitoring_overrides['check_every'] = '1m'
    monitoring_overrides['runbook'] = monitoring_tools.get_runbook(monitoring_overrides, service, soa_dir=soa_dir)
    return monitoring_overrides


def compose_check_name_for_job(service, instance):
    """Compose a sensu check name for a given job"""
    return 'check-chronos-jobs.%s%s%s' % (service, utils.SPACER, instance)


def send_event_to_sensu(service, instance, monitoring_overrides, soa_dir, status_code, message):
    check_name = compose_check_name_for_job(service, instance)

    monitoring_tools.send_event(
        service=service,
        check_name=check_name,
        overrides=monitoring_overrides,
        status=status_code,
        output=message,
        soa_dir=soa_dir,
    )


def last_run_state_for_jobs(jobs):
    """
    Map over a list of jobs to create a pair of (job, LasRunState).
    ``chronos_tools.get_status_last_run`` returns a pair of (time, state), of which
    we only need the latter([-1]).
    """
    return [(chronos_job, chronos_tools.get_status_last_run(chronos_job)[-1]) for chronos_job in jobs]


def sensu_event_for_last_run_state(state):
    """
    Given a LastRunState, return a corresponding sensu event type.
    Raise a ValueError in the case that the state is not valid.
    """
    if state not in (
        chronos_tools.LastRunState.Fail,
        chronos_tools.LastRunState.Success,
        chronos_tools.LastRunState.NotRun,
    ):
        raise ValueError('Expected valid LastRunState. Found %s' % state)

    if state is chronos_tools.LastRunState.Fail:
        return pysensu_yelp.Status.CRITICAL
    else:
        return pysensu_yelp.Status.OK


def build_service_job_mapping(client, configured_jobs):
    """
    :param client: A Chronos client used for getting the list of running jobs
    :param configured_jobs: A list of jobs configured in Paasta, i.e. jobs we
    expect to be able to find
    :returns: A dict of {(service, instance): [(chronos job, lastrunstate)]}
    where the chronos job is any with a matching (service, instance) in its
    name and disabled == False
    """
    service_job_mapping = {}
    for job in configured_jobs:
        # find all the jobs belonging to each service
        matching_jobs = chronos_tools.lookup_chronos_jobs(
            service=job[0],
            instance=job[1],
            client=client,
        )
        # filter the enabled jobs
        enabled = chronos_tools.filter_enabled_jobs(matching_jobs)
        # get the last run state for the job
        with_states = last_run_state_for_jobs(enabled)
        service_job_mapping[job] = with_states
    return service_job_mapping


def message_for_status(status, service, instance):
    if status not in (pysensu_yelp.Status.CRITICAL, pysensu_yelp.Status.OK, pysensu_yelp.Status.UNKNOWN):
        raise ValueError('unknown sensu status: %s' % status)
    if status == pysensu_yelp.Status.CRITICAL:
        return 'Last run of job %s%s%s Failed' % (service, utils.SPACER, instance)
    elif status == pysensu_yelp.Status.UNKNOWN:
        return 'Last run of job %s%s%s Unknown' % (service, utils.SPACER, instance)
    else:
        return 'Last run of job %s%s%s Succeded' % (service, utils.SPACER, instance)


def sensu_message_status_for_jobs(service, instance, job_state_pairs):
    if len(job_state_pairs) > 1:
        sensu_status = pysensu_yelp.Status.UNKNOWN
        output = (
            "Unknown: somehow there was more than one enabled job for %s%s%s. "
            "Talk to the PaaSTA team as this indicates a bug" % (service, utils.SPACER, instance)
        )
    elif len(job_state_pairs) == 0:
        sensu_status = pysensu_yelp.Status.WARNING
        output = (
            "Warning: %s%s%s isn't in chronos at all, "
            "which means it may not be deployed yet" % (service, utils.SPACER, instance)
        )
    else:
        state = job_state_pairs[0][1]
        sensu_status = sensu_event_for_last_run_state(state)
        output = message_for_status(sensu_status, service, instance)
    return output, sensu_status


def main(args):
    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)
    system_paasta_config = utils.load_system_paasta_config()

    # get those jobs listed in configs
    configured_jobs = chronos_tools.get_chronos_jobs_for_cluster(soa_dir=args.soa_dir)

    service_job_mapping = build_service_job_mapping(client, configured_jobs)
    for service_instance, job_state_pairs in service_job_mapping.items():
        service, instance = service_instance[0], service_instance[1]
        sensu_output, sensu_status = sensu_message_status_for_jobs(service, instance, job_state_pairs)
        monitoring_overrides = compose_monitoring_overrides_for_service(
            cluster=system_paasta_config.get_cluster(),
            service=service,
            instance=instance,
            soa_dir=args.soa_dir
        )
        send_event_to_sensu(
            service=service,
            instance=instance,
            monitoring_overrides=monitoring_overrides,
            status_code=sensu_status,
            message=sensu_output,
            soa_dir=args.soa_dir,
        )

if __name__ == '__main__':
    args = parse_args()
    main(args)
