#!/usr/bin/env python
"""Usage: ./check_chronos_jobs.py [options]

Check the status of chronos jobs. If the last run of the job was a failure, then
a CRITICAL event to sensu.

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
"""
import argparse

import pysensu_yelp

from paasta_tools import chronos_tools
from paasta_tools import monitoring_tools
from paasta_tools import utils
from paasta_tools.chronos_tools import compose_check_name_for_service_instance
from paasta_tools.chronos_tools import DEFAULT_SOA_DIR
from paasta_tools.chronos_tools import load_chronos_job_config


def parse_args():
    parser = argparse.ArgumentParser(description=('Check the status of Chronos jobs, and report'
                                                  'their status to Sensu.'))
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    args = parser.parse_args()
    return args


def compose_monitoring_overrides_for_service(chronos_job_config, soa_dir):
    """ Compose a group of monitoring overrides """
    monitoring_overrides = chronos_job_config.get_monitoring()
    if 'alert_after' not in monitoring_overrides:
        monitoring_overrides['alert_after'] = '2m'
    monitoring_overrides['check_every'] = '1m'
    monitoring_overrides['runbook'] = monitoring_tools.get_runbook(
        monitoring_overrides, chronos_job_config.service, soa_dir=soa_dir)
    return monitoring_overrides


def send_event(service, instance, monitoring_overrides, soa_dir, status_code, message):
    check_name = compose_check_name_for_service_instance('check_chronos_jobs', service, instance)

    monitoring_tools.send_event(
        service=service,
        check_name=check_name,
        overrides=monitoring_overrides,
        status=status_code,
        output=message,
        soa_dir=soa_dir,
    )


def compose_check_name_for_job(service, instance):
    """Compose a sensu check name for a given job"""
    return 'check-chronos-jobs.%s%s%s' % (service, utils.SPACER, instance)


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
        filtered = chronos_tools.filter_non_temporary_chronos_jobs(matching_jobs)
        with_states = last_run_state_for_jobs(filtered)
        service_job_mapping[job] = with_states
    return service_job_mapping


def message_for_status(status, service, instance, cluster):
    if status not in (pysensu_yelp.Status.CRITICAL, pysensu_yelp.Status.OK, pysensu_yelp.Status.UNKNOWN):
        raise ValueError('unknown sensu status: %s' % status)
    if status == pysensu_yelp.Status.CRITICAL:
        return (
            "Last run of job %(service)s%(separator)s%(instance)s failed.\n"
            "You can view the logs for the job with:\n"
            "\n"
            "    paasta logs -s %(service)s -c %(cluster)s\n"
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
            'service': service,
            'instance': instance,
            'cluster': cluster,
            'separator': utils.SPACER
        }
    elif status == pysensu_yelp.Status.UNKNOWN:
        return 'Last run of job %s%s%s Unknown' % (service, utils.SPACER, instance)
    else:
        return 'Last run of job %s%s%s Succeded' % (service, utils.SPACER, instance)


def sensu_message_status_for_jobs(chronos_job_config, service, instance, cluster, job_state_pairs):
    if len(job_state_pairs) > 1:
        sensu_status = pysensu_yelp.Status.UNKNOWN
        output = (
            "Unknown: somehow there was more than one enabled job for %s%s%s.\n"
            "Talk to the PaaSTA team as this indicates a bug." % (service, utils.SPACER, instance)
        )
    elif len(job_state_pairs) == 0:
        if chronos_job_config.get_disabled():
            sensu_status = pysensu_yelp.Status.OK
            output = "Job %s%s%s is disabled - ignoring status." % (service, utils.SPACER, instance)
        else:
            sensu_status = pysensu_yelp.Status.WARNING
            output = (
                "Warning: %s%s%s isn't in chronos at all, "
                "which means it may not be deployed yet" % (service, utils.SPACER, instance)
            )
    else:
        if job_state_pairs[0][0].get('disabled') is True:
            sensu_status = pysensu_yelp.Status.OK
            output = "Job %s%s%s is disabled - ignoring status." % (service, utils.SPACER, instance)
        else:
            state = job_state_pairs[0][1]
            sensu_status = sensu_event_for_last_run_state(state)
            output = message_for_status(sensu_status, service, instance, cluster)
    return output, sensu_status


def main(args):
    soa_dir = args.soa_dir
    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)
    system_paasta_config = utils.load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    configured_jobs = chronos_tools.get_chronos_jobs_for_cluster(cluster, soa_dir=soa_dir)

    service_job_mapping = build_service_job_mapping(client, configured_jobs)
    for service_instance, job_state_pairs in service_job_mapping.items():
        service, instance = service_instance[0], service_instance[1]
        chronos_job_config = load_chronos_job_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
        )
        sensu_output, sensu_status = sensu_message_status_for_jobs(
            chronos_job_config=chronos_job_config,
            service=service,
            instance=instance,
            cluster=cluster,
            job_state_pairs=job_state_pairs
        )
        monitoring_overrides = compose_monitoring_overrides_for_service(
            chronos_job_config=chronos_job_config,
            soa_dir=soa_dir
        )
        send_event(
            service=service,
            instance=instance,
            monitoring_overrides=monitoring_overrides,
            status_code=sensu_status,
            message=sensu_output,
            soa_dir=soa_dir,
        )

if __name__ == '__main__':
    args = parse_args()
    main(args)
