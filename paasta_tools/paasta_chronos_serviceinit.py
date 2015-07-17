#!/usr/bin/env python
import argparse
import logging
import sys

import requests_cache

from paasta_tools.utils import PaastaColors
import marathon_tools
import chronos_tools


log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


def parse_args():
    parser = argparse.ArgumentParser(description='Runs status an Chronos job.')
    parser.add_argument('-d', '--debug', action='store_true', dest="debug", default=False,
                        help="Output debug logs regarding files, connections, etc")
    parser.add_argument('service_instance', help='Instance to operate on. Eg: example_service.job')
    command_choices = ['status']
    parser.add_argument('command', choices=command_choices, help='Command to run. Eg: status')
    args = parser.parse_args()
    return args


def format_chronos_job_status(job):
    """Given a job, returns a pretty-printed human readable output regarding
    the status of the job.
    Currently only reports whether the job is disabled or enabled

    :param job: dictionary of the job status
    """
    if job.get('disabled', False):
        status = PaastaColors.red("Disabled")
    else:
        status = PaastaColors.green("Enabled")
    return "Status: %s" % status


def status_chronos_job(service, instance, job_id, all_jobs):
    """Returns a formatted string of the status of a chronos job

    :param service: Name of the service, like example_service
    :param instance: name of the job, like nightly_batch
    :param job_id: the idenfier of the job in the chronos api
    :param all_jobs: list of all the jobs from chronos
    """
    our_jobs = [job for job in all_jobs if job['name'] == job_id]
    if our_jobs == []:
        return "%s: %s is not setup yet" % (PaastaColors.yellow("Warning"), job_id)
    elif len(our_jobs) == 1:
        our_job = our_jobs[0]
        return format_chronos_job_status(our_job)
    else:
        return ("Error: there are multiple jobs. Only expecting 1 job.\n"
                "This should not happen:\n%s" % str(our_jobs))


def main():
    args = parse_args()
    if args.debug:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    command = args.command
    service_instance = args.service_instance
    (service, instance) = service_instance.split(marathon_tools.ID_SPACER)

    job_id = chronos_tools.get_job_id(service, instance)
    client = chronos_tools.get_chronos_client()

    if command == 'status':
        # Setting up transparent cache for http API calls
        requests_cache.install_cache('paasta_serviceinit', backend='memory')

        all_jobs = client.list()
        print status_chronos_job(service, instance, job_id, all_jobs)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    sys.exit(0)


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
