#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
"""
Chronos Rerun: Designed to 'rerun' jobs with the right parameters.

``chronos_rerun`` will clone any given job, and give it a schedule to
run as soon as possible, and only once. If the job being rerun is a
'dependent job', that is, a job triggered by the successful running of 'parent'
jobs, then it is cloned without any children attached, and run as a regular
scheduled job.
"""
import argparse
import copy
import datetime

from paasta_tools import chronos_tools
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import paasta_print


def parse_args():
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', dest="verbose", default=False,
        help="Print out more output regarding the state of the service",
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=chronos_tools.DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-a', '--run-all-related-jobs', action='store_true', dest='run_all_related_jobs',
        default=False, help='Run all the parent-dependent related jobs',
    )
    parser.add_argument(
        '-f', '--force-disabled', action='store_true', dest='force_disabled',
        default=False, help='Run services that are configured to be disabled',
    )
    parser.add_argument('service_instance', help='Instance to operate on. Eg: example_service.main')
    parser.add_argument(
        'execution_date',
        help="The date the job should be rerun for. Expected in the format %%Y-%%m-%%dT%%H:%%M:%%S .",
    )
    args = parser.parse_args()
    return args


def modify_command_for_date(chronos_job, date, verbose):
    """
    Given a chronos job config, return a cloned job config where the command
    has been modified to reflect what it would have run as on
    a given date.

    :param chronos_job: a chronos job dictionary, as created by
        ``chronos_tools.create_complete_config``
    :param date: a ``datetime.datetime`` object.
    :returns chronos_job: a chronos_job dict with the command modified to
        interpolate in the context of the date provided.
    """
    current_command = chronos_job['command']
    if current_command is not None:
        chronos_job['command'] = chronos_tools.parse_time_variables(
            command=current_command,
            parse_time=date,
        )
    else:
        if verbose:
            job_name = ".".join(chronos_tools.decompose_job_id(chronos_job['name']))
            paasta_print(f'command in job {job_name} is empty - skipping formatting and depending on command in image')
    return chronos_job


def set_default_schedule(chronos_job):
    """
    Given a chronos job, return a new job identical to the first, but with the
    schedule replaced with one that will set the job to run now.

    :param chronos_job: a chronos job dictionary suitable for POSTing to
        Chronos
    :returns: the chronos_job parameter, with the 'schedule' field modified to
        a schedule for chronos to run the job now and only once. The interval field
        of the schedule is irrelevant, but required by Chronos.
    """
    chronos_job['schedule'] = 'R1//PT1M'
    return chronos_job


def get_tmp_naming_scheme_prefix(timestamp=None):
    timestamp = timestamp if timestamp else datetime.datetime.utcnow().isoformat()
    timestamp = timestamp.replace(':', '')
    timestamp = timestamp.replace('.', '')

    return '{}-{}'.format(
        chronos_tools.TMP_JOB_IDENTIFIER,
        timestamp,
    )


def set_tmp_naming_scheme(chronos_job, timestamp=None):
    """
    Given a chronos job, return a new job identical to the first, but with the
    name set to one which makes it identifiable as a temporary job.

    :param chronos_job: a chronos job suitable for POSTing to Chronos
    :param timestamp: timestamp to use for the generation of the tmp job name
    :returns: the chronos_job parameter, with the name of the job modified to
        allow it to be identified as a temporary job.
    """
    current_name = chronos_job['name']

    chronos_job['name'] = '{}{}{}'.format(
        get_tmp_naming_scheme_prefix(timestamp),
        chronos_tools.SPACER,
        current_name,
    )

    return chronos_job


def remove_parents(chronos_job):
    """
    Given a chronos job, return a new job identical to the first, but with the
    parents field removed

    :param chronos_job: a chronos_job suitable for POSTing to Chronos
    :returns: the chronos_job parameter, with the parents field of the job
        removed.
    """
    chronos_job.pop('parents', None)
    return chronos_job


def clone_job(chronos_job, timestamp=None, force_disabled=False):
    """
    Given a chronos job, create a 'rerun' clone that respects the parents relations.
    If the job has his own schedule it will be executed once and only once, and as soon as possible.

    :param chronos_job: a chronos job suitable for POSTing to Chronos
    :param timestamp: timestamp to use for the generation of the tmp job name
    :returns: the chronos_job parameter, modified to be submitted as a
        temporary clone used to rerun a job in the context of a given date.
    """
    clone = copy.deepcopy(chronos_job)
    job_type = chronos_tools.get_job_type(clone)

    # modify the name of the job
    clone = set_tmp_naming_scheme(clone, timestamp)

    # If the jobs is a dependent job rename the parents dependencies
    # in order to make this job dependent from the temporary clone of the parents
    if job_type == chronos_tools.JobType.Dependent:
        clone['parents'] = [
            '{}{}{}'.format(
                get_tmp_naming_scheme_prefix(timestamp),
                chronos_tools.SPACER,
                parent,
            )
            for parent in chronos_job['parents']
        ]
    else:
        # If the job is a scheduled one update the schedule to start it NOW
        clone = set_default_schedule(clone)

    # Set disabled to false if force_disabled is on
    if force_disabled:
        clone['disabled'] = False

    return clone


def main():
    args = parse_args()

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    service, instance = chronos_tools.decompose_job_id(args.service_instance)

    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)

    related_jobs = chronos_tools.get_related_jobs_configs(cluster, service, instance, soa_dir=args.soa_dir)
    if not related_jobs:
        error_msg = "No deployment found for {} in cluster {}. Has Jenkins run for it?".format(
            args.service_instance, cluster,
        )
        paasta_print(error_msg)
        raise NoDeploymentsAvailable

    if not args.run_all_related_jobs:
        # Strip all the configuration for the related services
        # those information will not be used by the rest of the flow
        related_jobs = {
            (service, instance): related_jobs[(service, instance)],
        }

    complete_job_configs = {}
    for (srv, inst) in related_jobs:
        try:
            complete_job_configs.update(
                {
                    (srv, inst): chronos_tools.create_complete_config(
                        service=srv,
                        job_name=inst,
                        soa_dir=args.soa_dir,
                    ),
                },
            )
        except (NoDeploymentsAvailable, NoDockerImageError) as e:
            error_msg = "No deployment found for {} in cluster {}. Has Jenkins run for it?".format(
                chronos_tools.compose_job_id(srv, inst), cluster,
            )
            paasta_print(error_msg)
            raise e
        except NoConfigurationForServiceError as e:
            error_msg = (
                "Could not read chronos configuration file for {} in cluster {}\nError was: {}".format(
                    chronos_tools.compose_job_id(srv, inst), cluster, str(e),
                )
            )
            paasta_print(error_msg)
            raise e
        except chronos_tools.InvalidParentError as e:
            raise e

    if not args.run_all_related_jobs:
        sorted_jobs = [(service, instance)]
    else:
        sorted_jobs = chronos_tools.topological_sort_related_jobs(cluster, service, instance, soa_dir=args.soa_dir)

    timestamp = datetime.datetime.utcnow().isoformat()

    chronos_to_add = []
    for (service, instance) in sorted_jobs:
        # complete_job_config is a formatted version of the job,
        # so the command is formatted in the context of 'now'
        # replace it with the 'original' cmd so it can be re rendered
        chronos_job_config = chronos_tools.load_chronos_job_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=args.soa_dir,
        )
        original_command = chronos_job_config.get_cmd()
        complete_job_config = complete_job_configs[(service, instance)]
        complete_job_config['command'] = original_command
        clone = clone_job(
            chronos_job=complete_job_config,
            timestamp=timestamp,
            force_disabled=args.force_disabled,
        )
        # modify the command to run commands for a given date
        clone = modify_command_for_date(
            chronos_job=clone,
            date=datetime.datetime.strptime(args.execution_date, "%Y-%m-%dT%H:%M:%S"),
            verbose=args.verbose,
        )

        if not args.run_all_related_jobs and chronos_tools.get_job_type(clone) == chronos_tools.JobType.Dependent:
            # If the job is a dependent job and we want to re-run only the specific instance
            # remove the parents and update the schedule to start the job as soon as possible
            clone = set_default_schedule(remove_parents(clone))

        chronos_to_add.append(clone)

    for job_to_add in chronos_to_add:
        client.add(job_to_add)


if __name__ == "__main__":
    main()
