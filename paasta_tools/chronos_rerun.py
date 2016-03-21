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
"""
Chronos Rerun: Designed to 'rerun' jobs with the right parameters.

If the job is a regular schedueled job, then chronos_rereun will use the
/scheduler/job endpoint to manually start a job now with the arguments set to
the values they would have been on the date specified.

In the case of a parent job, that is a scheduled job where other jobs have it
listed as a parent, chronos_rerun will create a new, temporary clone of the
scheduled job, without the child jobs. The job will then be rerun, without
affecting the child jobs

"""
import argparse
import copy
import datetime

import service_configuration_lib

from paasta_tools import chronos_tools
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError


def parse_args():
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument('-v', '--verbose', action='store_true', dest="verbose", default=False,
                        help="Print out more output regarding the state of the service")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('service_instance', help='Instance to operate on. Eg: example_service.main')
    parser.add_argument('execution_date', help="The date the job should be rerun for")
    args = parser.parse_args()
    return args


def modify_command_for_date(chronos_job, date):
    """
    Given a chronos job config, return a cloned job config where the command
    has been modified to reflect what it would have run as on
    a given date.
    """
    current_command = chronos_job['command']
    chronos_job['command'] = chronos_tools.parse_time_variables(current_command, date)
    return chronos_job


def set_default_schedule(chronos_job):
    """
    Given a chronos job, return a new job identical to the first, but with the
    schedule replaced with one that will set the job to run now.
    """
    chronos_job['schedule'] = 'R1//PT1M'
    return chronos_job


def set_tmp_naming_scheme(chronos_job):
    """
    Given a chronos job, return a new job identical to the first, but with the
    schedule replaced with one that will set the job to run now.
    """
    current_name = chronos_job['name']
    chronos_job['name'] = '%s%s%s' % (chronos_tools.TMP_JOB_IDENTIFIER, chronos_tools.SPACER, current_name)
    return chronos_job


def remove_parents(chronos_job):
    """
    Given a chronos job, return a new job identifcal to the first, but with the
    parents field removed
    """
    chronos_job.pop('parents', None)
    return chronos_job


def clone_job(chronos_job, date):
    """
    Given a chronos job, create a 'rerun' clone, that is due to run once and
    only once, and as soon as possible.
    """
    clone = copy.deepcopy(chronos_job)
    job_type = chronos_tools.get_job_type(clone)

    # modify the name of the job
    clone = set_tmp_naming_scheme(clone)

    # give the job a schedule for it to run now
    clone = set_default_schedule(clone)

    # if the job is a dependent job
    # then convert it to be a scheduled job
    # that should run now
    if job_type == chronos_tools.JobType.Dependent:
        clone = remove_parents(clone)

    # set the job to run now
    clone = set_default_schedule(clone)

    # modify the command to run commands
    # for a given date
    clone = modify_command_for_date(clone, date)
    return clone


def main():
    args = parse_args()

    cluster = load_system_paasta_config().get_cluster()

    service, instance = chronos_tools.decompose_job_id(args.service_instance)

    config = chronos_tools.load_chronos_config()
    client = chronos_tools.get_chronos_client(config)
    system_paasta_config = load_system_paasta_config()

    chronos_job_config = chronos_tools.load_chronos_job_config(
        service, instance, system_paasta_config.get_cluster(), soa_dir=args.soa_dir)

    try:
        complete_job_config = chronos_tools.create_complete_config(
            service=service,
            job_name=instance,
            soa_dir=args.soa_dir,
        )

    except (NoDeploymentsAvailable, NoDockerImageError) as e:
        error_msg = "No deployment found for %s in cluster %s. Has Jenkins run for it?" % (
            args.service_instance, cluster)
        print error_msg
        raise e
    except chronos_tools.UnknownChronosJobError as e:
        error_msg = (
            "Could not read chronos configuration file for %s in cluster %s\n" % (args.service_instance, cluster) +
            "Error was: %s" % str(e))
        print error_msg
        raise e
    except chronos_tools.InvalidParentError as e:
        raise e

    # complete_job_config is a formatted version
    # of the job, so the command is fornatted in the context
    # of 'now'
    # replace it with the 'original' cmd so it can be
    # re rendered
    original_command = chronos_job_config.get_cmd()
    complete_job_config['command'] = original_command
    clone = clone_job(complete_job_config, datetime.datetime.strptime(args.execution_date, "%Y-%m-%dT%H:%M:%S"))
    client.add(clone)


if __name__ == "__main__":
    main()
