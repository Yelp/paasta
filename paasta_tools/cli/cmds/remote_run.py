#!/usr/bin/env python
# Copyright 2015-2017 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import os

sys.path.insert(0, '/usr/lib/python2.7/site-packages/mesos')
from mesos.interface import Scheduler
from mesos.interface import mesos_pb2

from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig

from paasta_tools.native_mesos_scheduler import create_driver_with


class PaastaAdhocScheduler(Scheduler):

    def __init__ (self, command, service_config, system_paasta_config):
        self.command = command
        self.service_config = service_config
        self.system_paasta_config = system_paasta_config
        self.dry_run = dry_run


    def registered(self, driver, framework_id, master_info):
        paasta_print("Registered with framework id: {}".format(framework_id))


    def resourceOffers(self, driver, offers):
        if self.launched:
            return

        paasta_print("Recieved resource offers: {}".format([o.id.value for o in offers]))
        offer = offers[0]
        task = self.new_task(offer)
        paasta_print("Launching task {task} "
                     "using offer {offer}.".format(task=task.task_id.value,
                                                   offer=offer.id.value))

        self.launched = True
        if self.dry_run:
            paasta_print("Not doing anything in dry-run mode."
                         "Would have launched: {task}".format(task=task))
            self.status = 0
            driver.stop()
        else:
            driver.launchTasks(offer.id, tasks)


    def statusUpdate(self, driver, update):
        paasta_print("Mesos Scheduler: task %s is in state %d", update.task_id.value, update.state)

        if update.state == mesos_pb2.TASK_FINISHED:
            self.status = 0
        elif update.state == mesos_pb2.TASK_FAILED:
            # TODO: self.launched = False to retry?
            self.status = 1
        elif update.state == mesos_pb2.TASK_LOST or update.state == mesos_pb2.TASK_KILLED:
            self.launched = False

        if self.status != None:
            paasta_print("Task %s is finished.", update.task_id.value)
            for stream in mesos.cli.cluster.files(flist=['stdout','stderr'], fltr=update.task_id.value):
                print "Printing %s for task %s" % (stream[0].path, update.task_id.value)
                for line in stream[0].readlines():
                    print line
            driver.stop() # is this the right place to stop the driver?


    def new_task(self, offer):
        task = self.service_config.base_task(self.system_paasta_config)
        id = uuid.uuid4()
        task.task_id.value = str(id)
        task.slave_id.value = offer.slave_id.value

        if self.command:
            task.command.value = self.command

        return task


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'remote-run',
        help="Schedule Mesos to run adhoc command in context of a service",
        description=(
            "'paasta remote-run' is useful for running adhoc commands in context "
            "of a service's Docker image. The command will be scheduled on a "
            "Mesos cluster and stdout/stderr printed after execution is finished."
        ),
        epilog=(
            "Note: 'paasta remote-run' Mesos API that may require authentication."
        ),
    )
    list_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect',
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-c', '--cluster',
        help=("The name of the cluster you wish to run your task on. "
              "If omitted, uses the default cluster defined in the paasta remote-run configs"),
    ).completer = lazy_choices_completer(list_clusters)
    list_parser.add_argument(
        '-y', '--yelpsoa-config-root',
        dest='yelpsoa_config_root',
        help='A directory from which yelpsoa-configs should be read from',
        default=DEFAULT_SOA_DIR,
    )
    list_parser.add_argument(
        '--json-dict',
        help='When running dry run, output the arguments as a json dict',
        action='store_true',
        dest='dry_run_json_dict',
    )
    list_parser.add_argument(
        '-C', '--cmd',
        help=('Run Docker container with particular command, '
              'for example: "bash". By default will use the command or args specified by the '
              'soa-configs or what was specified in the Dockerfile'),
        required=False,
        default=None,
    )
    list_parser.add_argument(
        '-i', '--instance',
        help=("Simulate a docker run for a particular instance of the service, like 'main' or 'canary'"),
        required=False,
        default=None,
    ).completer = lazy_choices_completer(list_instances)
    list_parser.add_argument(
        '-v', '--verbose',
        help='Show Docker commands output',
        action='store_true',
        required=False,
        default=True,
    )
    list_parser.set_defaults(command=paasta_local_run)


def paasta_remote_run(args):
    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        paasta_print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This indicates"
                "PaaSTA is not configured locally on this host, and remote-run may not behave"
                "the same way it would behave on a server configured for PaaSTA."
            ),
            sep='\n',
        )
        system_paasta_config = SystemPaastaConfig({"volumes": []}, '/etc/paasta')

    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    cluster = args.cluster or system_paasta_config.get_local_run_config().get('default_cluster', None)

    if !cluster
        paasta_print(
            PaastaColors.red(
                "PaaSTA on this machine has not been configured with a default cluster."
                "Please pass one using '-c'."),
            sep='\n',
            file=sys.stderr,
        )
        return 1

    soa_dir = args.yelpsoa_config_root
    load_deployments = docker_hash is None
    dry_run = args.action == 'dry_run'
    instance = args.instance

    if instance is None:
        instance_type = 'adhoc'
        instance = 'interactive'
    else:
        instance_type = validate_service_instance(service, instance, cluster, soa_dir)

    command = shlex.split(args.cmd, posix=False) if args.cmd else None

    paasta_print('Scheduling a task on Mesos')

    service_config = load_paasta_native_job_config(
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=args.yelpsoa_config_root
    )
    scheduler = PaastaAdhocScheduler(
        command=command,
        service_config=service_config,
        system_paasta_config=system_paasta_config,
        dry_run=dry_run
    )
    driver = create_driver_with(
        # TODO: randomize framework name to get one per task?
        framework_name="paasta %s" % compose_job_id(service, instance),
        scheduler=scheduler,
        system_paasta_config=system_paasta_config,
        implicit_acks=True
    )
    driver.run()
    return scheduler.status
