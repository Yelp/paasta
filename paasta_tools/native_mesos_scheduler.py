#!/usr/bin/env python
# Without this, the import of mesos.interface breaks because paasta_tools.mesos exists
from __future__ import absolute_import

import argparse
import binascii
import logging
import sys
import time
import uuid
from time import sleep

import mesos.interface
import service_configuration_lib
from mesos.interface import mesos_pb2
try:
    from mesos.native import MesosSchedulerDriver
except ImportError:
    # mesos.native is tricky to install - it's not available on pypi. When building the dh-virtualenv or running itests,
    # we hack together a wheel from the mesos.native module installed by the mesos debian package.
    # This try/except allows us to unit-test this module.
    pass

from paasta_tools import mesos_tools
from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import get_services_for_cluster

from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_docker_url


log = logging.getLogger(__name__)

MESOS_TASK_SPACER = '.'

# Bring these into local scope for shorter lines of code.
TASK_STAGING = mesos_pb2.TASK_STAGING
TASK_STARTING = mesos_pb2.TASK_STARTING
TASK_RUNNING = mesos_pb2.TASK_RUNNING

TASK_KILLING = mesos_pb2.TASK_KILLING
TASK_FINISHED = mesos_pb2.TASK_FINISHED
TASK_FAILED = mesos_pb2.TASK_FAILED
TASK_KILLED = mesos_pb2.TASK_KILLED
TASK_LOST = mesos_pb2.TASK_LOST
TASK_ERROR = mesos_pb2.TASK_ERROR

LIVE_TASK_STATES = (TASK_STAGING, TASK_STARTING, TASK_RUNNING)


class MesosTaskParameters(object):
    def __init__(
        self,
        health=None,
        mesos_task_state=None,
        is_draining=False,
        is_healthy=False,
    ):
        self.health = health
        self.mesos_task_state = mesos_task_state

        self.is_draining = is_draining
        self.is_healthy = is_healthy


class PaastaScheduler(mesos.interface.Scheduler):
    def __init__(self, service_name, instance_name, cluster, system_paasta_config, soa_dir=DEFAULT_SOA_DIR,
                 service_config=None, reconcile_backoff=30):
        self.service_name = service_name
        self.instance_name = instance_name
        self.cluster = cluster
        self.system_paasta_config = system_paasta_config
        self.soa_dir = soa_dir
        self.tasks_with_flags = {}

        self.reconcile_start_time = float('inf')  # don't accept resources until we reconcile.
        self.reconcile_backoff = reconcile_backoff  # wait this long after starting a reconcile before accepting offers.
        self.framework_id = None  # Gets set when registered() is called

        if service_config is not None:
            self.service_config = service_config
            self.recreate_drain_method()
        else:
            self.load_config()

    def registered(self, driver, frameworkId, masterInfo):
        self.framework_id = frameworkId.value
        print "Registered with framework ID %s" % frameworkId.value

        self.reconcile_start_time = time.time()
        driver.reconcileTasks([])

    def resourceOffers(self, driver, offers):
        if self.within_reconcile_backoff():
            print "Declining all offers since we started reconciliation too recently"
            for offer in offers:
                driver.declineOffer(offer.id)
            return

        for offer in offers:
            tasks = self.start_task(driver, offer)
            if tasks:
                operation = mesos_pb2.Offer.Operation()
                operation.type = mesos_pb2.Offer.Operation.LAUNCH
                operation.launch.task_infos.extend(tasks)
                driver.acceptOffers([offer.id], [operation])
            else:
                driver.declineOffer(offer.id)

    def task_fits(self, offer):
        """Checks whether the offer is big enough to fit the tasks"""
        needed_resources = {
            "cpus": self.service_config.get_cpus(),
            "mem": self.service_config.get_mem(),
            "disk": self.service_config.get_disk(),
        }
        for resource in offer.resources:
            try:
                if resource.scalar.value < needed_resources[resource.name]:
                    return False
            except KeyError:
                pass

        return True

    def need_more_tasks(self, name):
        """Returns whether we need to start more tasks."""
        num_have = 0
        for task, parameters in self.tasks_with_flags.iteritems():
            if self.is_task_new(name, task) and (parameters.mesos_task_state in LIVE_TASK_STATES):
                num_have += 1
        return num_have < self.service_config.get_instances()

    def get_new_tasks(self, name, tasks):
        return set([tid for tid in tasks if self.is_task_new(name, tid)])

    def get_old_tasks(self, name, tasks):
        return set([tid for tid in tasks if not self.is_task_new(name, tid)])

    def is_task_new(self, name, tid):
        return tid.startswith("%s." % name)

    def start_task(self, driver, offer):
        """Starts a task using the offer, and subtracts any resources used from the offer."""
        tasks = []
        offerCpus = 0
        offerMem = 0
        for resource in offer.resources:
            if resource.name == "cpus":
                offerCpus += resource.scalar.value
            elif resource.name == "mem":
                offerMem += resource.scalar.value
        remainingCpus = offerCpus
        remainingMem = offerMem

        base_task = self.service_config.base_task(self.system_paasta_config)
        base_task.slave_id.value = offer.slave_id.value

        task_mem = self.service_config.get_mem()
        task_cpus = self.service_config.get_cpus()

        while self.need_more_tasks(base_task.name) and \
                remainingCpus >= task_cpus and \
                remainingMem >= task_mem:
            t = mesos_pb2.TaskInfo()
            t.MergeFrom(base_task)
            tid = "%s.%s" % (t.name, uuid.uuid4().hex)
            t.task_id.value = tid
            tasks.append(t)
            self.tasks_with_flags.setdefault(
                tid,
                MesosTaskParameters(
                    health=None,
                    mesos_task_state=TASK_STAGING,
                ),
            )

            remainingCpus -= task_cpus
            remainingMem -= task_mem

        return tasks

    def within_reconcile_backoff(self):
        return time.time() - self.reconcile_backoff < self.reconcile_start_time

    def periodic(self, driver):
        self.periodic_was_called = True  # Used for testing.
        if not self.within_reconcile_backoff():
            driver.reviveOffers()

        self.load_config()
        self.kill_tasks_if_necessary(driver)

    def statusUpdate(self, driver, update):
        # update tasks
        task_id = update.task_id.value
        state = update.state
        print "Task %s is in state %s" % \
            (task_id, mesos_pb2.TaskState.Name(state))
        if state == TASK_LOST or \
                state == TASK_KILLED or \
                state == TASK_FAILED or \
                state == TASK_FINISHED:

            self.tasks_with_flags.pop(task_id)
        else:
            task_params = self.tasks_with_flags.setdefault(task_id, MesosTaskParameters(health=None))
            task_params.mesos_task_state = state

        driver.acknowledgeStatusUpdate(update)
        self.kill_tasks_if_necessary(driver)

    def make_healthiness_sorter(self, base_task_name):
        def healthiness_score(task_id):
            """Return a tuple that can be used as a key for sorting, that expresses our desire to keep this task around.
            Higher values (things that sort later) are more desirable."""
            params = self.tasks_with_flags[task_id]

            state_score = {
                TASK_KILLING: 0,
                TASK_FINISHED: 0,
                TASK_FAILED: 0,
                TASK_KILLED: 0,
                TASK_LOST: 0,
                TASK_ERROR: 0,
                TASK_STAGING: 1,
                TASK_STARTING: 2,
                TASK_RUNNING: 3,
            }[params.mesos_task_state]

            # unhealthy tasks < healthy
            # staging < starting < running
            # old < new
            return (params.is_healthy, state_score, self.is_task_new(base_task_name, task_id))
        return healthiness_score

    def kill_tasks_if_necessary(self, driver):
        base_task = self.service_config.base_task(self.system_paasta_config)

        new_tasks = self.get_new_tasks(base_task.name, self.tasks_with_flags.keys())
        happy_new_tasks = self.get_happy_tasks(new_tasks)

        desired_instances = self.service_config.get_instances()
        # this puts the most-desired tasks first. I would have left them in order of bad->good and used
        # new_tasks_by_desirability[:-desired_instances] instead, but list[:-0] is an empty list, rather than the full
        # list.
        new_tasks_by_desirability = sorted(list(new_tasks), key=self.make_healthiness_sorter(base_task.name),
                                           reverse=True)
        new_tasks_to_kill = new_tasks_by_desirability[desired_instances:]

        old_tasks = self.get_old_tasks(base_task.name, self.tasks_with_flags.keys())
        old_happy_tasks = self.get_happy_tasks(old_tasks)
        old_draining_tasks = self.get_draining_tasks(old_tasks)
        old_unhappy_tasks = set(old_tasks) - set(old_happy_tasks) - set(old_draining_tasks)

        actions = bounce_lib.crossover_bounce(
            new_config={"instances": desired_instances},
            new_app_running=True,
            happy_new_tasks=happy_new_tasks,
            old_app_live_happy_tasks=self.group_tasks_by_version(old_happy_tasks + new_tasks_to_kill),
            old_app_live_unhappy_tasks=self.group_tasks_by_version(old_unhappy_tasks),
        )

        for task in set(new_tasks) - set(actions['tasks_to_drain']):
            self.undrain_task(task)
        for task in actions['tasks_to_drain']:
            self.drain_task(task)

        for task in [task for task, parameters in self.tasks_with_flags.iteritems() if parameters.is_draining]:
            if self.drain_method.is_safe_to_kill(DrainTask(id=task)):
                self.kill_task(driver, task)

    def get_happy_tasks(self, tasks):
        """Filter a list of tasks to those that are happy."""
        happy_tasks = []
        for task in tasks:
            params = self.tasks_with_flags[task]
            if params.mesos_task_state == TASK_RUNNING and not params.is_draining:
                happy_tasks.append(task)
        return happy_tasks

    def get_draining_tasks(self, tasks):
        """Filter a list of tasks to those that are draining."""
        return [t for t, p in self.tasks_with_flags.iteritems() if p.is_draining]

    def undrain_task(self, task):
        self.drain_method.stop_draining(DrainTask(id=task))
        self.tasks_with_flags[task].is_draining = False

    def drain_task(self, task):
        self.drain_method.drain(DrainTask(id=task))
        self.tasks_with_flags[task].is_draining = True

    def kill_task(self, driver, task):
        tid = mesos_pb2.TaskID()
        tid.value = task
        driver.killTask(tid)
        self.tasks_with_flags[task].mesos_task_state = TASK_KILLING

    def group_tasks_by_version(self, task_ids):
        d = {}
        for task_id in task_ids:
            version = task_id.rsplit('.', 1)[0]
            d.setdefault(version, []).append(task_id)
        return d

    def load_config(self):
        service_configuration_lib._yaml_cache = {}
        self.service_config = load_paasta_native_job_config(
            service=self.service_name,
            instance=self.instance_name,
            cluster=self.cluster,
            soa_dir=self.soa_dir,
        )
        self.recreate_drain_method()

    def recreate_drain_method(self):
        """Re-instantiate self.drain_method. Should be called after self.service_config changes."""
        self.drain_method = drain_lib.get_drain_method(
            name=self.service_config.get_drain_method(self.service_config.service_namespace_config),
            service=self.service_name,
            instance=self.instance_name,
            nerve_ns=self.service_config.get_nerve_namespace(),
            **self.service_config.get_drain_method_params(self.service_config.service_namespace_config)
        )


class DrainTask(object):
    def __init__(self, id):
        self.id = id


def find_existing_id_if_exists_or_gen_new(name):
    for framework in mesos_tools.get_all_frameworks(active_only=True):
        if framework.name == name:
            return framework.id
    else:
        return uuid.uuid4().hex


def create_driver(service, instance, scheduler, system_paasta_config):
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "paasta %s" % compose_job_id(service, instance)
    framework.failover_timeout = 604800
    framework.id.value = find_existing_id_if_exists_or_gen_new(framework.name)
    framework.checkpoint = True

    credential = mesos_pb2.Credential()
    credential.principal = system_paasta_config.get_paasta_native_config()['principal']
    credential.secret = system_paasta_config.get_paasta_native_config()['secret']

    framework.principal = system_paasta_config.get_paasta_native_config()['principal']
    implicitAcknowledgements = False

    driver = MesosSchedulerDriver(
        scheduler,
        framework,
        '%s:%d' % (mesos_tools.get_mesos_leader(), mesos_tools.MESOS_MASTER_PORT),
        implicitAcknowledgements,
        credential
    )
    return driver


class UnknownPaastaNativeServiceError(Exception):
    pass


def read_paasta_native_jobs_for_service(service, cluster, soa_dir=DEFAULT_SOA_DIR):
    paasta_native_conf_file = 'paasta_native-%s' % cluster
    log.info("Reading paasta_native configuration file: %s/%s/paasta_native-%s.yaml" % (soa_dir, service, cluster))

    return service_configuration_lib.read_extra_service_information(
        service,
        paasta_native_conf_file,
        soa_dir=soa_dir
    )


def load_paasta_native_job_config(service, instance, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR):
    service_paasta_native_jobs = read_paasta_native_jobs_for_service(service, cluster, soa_dir=soa_dir)
    if instance not in service_paasta_native_jobs:
        filename = '%s/%s/paasta_native-%s.yaml' % (soa_dir, service, cluster)
        raise UnknownPaastaNativeServiceError(
            'No job named "%s" in config file %s: \n%s' % (instance, filename, open(filename).read())
        )
    branch_dict = {}
    if load_deployments:
        deployments_json = load_deployments_json(service, soa_dir=soa_dir)
        branch = get_paasta_branch(cluster=cluster, instance=instance)
        branch_dict = deployments_json.get_branch_dict(service, branch)

    service_config = PaastaNativeServiceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=service_paasta_native_jobs[instance],
        branch_dict=branch_dict,
    )

    service_namespace_config = load_service_namespace_config(service, service_config.get_nerve_namespace(),
                                                             soa_dir=soa_dir)
    service_config.service_namespace_config = service_namespace_config

    return service_config


class PaastaNativeServiceConfig(LongRunningServiceConfig):
    def __init__(self, service, instance, cluster, config_dict, branch_dict, service_namespace_config=None):
        super(PaastaNativeServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )
        # service_namespace_config may be omitted/set to None at first, then set after initializing. e.g. we do this in
        # load_paasta_native_job_config so we can call get_nerve_namespace() to figure out what SNC to read.
        # It may also be set to None if this service is not in nerve.
        if service_namespace_config is not None:
            self.service_namespace_config = service_namespace_config
        else:
            self.service_namespace_config = ServiceNamespaceConfig()

    def get_instances(self):
        if self.get_desired_state() == 'start':
            return self.config_dict.get('instances')
        else:
            return 0

    def task_name(self, base_task):
        code_sha = get_code_sha_from_dockerurl(base_task.container.docker.image)

        filled_in_task = mesos_pb2.TaskInfo()
        filled_in_task.MergeFrom(base_task)
        filled_in_task.name = ""
        filled_in_task.task_id.value = ""
        filled_in_task.slave_id.value = ""

        config_hash = get_config_hash(
            binascii.b2a_base64(filled_in_task.SerializeToString()),
            force_bounce=self.get_force_bounce(),
        )

        return compose_job_id(
            self.service,
            self.instance,
            git_hash=code_sha,
            config_hash=config_hash,
            spacer=MESOS_TASK_SPACER,
        )

    def base_task(self, system_paasta_config):
        """Return a TaskInfo protobuf with all the fields corresponding to the configuration filled in. Does not
        include task.slave_id or a task.id; those need to be computed separately."""
        task = mesos_pb2.TaskInfo()
        task.container.type = mesos_pb2.ContainerInfo.DOCKER
        task.container.docker.image = get_docker_url(system_paasta_config.get_docker_registry(),
                                                     self.get_docker_image())
        task.command.value = self.get_cmd()
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.get_cpus()
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.get_mem()

        task.name = self.task_name(task)

        return task


def get_paasta_native_jobs_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """A paasta_native-specific wrapper around utils.get_services_for_cluster

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, job_name)"""
    return get_services_for_cluster(cluster, 'paasta_native', soa_dir)


def parse_args(argv):
    parser = argparse.ArgumentParser(description='Runs native paasta mesos scheduler.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR", default=DEFAULT_SOA_DIR)
    parser.add_argument('--stay-alive-seconds', dest="stay_alive_seconds", type=int, default=300)
    parser.add_argument('--periodic-interval', dest="periodic_interval", type=int, default=30)
    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    drivers = []
    schedulers = []
    for service, instance in get_paasta_native_jobs_for_cluster(cluster=cluster, soa_dir=args.soa_dir):
        scheduler = PaastaScheduler(
            service_name=service,
            instance_name=instance,
            cluster=cluster,
            system_paasta_config=system_paasta_config,
            soa_dir=args.soa_dir,
        )
        schedulers.append(scheduler)

        driver = create_driver(
            service=service,
            instance=instance,
            scheduler=scheduler,
            system_paasta_config=system_paasta_config,
        )
        driver.start()
        drivers.append(driver)

    end_time = time.time() + args.stay_alive_seconds
    while time.time() < end_time:
        sleep(args.periodic_interval)
        for scheduler, driver in zip(schedulers, drivers):
            scheduler.periodic(driver)

    return schedulers

if __name__ == '__main__':
    main(sys.argv[1:])
