import logging
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

import paasta_tools.mesos_tools
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import get_services_for_cluster

log = logging.getLogger(__name__)


class PaastaScheduler(mesos.interface.Scheduler):
    def __init__(self, service_name, instance_name, cluster, service_config=None):
        self.service_name = service_name
        self.instance_name = instance_name
        self.cluster = cluster
        self.tasks = {}
        self.running = set()
        self.started = set()
        if service_config is not None:
            self.service_config = service_config
        else:
            self.load_config()

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value
        driver.reconcileTasks([])

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = self.start_task(driver, offer)
            if tasks:
                operation = mesos_pb2.Offer.Operation()
                operation.type = mesos_pb2.Offer.Operation.LAUNCH
                operation.launch.task_infos.extend(tasks)
                driver.acceptOffers([offer.id], [operation])
        # do something with the rest of the offers

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

    def need_more_tasks(self):
        """Returns whether we need to start more tasks."""
        return len(self.tasks) < self.service_config.get_instances()

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
        task = mesos_pb2.TaskInfo()
        task.container.type = mesos_pb2.ContainerInfo.DOCKER
        task.container.docker.image = self.service_config.get_docker_image()
        task.slave_id.value = offer.slave_id.value
        task.name = self.service_config.get_service()
        task.command.value = self.service_config.get_cmd()
        TOTAL_TASKS = self.service_config.config_dict['instances']
        TASK_CPUS = self.service_config.get_cpus()
        TASK_MEM = self.service_config.get_mem()
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM

        while len(self.started) + len(self.running) < TOTAL_TASKS and \
                remainingCpus >= TASK_CPUS and \
                remainingMem >= TASK_MEM:
            t = mesos_pb2.TaskInfo()
            t.MergeFrom(task)
            tid = uuid.uuid4().hex
            t.task_id.value = tid
            self.started.add(tid)
            tasks.append(t)
            self.tasks[tid] = (
                offer.slave_id, task.executor.executor_id)
            self.started.add(tid)

            remainingCpus -= TASK_CPUS
            remainingMem -= TASK_MEM

        return tasks

    def periodic(self, driver):
        self.load_config()
        self.kill_tasks_if_necessary()

    def statusUpdate(self, driver, update):
        # update tasks
        task_id = update.task_id.value
        state = update.state
        print "Task %s is in state %s" % \
            (task_id, mesos_pb2.TaskState.Name(state))
        print self.started, self.running
        if task_id in self.tasks:
            if state == mesos_pb2.TASK_RUNNING and task_id in self.started:
                self.started.remove(task_id)
                self.running.add(task_id)
            if state == mesos_pb2.TASK_LOST or \
                    state == mesos_pb2.TASK_KILLED or \
                    state == mesos_pb2.TASK_FAILED or \
                    state == mesos_pb2.TASK_FINISHED:
                if task_id in self.started:
                    self.started.remove(task_id)
                if task_id in self.running:
                    self.running.remove(update.task_id.value)
                self.tasks.remove(task_id)
        print self.started, self.running
        driver.acknowledgeStatusUpdate(update)
        # self.kill_tasks_if_necessary()

    def load_config(self):
        self.service_config = load_paasta_native_job_config(
            service=self.service_name,
            instance=self.instance_name,
            cluster=self.cluster,
        )


def create_driver(service, instance, scheduler, system_paasta_config):
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "paasta %s" % compose_job_id(service, instance)
    framework.failover_timeout = 604800
    framework.id.value = framework.name
    framework.checkpoint = True

    credential = mesos_pb2.Credential()
    credential.principal = system_paasta_config.get_paasta_native_config()['principal']
    credential.secret = system_paasta_config.get_paasta_native_config()['secret']

    framework.principal = system_paasta_config.get_paasta_native_config()['principal']
    implicitAcknowledgements = False

    driver = MesosSchedulerDriver(
        scheduler,
        framework,
        '%s:%d' % (paasta_tools.mesos_tools.get_mesos_leader(), paasta_tools.mesos_tools.MESOS_MASTER_PORT),
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
        raise UnknownPaastaNativeServiceError(
            'No job named "%s" in config file paasta_native-%s.yaml' % (instance, cluster)
        )
    branch_dict = {}
    if load_deployments:
        deployments_json = load_deployments_json(service, soa_dir=soa_dir)
        branch = get_paasta_branch(cluster=cluster, instance=instance)
        branch_dict = deployments_json.get_branch_dict(service, branch)
    return PaastaNativeServiceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=service_paasta_native_jobs[instance],
        branch_dict=branch_dict,
    )


class PaastaNativeServiceConfig(LongRunningServiceConfig):
    def __init__(self, service, instance, cluster, config_dict, branch_dict):
        super(PaastaNativeServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )


def get_paasta_native_jobs_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """A paasta_native-specific wrapper around utils.get_services_for_cluster

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, job_name)"""
    return get_services_for_cluster(cluster, 'paasta_native', soa_dir)


def main():
    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    drivers = []
    for service, instance in get_paasta_native_jobs_for_cluster(cluster=cluster):
        scheduler = PaastaScheduler(
            service_name=service,
            instance_name=instance,
            cluster=cluster
        )
        driver = create_driver(
            service=service,
            instance=instance,
            scheduler=scheduler,
            system_paasta_config=system_paasta_config(),
        )
        driver.start()
        driver.append(drivers)

    sleep(300)
