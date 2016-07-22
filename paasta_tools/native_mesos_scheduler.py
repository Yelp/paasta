import logging

import mesos.interface
import service_configuration_lib
from mesos.interface import mesos_pb2
from mesos.native import MesosSchedulerDriver

import paasta_tools.mesos_tools
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import load_deployments_json

log = logging.getLogger(__name__)


class PaastaScheduler(mesos.interface.Scheduler):
    def __init__(self, service_name, instance_name, cluster, service_config=None):
        self.service_name = service_name
        self.instance_name = instance_name
        self.cluster = cluster
        self.tasks = []
        if service_config is not None:
            self.service_config = service_config
        else:
            raise Exception(repr(service_config))
            self.load_config()

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    def resourceOffers(self, driver, offers):
        for offer in offers:
            while self.task_fits(offer) and self.need_more_tasks():
                self.start_task(driver, offer)
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
        pass

    def periodic(self, driver):
        self.load_config()
        self.kill_tasks_if_necessary()

    def statusUpdate(self, driver, update):
        # update tasks
        driver.acknowledgeStatusUpdate(update)
        self.kill_tasks_if_necessary()

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
    log.info("Reading Chronos configuration file: %s/%s/paasta_native-%s.yaml" % (soa_dir, service, cluster))

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
