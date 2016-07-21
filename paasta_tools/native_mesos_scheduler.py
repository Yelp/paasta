import logging

import mesos.interface
import service_configuration_lib

from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import load_deployments_json

log = logging.getLogger(__name__)


class PaastaScheduler(mesos.interface.Scheduler):
    def __init__(self, service_name, instance_name, cluster):
        self.service_name = service_name
        self.instance_name = instance_name
        self.cluster = cluster
        self.tasks = []
        self.load_config()

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    def resourceOffers(self, driver, offers):
        for offer in offers:
            while self.task_fits(offer) and self.need_more_tasks():
                self.start_task(driver, offer)
        # do something with the rest of the offers

    def periodic(self, driver):
        self.load_config()
        self.kill_tasks_if_necessary()

    def statusUpdate(self, driver, status):
        # update tasks
        self.kill_tasks_if_necessary()

    def load_config(self):
        self.service_config = load_paasta_native_job_config(
            service=self.service_name,
            instance=self.instance_name,
            cluster=self.cluster,
        )


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
