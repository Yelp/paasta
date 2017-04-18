from __future__ import absolute_import
from __future__ import unicode_literals

import time

from paasta_tools import marathon_tools
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.setup_marathon_job import deploy_marathon_service


class PaastaDeployWorker(PaastaThread):
    def __init__(self, worker_number, inbox_q, bounce_q):
        super(PaastaDeployWorker, self).__init__()
        self.daemon = True
        self.name = "Worker{}".format(worker_number)
        self.inbox_q = inbox_q
        self.bounce_q = bounce_q
        self.setup()

    def setup(self):
        self.marathon_config = marathon_tools.load_marathon_config()
        self.marathon_client = marathon_tools.get_marathon_client(self.marathon_config.get_url(),
                                                                  self.marathon_config.get_username(),
                                                                  self.marathon_config.get_password())

    def run(self):
        self.log.info("{} starting up".format(self.name))
        while True:
            service_instance = self.bounce_q.get()
            self.log.info("{} processing {}".format(self.name, service_instance))
            marathon_apps = marathon_tools.get_all_marathon_apps(self.marathon_client, embed_failures=True)
            return_code, bounce_again_in_seconds = deploy_marathon_service(service=service_instance.service,
                                                                           instance=service_instance.instance,
                                                                           client=self.marathon_client,
                                                                           soa_dir=marathon_tools.DEFAULT_SOA_DIR,
                                                                           marathon_config=self.marathon_config,
                                                                           marathon_apps=marathon_apps)
            self.log.info("setup marathon completed with exit code {} for {}".format(return_code, service_instance))
            if bounce_again_in_seconds:
                self.log.info("{} not in steady state so bouncing again in {} seconds".format(service_instance,
                                                                                              bounce_again_in_seconds))
                service_instance = ServiceInstance(service=service_instance.service,
                                                   instance=service_instance.instance,
                                                   bounce_by=int(time.time()) + bounce_again_in_seconds,
                                                   watcher=self.name)
                self.inbox_q.put(service_instance)
            else:
                self.log.info("{} in steady state".format(service_instance))
            time.sleep(0.1)
