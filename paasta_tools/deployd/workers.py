from __future__ import absolute_import
from __future__ import unicode_literals

import time

from paasta_tools import marathon_tools
from paasta_tools.deployd.common import PaastaThread
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
            deploy_marathon_service(service=service_instance.service,
                                    instance=service_instance.instance,
                                    client=self.marathon_client,
                                    soa_dir=marathon_tools.DEFAULT_SOA_DIR,
                                    marathon_config=self.marathon_config,
                                    marathon_apps=marathon_apps)
            # TODO: make this return a time to wait for bounce and re-add to inbox_q
            time.sleep(0.1)
