from __future__ import absolute_import
from __future__ import unicode_literals

import time

from paasta_tools import marathon_tools
from paasta_tools.deployd.common import BounceTimers
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.setup_marathon_job import deploy_marathon_service


class PaastaDeployWorker(PaastaThread):
    def __init__(self, worker_number, inbox_q, bounce_q, cluster, metrics_provider):
        super(PaastaDeployWorker, self).__init__()
        self.daemon = True
        self.name = "Worker{}".format(worker_number)
        self.inbox_q = inbox_q
        self.bounce_q = bounce_q
        self.metrics = metrics_provider
        self.cluster = cluster
        self.setup()

    def setup(self):
        self.marathon_config = marathon_tools.load_marathon_config()
        self.marathon_client = marathon_tools.get_marathon_client(self.marathon_config.get_url(),
                                                                  self.marathon_config.get_username(),
                                                                  self.marathon_config.get_password())

    def setup_timers(self, service_instance):
        bounce_timers = service_instance.bounce_timers
        if bounce_timers:
            bounce_timers.processed_by_worker.stop()
            bounce_length_timer = bounce_timers.bounce_length
        else:
            bounce_length_timer = self.metrics.create_timer('bounce_length_timer',
                                                            service=service_instance.service,
                                                            instance=service_instance.instance,
                                                            paasta_cluster=self.cluster)
            bounce_length_timer.start()
        processed_by_worker_timer = self.metrics.create_timer('processed_by_worker',
                                                              service=service_instance.service,
                                                              instance=service_instance.instance,
                                                              paasta_cluster=self.cluster)
        setup_marathon_timer = self.metrics.create_timer('setup_marathon_timer',
                                                         service=service_instance.service,
                                                         instance=service_instance.instance,
                                                         paasta_cluster=self.cluster)
        return BounceTimers(processed_by_worker=processed_by_worker_timer,
                            setup_marathon=setup_marathon_timer,
                            bounce_length=bounce_length_timer)

    def run(self):
        self.log.info("{} starting up".format(self.name))
        while True:
            service_instance = self.bounce_q.get()
            bounce_timers = self.setup_timers(service_instance)
            self.log.info("{} processing {}.{}".format(self.name, service_instance.service, service_instance.instance))
            marathon_apps = marathon_tools.get_all_marathon_apps(self.marathon_client, embed_failures=True)
            bounce_timers.setup_marathon.start()
            return_code, bounce_again_in_seconds = deploy_marathon_service(service=service_instance.service,
                                                                           instance=service_instance.instance,
                                                                           client=self.marathon_client,
                                                                           soa_dir=marathon_tools.DEFAULT_SOA_DIR,
                                                                           marathon_config=self.marathon_config,
                                                                           marathon_apps=marathon_apps)
            bounce_timers.setup_marathon.stop()
            self.log.info("setup marathon completed with exit code {} for {}.{}".format(return_code,
                                                                                        service_instance.service,
                                                                                        service_instance.instance))
            if bounce_again_in_seconds:
                bounce_timers.processed_by_worker.start()
                self.log.info("{}.{} not in steady state so bouncing again in {} "
                              "seconds".format(service_instance.service,
                                               service_instance.instance,
                                               bounce_again_in_seconds))
                service_instance = ServiceInstance(service=service_instance.service,
                                                   instance=service_instance.instance,
                                                   bounce_by=int(time.time()) + bounce_again_in_seconds,
                                                   watcher=self.name,
                                                   bounce_timers=bounce_timers)
                self.inbox_q.put(service_instance)
            else:
                bounce_timers.bounce_length.stop()
                self.log.info("{}.{} in steady state".format(service_instance.service,
                                                             service_instance.instance))
            time.sleep(0.1)
