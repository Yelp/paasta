import time
from collections import namedtuple

from paasta_tools import marathon_tools
from paasta_tools.deployd.common import BounceTimers
from paasta_tools.deployd.common import exponential_back_off
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.setup_marathon_job import deploy_marathon_service
from paasta_tools.utils import load_system_paasta_config

BounceResults = namedtuple('BounceResults', ['bounce_again_in_seconds', 'return_code', 'bounce_timers'])


class PaastaDeployWorker(PaastaThread):
    def __init__(self, worker_number, inbox_q, bounce_q, config, metrics_provider):
        super().__init__()
        self.daemon = True
        self.name = f"Worker{worker_number}"
        self.inbox_q = inbox_q
        self.bounce_q = bounce_q
        self.metrics = metrics_provider
        self.config = config
        self.cluster = self.config.get_cluster()
        self.setup()

    def setup(self):
        system_paasta_config = load_system_paasta_config()
        self.marathon_servers = marathon_tools.get_marathon_servers(system_paasta_config)
        self.marathon_clients = marathon_tools.get_marathon_clients(self.marathon_servers)

    def setup_timers(self, service_instance):
        bounce_timers = service_instance.bounce_timers
        if bounce_timers:
            bounce_timers.processed_by_worker.stop()
            bounce_length_timer = bounce_timers.bounce_length
        else:
            bounce_length_timer = self.metrics.create_timer(
                'bounce_length_timer',
                service=service_instance.service,
                instance=service_instance.instance,
                paasta_cluster=self.cluster,
            )
            bounce_length_timer.start()
        processed_by_worker_timer = self.metrics.create_timer(
            'processed_by_worker',
            service=service_instance.service,
            instance=service_instance.instance,
            paasta_cluster=self.cluster,
        )
        setup_marathon_timer = self.metrics.create_timer(
            'setup_marathon_timer',
            service=service_instance.service,
            instance=service_instance.instance,
            paasta_cluster=self.cluster,
        )
        return BounceTimers(
            processed_by_worker=processed_by_worker_timer,
            setup_marathon=setup_marathon_timer,
            bounce_length=bounce_length_timer,
        )

    def run(self):
        self.log.info(f"{self.name} starting up")
        while True:
            service_instance = self.bounce_q.get()
            try:
                bounce_again_in_seconds, return_code, bounce_timers = self.process_service_instance(service_instance)
            except Exception as e:
                self.log.error("Worker failed to process service instance and will retry. "
                               "Caused by exception: {}".format(e))
                return_code = -2
                bounce_timers = service_instance.bounce_timers
            failures = service_instance.failures
            if return_code != 0:
                failures = service_instance.failures + 1
                bounce_again_in_seconds = exponential_back_off(
                    failures=failures,
                    factor=self.config.get_deployd_worker_failure_backoff_factor(),
                    base=2,
                    max_time=6000,
                )
            if bounce_again_in_seconds:
                service_instance = ServiceInstance(
                    service=service_instance.service,
                    instance=service_instance.instance,
                    cluster=self.config.get_cluster(),
                    bounce_by=int(time.time()) + bounce_again_in_seconds,
                    watcher=self.name,
                    bounce_timers=bounce_timers,
                    priority=service_instance.priority,
                    failures=failures,
                )
                self.inbox_q.put(service_instance)
            time.sleep(0.1)

    def process_service_instance(self, service_instance):
        bounce_timers = self.setup_timers(service_instance)
        self.log.info(f"{self.name} processing {service_instance.service}.{service_instance.instance}")

        bounce_timers.setup_marathon.start()
        return_code, bounce_again_in_seconds = deploy_marathon_service(
            service=service_instance.service,
            instance=service_instance.instance,
            clients=self.marathon_clients,
            soa_dir=marathon_tools.DEFAULT_SOA_DIR,
            marathon_apps_with_clients=None,
        )

        bounce_timers.setup_marathon.stop()
        self.log.info("setup marathon completed with exit code {} for {}.{}".format(
            return_code,
            service_instance.service,
            service_instance.instance,
        ))
        if bounce_again_in_seconds:
            bounce_timers.processed_by_worker.start()
            self.log.info("{}.{} not in steady state so bouncing again in {} "
                          "seconds".format(
                              service_instance.service,
                              service_instance.instance,
                              bounce_again_in_seconds,
                          ))
        else:
            bounce_timers.bounce_length.stop()
            self.log.info("{}.{} in steady state".format(
                service_instance.service,
                service_instance.instance,
            ))
        return BounceResults(bounce_again_in_seconds, return_code, bounce_timers)
