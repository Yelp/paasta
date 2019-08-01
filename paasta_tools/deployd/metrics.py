import time

from paasta_tools.deployd.common import PaastaThread


class QueueMetrics(PaastaThread):
    def __init__(self, queue, cluster, metrics_provider):
        super().__init__()
        self.daemon = True
        self.metrics = metrics_provider
        self.queue = queue

        self.instances_to_bounce_later_gauge = self.metrics.create_gauge(
            "instances_to_bounce_later", paasta_cluster=cluster
        )
        self.instances_that_need_to_be_checked_up_on_gauge = self.metrics.create_gauge(
            "instances_that_need_to_be_checked_up_on", paasta_cluster=cluster
        )
        self.instances_to_bounce_now_gauge = self.metrics.create_gauge(
            "instances_to_bounce_now", paasta_cluster=cluster
        )

    def run(self):
        while True:
            self.instances_to_bounce_later_gauge.set(
                self.queue.unavailable_service_instances.qsize()
            )
            self.instances_to_bounce_now_gauge.set(
                self.queue.available_service_instances.qsize()
            )
            time.sleep(20)
