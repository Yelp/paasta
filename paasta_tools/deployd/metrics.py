import time

from paasta_tools.deployd.common import PaastaThread


class QueueMetrics(PaastaThread):
    def __init__(self, inbox, cluster, metrics_provider):
        super().__init__()
        self.daemon = True
        self.metrics = metrics_provider

        self.inbox = inbox.to_bounce
        self.instances_to_bounce_later = inbox.instances_to_bounce_later
        self.instances_to_bounce_now = inbox.instances_to_bounce_now

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
                self.instances_to_bounce_later.qsize()
            )
            self.instances_that_need_to_be_checked_up_on_gauge.set(
                len(self.inbox.keys())
            )
            self.instances_to_bounce_now_gauge.set(self.instances_to_bounce_now.qsize())
            time.sleep(20)
