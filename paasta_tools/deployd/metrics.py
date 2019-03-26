import time

from paasta_tools.deployd.common import PaastaThread


class QueueMetrics(PaastaThread):
    def __init__(self, inbox, instances_that_need_to_be_bounced_asap, cluster, metrics_provider):
        super().__init__()
        self.daemon = True
        self.instances_that_need_to_be_bounced_in_the_future = inbox.instances_that_need_to_be_bounced_in_the_future
        self.inbox = inbox.to_bounce
        self.instances_that_need_to_be_bounced_asap = instances_that_need_to_be_bounced_asap
        self.metrics = metrics_provider
        self.instances_that_need_to_be_bounced_in_the_future_gauge = self.metrics.create_gauge(
            "instances_that_need_to_be_bounced_in_the_future", paasta_cluster=cluster,
        )
        self.inbox_gauge = self.metrics.create_gauge("inbox", paasta_cluster=cluster)
        self.instances_that_need_to_be_bounced_asap_gauge = self.metrics.create_gauge(
            "instances_that_need_to_be_bounced_asap", paasta_cluster=cluster,
        )

    def run(self):
        while True:
            self.instances_that_need_to_be_bounced_in_the_future_gauge.set(
                self.instances_that_need_to_be_bounced_in_the_future.qsize(),
            )
            self.inbox_gauge.set(len(self.inbox.keys()))
            self.instances_that_need_to_be_bounced_asap_gauge.set(self.instances_that_need_to_be_bounced_asap.qsize())
            time.sleep(20)
