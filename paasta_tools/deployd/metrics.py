import time

from paasta_tools.deployd.common import PaastaThread


class QueueMetrics(PaastaThread):
    def __init__(self, inbox, bounce_q, cluster, metrics_provider):
        super().__init__()
        self.daemon = True
        self.instances_that_need_to_be_bounced_in_the_future = inbox.instances_that_need_to_be_bounced_in_the_future
        self.inbox = inbox.to_bounce
        self.bounce_q = bounce_q
        self.metrics = metrics_provider
        self.instances_that_need_to_be_bounced_in_the_future_gauge = self.metrics.create_gauge(
            "instances_that_need_to_be_bounced_in_the_future", paasta_cluster=cluster,
        )
        self.inbox_gauge = self.metrics.create_gauge("inbox", paasta_cluster=cluster)
        self.bounce_q_gauge = self.metrics.create_gauge("bounce_queue", paasta_cluster=cluster)

    def run(self):
        while True:
            self.instances_that_need_to_be_bounced_in_the_future_gauge.set(
                self.instances_that_need_to_be_bounced_in_the_future.qsize(),
            )
            self.inbox_gauge.set(len(self.inbox.keys()))
            self.bounce_q_gauge.set(self.bounce_q.qsize())
            time.sleep(20)
