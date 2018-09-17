import time

from paasta_tools.deployd.common import PaastaThread


class QueueMetrics(PaastaThread):
    def __init__(self, inbox, bounce_q, cluster, metrics_provider):
        super().__init__()
        self.daemon = True
        self.inbox_q = inbox.inbox_q
        self.inbox = inbox.to_bounce
        self.bounce_q = bounce_q
        self.metrics = metrics_provider
        self.inbox_q_gauge = self.metrics.create_gauge("inbox_queue", paasta_cluster=cluster)
        self.inbox_gauge = self.metrics.create_gauge("inbox", paasta_cluster=cluster)
        self.bounce_q_gauge = self.metrics.create_gauge("bounce_queue", paasta_cluster=cluster)

    def run(self):
        while True:
            self.inbox_q_gauge.set(self.inbox_q.qsize())
            self.inbox_gauge.set(len(self.inbox.keys()))
            self.bounce_q_gauge.set(self.bounce_q.qsize())
            time.sleep(20)
