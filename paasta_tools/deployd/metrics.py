import time
from typing import List

from paasta_tools.deployd.common import DelayDeadlineQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.workers import PaastaDeployWorker
from paasta_tools.metrics.metrics_lib import BaseMetrics


class MetricsThread(PaastaThread):
    def __init__(self, metrics_provider):
        super().__init__()
        self.metrics = metrics_provider

    def run_once(self):
        raise NotImplementedError()

    def run(self):
        while True:
            last_run_time = time.time()
            self.run_once()
            time.sleep(last_run_time + 20 - time.time())


class QueueAndWorkerMetrics(MetricsThread):
    def __init__(
        self,
        queue: DelayDeadlineQueue,
        workers: List[PaastaDeployWorker],
        cluster: str,
        metrics_provider: BaseMetrics,
    ) -> None:
        super().__init__(metrics_provider)
        self.daemon = True
        self.queue = queue

        self.instances_to_bounce_later_gauge = self.metrics.create_gauge(
            "instances_to_bounce_later", paasta_cluster=cluster
        )
        self.instances_to_bounce_now_gauge = self.metrics.create_gauge(
            "instances_to_bounce_now", paasta_cluster=cluster
        )
        self.instances_with_past_deadline_gauge = self.metrics.create_gauge(
            "instances_with_past_deadline", paasta_cluster=cluster
        )
        self.instances_with_deadline_in_next_n_seconds_gauges = {
            (available, n): self.metrics.create_gauge(
                f"{available}_instances_with_deadline_in_next_{n}s",
                paasta_cluster=cluster,
            )
            for n in [60, 300, 3600]
            for available in ["available", "unavailable"]
        }
        self.max_time_past_deadline_gauge = self.metrics.create_gauge(
            "max_time_past_deadline", paasta_cluster=cluster
        )
        self.sum_time_past_deadline_gauge = self.metrics.create_gauge(
            "sum_time_past_deadline", paasta_cluster=cluster
        )

        self.workers = workers

        self.workers_busy_gauge = self.metrics.create_gauge(
            "workers_busy", paasta_cluster=cluster
        )
        self.workers_idle_gauge = self.metrics.create_gauge(
            "workers_idle", paasta_cluster=cluster
        )
        self.workers_dead_gauge = self.metrics.create_gauge(
            "workers_dead", paasta_cluster=cluster
        )

    def run_once(self) -> None:
        self.instances_to_bounce_later_gauge.set(
            self.queue.unavailable_service_instances.qsize()
        )
        self.instances_to_bounce_now_gauge.set(
            self.queue.available_service_instances.qsize()
        )

        currently_available_instances = tuple(
            self.queue.available_service_instances.queue
        )
        currently_unavailable_instances = tuple(
            self.queue.unavailable_service_instances.queue
        )

        available_deadlines = [
            deadline for deadline, _ in currently_available_instances
        ]
        unavailable_deadlines = [
            deadline for _, deadline, _ in currently_unavailable_instances
        ]

        now = time.time()
        self.instances_with_past_deadline_gauge.set(
            len([1 for deadline in available_deadlines if deadline < now])
        )

        for (
            (available, n),
            gauge,
        ) in self.instances_with_deadline_in_next_n_seconds_gauges.items():
            if available == "available":
                deadlines = available_deadlines
            else:
                deadlines = unavailable_deadlines

            gauge.set(len([1 for deadline in deadlines if now < deadline < now + n]))

        self.max_time_past_deadline_gauge.set(
            max(
                [now - deadline for deadline in available_deadlines if deadline < now],
                default=0,
            )
        )

        self.sum_time_past_deadline_gauge.set(
            sum([max(0, now - deadline) for deadline in available_deadlines])
        )

        self.workers_busy_gauge.set(
            len([worker for worker in self.workers if worker.busy])
        )
        self.workers_idle_gauge.set(
            len([worker for worker in self.workers if not worker.busy])
        )
        self.workers_dead_gauge.set(
            len([worker for worker in self.workers if not worker.is_alive()])
        )
