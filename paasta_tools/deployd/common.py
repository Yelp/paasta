import logging
import time
from collections import namedtuple
from queue import Empty
from queue import PriorityQueue
from queue import Queue
from threading import Thread
from typing import Any
from typing import Collection
from typing import List
from typing import Optional
from typing import Tuple

from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import get_all_marathon_apps
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import load_marathon_service_config_no_cache
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.utils import load_system_paasta_config

BounceTimers = namedtuple(
    "BounceTimers", ["processed_by_worker", "setup_marathon", "bounce_length"]
)
BaseServiceInstance = namedtuple(
    "ServiceInstance",
    [
        "service",
        "instance",
        "bounce_by",
        "wait_until",
        "watcher",
        "bounce_timers",
        "failures",
        "processed_count",
    ],
)


class ServiceInstance(BaseServiceInstance):
    __slots__ = ()

    def __new__(
        _cls,
        service: str,
        instance: str,
        watcher: str,
        cluster: str,
        bounce_by: float,
        wait_until: float,
        failures: int = 0,
        bounce_timers: Optional[BounceTimers] = None,
        processed_count: int = 0,
    ) -> "ServiceInstance":
        return super().__new__(  # type: ignore
            _cls=_cls,
            service=service,
            instance=instance,
            watcher=watcher,
            bounce_by=bounce_by,
            wait_until=wait_until,
            failures=failures,
            bounce_timers=bounce_timers,
            processed_count=processed_count,
        )


class PaastaThread(Thread):
    @property
    def log(self) -> logging.Logger:
        name = ".".join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)


class PaastaQueue(Queue):
    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.name = name
        super().__init__(*args, **kwargs)

    @property
    def log(self) -> logging.Logger:
        name = ".".join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, item: Any, *args: Any, **kwargs: Any) -> None:
        self.log.debug(f"Adding {item} to {self.name} queue")
        super().put(item, *args, **kwargs)


def exponential_back_off(
    failures: int, factor: float, base: float, max_time: float
) -> float:
    seconds = factor * base ** failures
    return seconds if seconds < max_time else max_time


def get_service_instances_needing_update(
    marathon_clients: MarathonClients,
    instances: Collection[Tuple[str, str]],
    cluster: str,
) -> List[Tuple[str, str]]:
    marathon_apps = {}
    for marathon_client in marathon_clients.get_all_clients():
        marathon_apps.update(
            {app.id: app for app in get_all_marathon_apps(marathon_client)}
        )

    marathon_app_ids = marathon_apps.keys()
    service_instances = []
    for service, instance in instances:
        try:
            config = load_marathon_service_config_no_cache(
                service=service,
                instance=instance,
                cluster=cluster,
                soa_dir=DEFAULT_SOA_DIR,
            )
            config_app = config.format_marathon_app_dict()
            app_id = "/{}".format(config_app["id"])
        # Not ideal but we rely on a lot of user input to create the app dict
        # and we really can't afford to bail if just one app definition is malformed
        except Exception as e:
            print(
                "ERROR: Skipping {}.{} because: '{}'".format(service, instance, str(e))
            )
            continue
        if app_id not in marathon_app_ids:
            service_instances.append((service, instance))
        elif marathon_apps[app_id].instances != config_app["instances"]:
            service_instances.append((service, instance))
    return service_instances


def get_marathon_clients_from_config() -> MarathonClients:
    system_paasta_config = load_system_paasta_config()
    marathon_servers = get_marathon_servers(system_paasta_config)
    marathon_clients = get_marathon_clients(marathon_servers)
    return marathon_clients


class DelayDeadlineQueue:
    """Entries into this queue have both a wait_until and a bounce_by. Before wait_until, get() will not return an entry.
    get() returns the entry whose wait_until has passed and which has the lowest bounce_by."""

    def __init__(self) -> None:
        self.available_service_instances: PriorityQueue[
            Tuple[float, ServiceInstance]
        ] = PriorityQueue()
        self.unavailable_service_instances: PriorityQueue[
            Tuple[float, float, ServiceInstance]
        ] = PriorityQueue()

    @property
    def log(self) -> logging.Logger:
        name = ".".join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, si: ServiceInstance, now: Optional[float] = None) -> None:
        self.log.debug(
            f"adding {si.service}.{si.instance} to queue with wait_until {si.wait_until} and bounce_by {si.bounce_by}"
        )
        self.unavailable_service_instances.put((si.wait_until, si.bounce_by, si))
        self.process_unavailable_service_instances(now=now)

    def process_unavailable_service_instances(
        self, now: Optional[float] = None
    ) -> None:
        """Take any entries in unavailable_service_instances that have a wait_until < now and put them in
        available_service_instances. Should not block."""
        if now is None:
            now = time.time()
        try:
            while True:
                wait_until, bounce_by, si = (
                    self.unavailable_service_instances.get_nowait()
                )
                if wait_until < now:
                    self.available_service_instances.put((bounce_by, si))
                else:
                    self.unavailable_service_instances.put((wait_until, bounce_by, si))
                    return
        except Empty:
            pass

    def get(
        self, block: bool = True, timeout: float = None, now: float = None
    ) -> ServiceInstance:
        self.process_unavailable_service_instances(now=now)
        bounce_by, si = self.available_service_instances.get(
            block=block, timeout=timeout
        )
        return si
