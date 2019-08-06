import asyncio
import concurrent
import logging
import time
from collections import namedtuple
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
        self.run_background_event_loop()

    @property
    def log(self) -> logging.Logger:
        name = ".".join([type(self).__module__, type(self).__name__])
        return logging.getLogger(name)

    def put(self, si: ServiceInstance) -> concurrent.futures.Future:
        self.log.debug(
            f"adding {si.service}.{si.instance} to queue with wait_until {si.wait_until} and bounce_by {si.bounce_by}"
        )

        async def inner() -> None:
            while time.time() < si.wait_until:
                # In python < 3.7.1, asyncio.sleep, loop.call_later, and loop.call_at all have an issue where they will
                # not accept times too far in the future. Repeatedly sleep a shorter amount of time to work around this.
                await asyncio.sleep(min(86400 - 1, si.wait_until - time.time()))
            self.available_service_instances.put((si.bounce_by, si))

        return asyncio.run_coroutine_threadsafe(inner(), self.loop)

    def run_background_event_loop(self) -> None:
        """Run an asyncio event loop in a thread so put_entry_in_queue_later can schedule
        callbacks on it."""
        self.loop = asyncio.new_event_loop()

        def inner() -> None:
            asyncio.set_event_loop(self.loop)
            self.loop.run_forever()

        Thread(target=inner, daemon=True).start()

    def get(self, block: bool = True, timeout: float = None) -> ServiceInstance:
        bounce_by, si = self.available_service_instances.get(
            block=block, timeout=timeout
        )
        return si
