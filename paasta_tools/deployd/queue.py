import json
import time
import uuid
from contextlib import contextmanager
from queue import Empty
from threading import Condition
from typing import Generator
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import WatchedEvent
from kazoo.protocol.states import ZnodeStat

from paasta_tools.deployd.common import DelayDeadlineQueueProtocol
from paasta_tools.deployd.common import ServiceInstance

DEPLOYD_QUEUE_ROOT = "/paasta-deployd-queue"
MAX_SLEEP_TIME = 3600


class ZKDelayDeadlineQueue(DelayDeadlineQueueProtocol):
    def __init__(self, client: KazooClient, path: str = DEPLOYD_QUEUE_ROOT) -> None:
        self.client = client
        self.id = uuid.uuid4().hex.encode()

        self.locks_path = path + "/locks"
        self.entries_path = path + "/entries"
        for path in (self.locks_path, self.entries_path):
            self.client.ensure_path(path)

        self.local_state_condition = Condition()
        self.entry_nodes: List[str] = []
        self.locked_entry_nodes: Set[str] = set()
        self._update_local_state(None)

    def _update_local_state(self, event: WatchedEvent) -> None:
        with self.local_state_condition:
            entry_nodes = self.client.retry(
                self.client.get_children,
                self.entries_path,
                watch=self._update_local_state,
            )
            self.entry_nodes = sorted(entry_nodes)
            self.locked_entry_nodes = set(
                self.client.retry(
                    self.client.get_children,
                    self.locks_path,
                    watch=self._update_local_state,
                )
            )
            self.local_state_condition.notify()

    def _format_timestamp(self, timestamp: float) -> str:
        if not isinstance(timestamp, (int, float)):
            raise TypeError(f"timestamp must be int or float, got {timestamp!r}")
        if not (0 < timestamp < 9999999999.9995):
            raise ValueError(
                f"timestamp must be between 0 and 9999999999.9995, got {timestamp}"
            )

        formatted = f"{timestamp:014.3f}"
        assert len(formatted) == 14

        return formatted

    def put(self, si: ServiceInstance) -> None:
        bounce_by = self._format_timestamp(si.bounce_by)
        wait_until = self._format_timestamp(si.wait_until)

        self.client.create(
            f"{self.entries_path}/entry-{bounce_by}-{wait_until}-",
            value=self._serialize_si(si),
            sequence=True,
        )

    def _serialize_si(self, si: ServiceInstance) -> bytes:
        si_dict = si._asdict()
        return json.dumps(si_dict).encode("utf-8")

    @contextmanager
    def get(
        self, block: bool = True, timeout: float = float("inf")
    ) -> Generator[ServiceInstance, None, None]:
        if not block:
            timeout = 0.0
        timeout_timestamp = time.time() + timeout

        entry = None
        with self.local_state_condition:
            while True:
                first_available_entry_node = self._get_first_available_entry_node()
                if first_available_entry_node is not None:
                    entry = self._lock_and_get_entry(first_available_entry_node)
                    if entry is not None:
                        break

                next_upcoming_wait_until = self._get_next_upcoming_wait_until()
                cond_wait_until = min(
                    timeout_timestamp,
                    next_upcoming_wait_until,
                    time.time() + MAX_SLEEP_TIME,
                )
                hit_timeout = not self.local_state_condition.wait(
                    timeout=cond_wait_until - time.time()
                )
                if hit_timeout and time.time() >= timeout_timestamp:
                    raise Empty()

        entry_data, entry_stat = entry

        try:
            yield self._parse_data(entry_data)
        except Exception:
            self._release(first_available_entry_node)
            raise
        else:
            self._consume(first_available_entry_node)

    def _parse_data(self, entry_data: bytes) -> ServiceInstance:
        now = time.time()
        defaults = {
            "watcher": "unknown",
            "bounce_by": now,
            "wait_until": now,
            "enqueue_time": now,
            "bounce_start_time": now,
            "failures": 0,
        }
        si_dict = json.loads(entry_data.decode("utf-8"))
        merged = {**defaults, **si_dict}
        return ServiceInstance(**merged)  # type: ignore

    def _parse_entry_node(self, path: str) -> Tuple[float, float]:
        basename = path.split("/")[-1]
        _, priority, wait_until, _ = basename.split("-", maxsplit=4)
        return float(priority), float(wait_until)

    def _get_first_available_entry_node(self) -> Optional[str]:
        for entry_node in self.entry_nodes:
            if entry_node not in self.locked_entry_nodes:
                _, wait_until = self._parse_entry_node(entry_node)
                now = time.time()
                if wait_until <= now:
                    return entry_node
        return None

    def _get_next_upcoming_wait_until(self) -> float:
        next_upcoming_wait_until = float("inf")
        for entry_node in self.entry_nodes:
            if entry_node not in self.locked_entry_nodes:
                _, wait_until = self._parse_entry_node(entry_node)
                next_upcoming_wait_until = min(next_upcoming_wait_until, wait_until)
        return next_upcoming_wait_until

    def _lock_and_get_entry(self, entry_node: str) -> Optional[Tuple[bytes, ZnodeStat]]:
        try:
            lock_path = f"{self.locks_path}/{entry_node}"
            self.locked_entry_nodes.add(entry_node)
            self.client.create(lock_path, value=self.id, ephemeral=True)
        except NodeExistsError:
            self.locked_entry_nodes.add(entry_node)
            return None

        try:
            return self.client.get(f"{self.entries_path}/{entry_node}")
        except NoNodeError:
            self.client.delete(lock_path)
            return None

    def _consume(self, entry_node: str) -> None:
        # necessary in case we lose connection at some point
        if not self._holds_lock(entry_node):
            return  # TODO: log?
        with self.client.transaction() as transaction:
            transaction.delete(f"{self.locks_path}/{entry_node}")
            transaction.delete(f"{self.entries_path}/{entry_node}")

    def _holds_lock(self, entry_node: str) -> bool:
        lock_path = f"{self.locks_path}/{entry_node}"
        self.client.sync(lock_path)
        value, stat = self.client.retry(self.client.get, lock_path)
        return value == self.id

    def _release(self, entry_node: str) -> None:
        if not self._holds_lock(entry_node):
            return
        self.client.delete(f"{self.locks_path}/{entry_node}")

    def _get_all_unlocked_service_instances(
        self, fetch_service_instances: bool
    ) -> Iterable[Tuple[float, float, Optional[ServiceInstance]]]:
        results = []
        with self.local_state_condition:
            for entry_node in self.entry_nodes:
                if entry_node not in self.locked_entry_nodes:
                    deadline, wait_until = self._parse_entry_node(entry_node)
                    if fetch_service_instances:
                        data, _ = self.client.get(f"{self.entries_path}/{entry_node}")
                        si = self._parse_data(data)
                    else:
                        si = None
                    results.append((deadline, wait_until, si))
        return results

    def get_available_service_instances(
        self, fetch_service_instances: bool
    ) -> Iterable[Tuple[float, Optional[ServiceInstance]]]:
        now = time.time()
        for deadline, wait_until, si in self._get_all_unlocked_service_instances(
            fetch_service_instances
        ):
            if wait_until <= now:
                yield (deadline, si)

    def get_unavailable_service_instances(
        self, fetch_service_instances: bool
    ) -> Iterable[Tuple[float, float, Optional[ServiceInstance]]]:
        now = time.time()
        for deadline, wait_until, si in self._get_all_unlocked_service_instances(
            fetch_service_instances
        ):
            if wait_until > now:
                yield (wait_until, deadline, si)
