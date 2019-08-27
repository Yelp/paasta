import uuid
from contextlib import contextmanager
from threading import Condition

from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError


class DistributedQueue:
    def __init__(self, client, path):
        self.client = client
        self.id = uuid.uuid4().hex.encode()

        self.locks_path = path + "/locks"
        self.entries_path = path + "/entries"
        for path in (self.locks_path, self.entries_path):
            self.client.ensure_path(path)

        self.local_state_condition = Condition()
        self.entry_nodes = []
        self.locked_entry_nodes = set()
        self.update_local_state(None)

    def update_local_state(self, event):
        with self.local_state_condition:
            print("refreshing state")
            if event is not None:
                print(f"  event: {event}")
            entry_nodes = self.client.retry(
                self.client.get_children,
                self.entries_path,
                watch=self.update_local_state,
            )
            self.entry_nodes = sorted(entry_nodes)
            self.locked_entry_nodes = set(
                self.client.retry(
                    self.client.get_children,
                    self.locks_path,
                    watch=self.update_local_state,
                )
            )
            self.local_state_condition.notify()

    def qsize(self):
        return len(self.entry_nodes)

    @property
    def queue(self):
        return self.entry_nodes

    # TODO: should priority just be a string?  can make it length 20 for two priorities?  or even make that configurable?
    def put(self, value, priority):
        self._check_put_arguments(value, priority)
        self.client.create(
            f"{self.entries_path}/entry-{priority:010d}-", value=value, sequence=True
        )

    def _check_put_arguments(self, value, priority=100):
        if not isinstance(value, bytes):
            raise TypeError(f"value must be a byte string (got type {type(value)})")
        elif not isinstance(priority, int):
            raise TypeError(f"priority must be an int (got type {type(priority)})")
        elif priority < 0 or priority > 9999999999:
            raise ValueError(
                f"priority must be between 0 and 9999999999 (got {priority})"
            )

    @contextmanager
    def get(self):  # TODO: timeout?
        entry = None
        with self.local_state_condition:
            while True:
                first_available_entry_node = self._get_first_available_entry_node()
                if first_available_entry_node is not None:
                    entry = self._lock_and_get_entry(first_available_entry_node)
                    if entry is not None:
                        break

                self.local_state_condition.wait()

        entry_data, entry_stat = entry

        try:
            yield entry_data
        except Exception:
            self._release(first_available_entry_node)
            raise
        else:
            self._consume(first_available_entry_node)

    def _get_first_available_entry_node(self):
        for entry_node in self.entry_nodes:
            if entry_node not in self.locked_entry_nodes:
                return entry_node
        return None

    def _lock_and_get_entry(self, entry_node):
        try:
            lock_path = f"{self.locks_path}/{entry_node}"
            self.client.create(lock_path, value=self.id, ephemeral=True)
        except NodeExistsError:
            return None

        try:
            return self.client.get(f"{self.entries_path}/{entry_node}")
        except NoNodeError:
            self.client.delete(lock_path)
            return None

    def _consume(self, entry_node):
        # necessary in case we lose connection at some point
        if not self._holds_lock(entry_node):
            return  # TODO: log?
        with self.client.transaction() as transaction:
            transaction.delete(f"{self.locks_path}/{entry_node}")
            transaction.delete(f"{self.entries_path}/{entry_node}")

    def _holds_lock(self, entry_node):
        lock_path = f"{self.locks_path}/{entry_node}"
        self.client.sync(lock_path)
        value, stat = self.client.retry(self.client.get, lock_path)
        return value == self.id

    def _release(self, entry_node):
        if not self._holds_lock(entry_node):
            return
        self.client.delete(f"{self.locks_path}/{entry_node}")
