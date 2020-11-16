import logging
import threading
import time
from typing import Any
from typing import Callable

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import ConnectionClosedError
from kazoo.recipe.election import Election

from paasta_tools.deployd.common import PaastaThread


class PaastaLeaderElection(Election):
    def __init__(self, client: KazooClient, *args: Any, **kwargs: Any) -> None:
        self.client = client
        self.control = kwargs.pop("control")
        super().__init__(self.client, *args, **kwargs)
        self.client.add_listener(self.connection_listener)
        self.waiting_for_reconnect = False

    @property
    def log(self) -> logging.Logger:
        name = ".".join([__name__, type(self).__name__])
        return logging.getLogger(name)

    def run(self, func: Callable, *args: Any, **kwargs: Any) -> None:
        try:
            super().run(func, *args, **kwargs)
        except ConnectionClosedError:
            self.log.error("Zookeeper connection closed so can't tidy up!")
            return

    def connection_listener(self, state: KazooState) -> None:
        self.log.warning(f"Zookeeper connection transitioned to: {state}")
        if state == KazooState.SUSPENDED:
            self.log.warning(
                "Zookeeper connection suspended, waiting to see if it recovers."
            )
            if not self.waiting_for_reconnect:
                self.waiting_for_reconnect = True
                reconnection_checker = PaastaThread(target=self.reconnection_listener)
                reconnection_checker.daemon = True
                reconnection_checker.start()
        elif state == KazooState.LOST:
            self.log.error("Leadership lost, quitting!")
            self._terminate()

    def reconnection_listener(self) -> None:
        attempts = 0
        while attempts < 5:
            if self.client.state == KazooState.CONNECTED:
                self.log.warning("Zookeeper connection recovered!")
                self.waiting_for_reconnect = False
                return
            self.log.warning("Waiting for zookeeper connection to recover")
            time.sleep(5)
            attempts += 1
        self.log.error("Connection did not recover, abdicating!")
        self._terminate()

    def _terminate(self) -> None:
        thread_info = [
            {"alive": t.is_alive(), "daemon": t.daemon, "name": t.__class__.__name__}
            for t in threading.enumerate()
        ]
        self.log.info(f"Thread info: {thread_info}")
        self.control.put("ABORT")
