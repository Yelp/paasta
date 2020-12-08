from typing import Any

from kazoo.client import KazooClient

from paasta_tools.util.config_loading import load_system_paasta_config


class ZookeeperPool:
    """
    A context manager that shares the same KazooClient with its children. The first nested context manager
    creates and deletes the client and shares it with any of its children. This allows to place a context
    manager over a large number of zookeeper calls without opening and closing a connection each time.
    GIL makes this 'safe'.
    """

    counter: int = 0
    zk: KazooClient = None

    @classmethod
    def __enter__(cls) -> KazooClient:
        if cls.zk is None:
            cls.zk = KazooClient(
                hosts=load_system_paasta_config().get_zk_hosts(), read_only=True
            )
            cls.zk.start()
        cls.counter = cls.counter + 1
        return cls.zk

    @classmethod
    def __exit__(cls, *_: Any, **__: Any) -> None:
        cls.counter = cls.counter - 1
        if cls.counter == 0:
            cls.zk.stop()
            cls.zk.close()
            cls.zk = None
