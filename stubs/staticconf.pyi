from typing import Mapping
from typing import List
from typing import Optional

from staticconf.config import DEFAULT as DEFAULT_NAMESPACE


def read(config_key: str, default: Optional[str] = None, namespace: str = DEFAULT_NAMESPACE) -> str:
    ...


def read_bool(config_key: str, default: Optional[bool] = None, namespace: str = DEFAULT_NAMESPACE) -> bool:
    ...


def read_float(config_key: str, default: Optional[float] = None, namespace: str = DEFAULT_NAMESPACE) -> float:
    ...


def read_list(config_key: str, default: Optional[List] = None, namespace: str = DEFAULT_NAMESPACE) -> List:
    ...


def read_int(config_key: str, default: Optional[int] = None, namespace: str = DEFAULT_NAMESPACE) -> int:
    ...


def read_string(config_key: str, default: Optional[str] = None, namespace: str = DEFAULT_NAMESPACE) -> str:
    ...


def YamlConfiguration(filename: str, namespace: str = DEFAULT_NAMESPACE) -> None:
    ...


def JSONConfiguration(filename: str, namespace: str = DEFAULT_NAMESPACE) -> None:
    ...


def DictConfiguration(config: Mapping, namespace: str = DEFAULT_NAMESPACE) -> None:
    ...


class NamespaceAccessor:
    def read_bool(self, config_key: str, default: Optional[bool] = None, namespace: str = DEFAULT_NAMESPACE) -> bool:
        ...

    def read_float(self, config_key: str, default: Optional[float] = None, namespace: str = DEFAULT_NAMESPACE) -> float:
        ...

    def read_list(self, config_key: str, default: Optional[List] = None, namespace: str = DEFAULT_NAMESPACE) -> List:
        ...

    def read_int(self, config_key: str, default: Optional[int] = None, namespace: str = DEFAULT_NAMESPACE) -> int:
        ...

    def read_string(self, config_key: str, default: Optional[str] = None, namespace: str = DEFAULT_NAMESPACE) -> str:
        ...


def NamespaceReaders(namespace: str) -> NamespaceAccessor:
    ...
