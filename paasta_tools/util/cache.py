import time
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple
from typing import TypeVar

from mypy_extensions import TypedDict


class TimeCacheEntry(TypedDict):
    data: Any
    fetch_time: float


_CacheRetT = TypeVar("_CacheRetT")


class time_cache:
    def __init__(self, ttl: float = 0) -> None:
        self.configs: Dict[Tuple, TimeCacheEntry] = {}
        self.ttl = ttl

    def __call__(self, f: Callable[..., _CacheRetT]) -> Callable[..., _CacheRetT]:
        def cache(*args: Any, **kwargs: Any) -> _CacheRetT:
            if "ttl" in kwargs:
                ttl = kwargs["ttl"]
                del kwargs["ttl"]
            else:
                ttl = self.ttl
            key = args
            for item in kwargs.items():
                key += item
            if (
                (not ttl)
                or (key not in self.configs)
                or (time.time() - self.configs[key]["fetch_time"] > ttl)
            ):
                self.configs[key] = {
                    "data": f(*args, **kwargs),
                    "fetch_time": time.time(),
                }
            return self.configs[key]["data"]

        return cache
