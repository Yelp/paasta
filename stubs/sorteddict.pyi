from typing import MutableMapping
from typing import TypeVar

K = TypeVar('K')
V = TypeVar('V')

class SortedDict(MutableMapping)
    def __getitem__(self, index: K) -> V: ...
