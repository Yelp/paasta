from typing import Any
from typing import AnyStr
from typing import Callable
from typing import Dict
from typing import IO
from typing import List
from typing import Optional
from typing import Tuple


def dumps(obj: Any,
    skipkeys: bool = ...,
    ensure_ascii: bool = ...,
    check_circular: bool = ...,
    allow_nan: bool = ...,
    cls: Any = ...,
    indent: Optional[int] = ...,
    separators: Optional[Tuple[str, str]] = ...,
    encoding: str = ...,
    default: Optional[Callable[[Any], Any]] = ...,
    sort_keys: bool = ...,
    use_decimanl: bool = ...,
    named_tuple_as_object: bool = ...,
    tuple_as_array: bool = ...,
    bigint_as_string: bool = ...,
    item_sort_key: Optional[Callable[[Any], Any]] = ...,
    for_json: bool = ...,
    ignore_nan: bool = ...,
    int_as_string_bitcount: Optional[int] = ...,
    iterable_as_array: bool = ...,
    **kwds: Any) -> str: ...


def loads(s: AnyStr,
    encoding: Any = ...,
    cls: Any = ...,
    object_hook: Optional[Callable[[Dict], Any]] = ...,
    parse_float: Optional[Callable[[str], Any]] = ...,
    parse_int: Optional[Callable[[str], Any]] = ...,
    parse_constant: Optional[Callable[[str], Any]] = ...,
    object_pairs_hook: Optional[Callable[[List[Tuple[Any, Any]]], Any]] = ...,
    use_decimal: bool = ...,
    **kwds: Any) -> Any: ...

def load(fp: IO[str],
    encoding: Optional[str] = ...,
    cls: Any = ...,
    object_hook: Optional[Callable[[Dict], Any]] = ...,
    parse_float: Optional[Callable[[str], Any]] = ...,
    parse_int: Optional[Callable[[str], Any]] = ...,
    parse_constant: Optional[Callable[[str], Any]] = ...,
    object_pairs_hook: Optional[Callable[[List[Tuple[Any, Any]]], Any]] = ...,
    use_decimal: bool = ...,
    namedtuple_as_object: bool = ...,
    tuple_as_array: bool = ...,
    **kwds: Any) -> Any: ...
