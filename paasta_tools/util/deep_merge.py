import copy
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import TypeVar

_DeepMergeT = TypeVar("_DeepMergeT", bound=Any)


class DuplicateKeyError(Exception):
    pass


def deep_merge_dictionaries(
    overrides: _DeepMergeT, defaults: _DeepMergeT, allow_duplicate_keys: bool = True
) -> _DeepMergeT:
    """
    Merges two dictionaries.
    """
    result = copy.deepcopy(defaults)
    stack: List[Tuple[Dict, Dict]] = [(overrides, result)]
    while stack:
        source_dict, result_dict = stack.pop()
        for key, value in source_dict.items():
            try:
                child = result_dict[key]
            except KeyError:
                result_dict[key] = value
            else:
                if isinstance(value, dict) and isinstance(child, dict):
                    stack.append((value, child))
                else:
                    if allow_duplicate_keys:
                        result_dict[key] = value
                    else:
                        raise DuplicateKeyError(
                            f"defaults and overrides both have key {key}"
                        )
    return result
