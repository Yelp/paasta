# Stubs for marathon.models.group (Python 3.7)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.

from typing import Any, Optional
from .base import (
    MarathonResource as MarathonResource,
    assert_valid_id as assert_valid_id,
)
from .app import MarathonApp as MarathonApp

class MarathonGroup(MarathonResource):
    apps = ...  # type: Any
    dependencies = ...  # type: Any
    groups = ...  # type: Any
    pods = ...  # type: Any
    id = ...  # type: Any
    version = ...  # type: Any
    def __init__(
        self,
        apps: Optional[Any] = ...,
        dependencies: Optional[Any] = ...,
        groups: Optional[Any] = ...,
        id: Optional[Any] = ...,
        pods: Optional[Any] = ...,
        version: Optional[Any] = ...,
    ) -> None: ...
