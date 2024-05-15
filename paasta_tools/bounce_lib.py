#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import math
from typing import Callable
from typing import Collection
from typing import Dict
from typing import Sequence
from typing import Set

from mypy_extensions import Arg
from mypy_extensions import DefaultArg
from mypy_extensions import TypedDict


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())
logging.getLogger("requests").setLevel(logging.WARNING)

ZK_LOCK_CONNECT_TIMEOUT_S = 10.0  # seconds to wait to connect to zookeeper
ZK_LOCK_PATH = "/bounce"
WAIT_CREATE_S = 3
WAIT_DELETE_S = 5

BounceMethodConfigDict = TypedDict("BounceMethodConfigDict", {"instances": int})

BounceMethodResult = TypedDict(
    "BounceMethodResult", {"create_app": bool, "tasks_to_drain": Set}
)

BounceMethod = Callable[
    [
        Arg(BounceMethodConfigDict, "new_config"),
        Arg(bool, "new_app_running"),
        Arg(Collection, "happy_new_tasks"),
        Arg(Sequence, "old_non_draining_tasks"),
        DefaultArg(float, "margin_factor"),
    ],
    BounceMethodResult,
]


_bounce_method_funcs: Dict[str, BounceMethod] = {}


def register_bounce_method(name: str) -> Callable[[BounceMethod], BounceMethod]:
    """Returns a decorator that registers that bounce function at a given name
    so get_bounce_method_func can find it."""

    def outer(bounce_func: BounceMethod):
        _bounce_method_funcs[name] = bounce_func
        return bounce_func

    return outer


def get_bounce_method_func(name) -> BounceMethod:
    return _bounce_method_funcs[name]


def list_bounce_methods() -> Collection[str]:
    return _bounce_method_funcs.keys()


@register_bounce_method("brutal")
def brutal_bounce(
    new_config: BounceMethodConfigDict,
    new_app_running: bool,
    happy_new_tasks: Collection,
    old_non_draining_tasks: Sequence,
    margin_factor=1.0,
) -> BounceMethodResult:
    """Pays no regard to safety. Starts the new app if necessary, and kills any
    old ones. Mostly meant as an example of the simplest working bounce method,
    but might be tolerable for some services.

    :param new_config: The configuration dictionary representing the desired new app.
    :param new_app_running: Whether there is an app in Marathon with the same ID as the new config.
    :param happy_new_tasks: Set of MarathonTasks belonging to the new application that are considered healthy and up.
    :param old_non_draining_tasks: A sequence of tasks not belonging to the new version. Tasks should be ordered from
                                   most desirable to least desirable.
    :param margin_factor: the multiplication factor used to calculate the number of instances to be drained
                          when the crossover method is used.
    :return: A dictionary representing the desired bounce actions and containing the following keys:
              - create_app: True if we should start the new Marathon app, False otherwise.
              - tasks_to_drain: a set of task objects which should be drained and killed. May be empty.
    """
    return {
        "create_app": not new_app_running,
        "tasks_to_drain": set(old_non_draining_tasks),
    }


@register_bounce_method("upthendown")
def upthendown_bounce(
    new_config: BounceMethodConfigDict,
    new_app_running: bool,
    happy_new_tasks: Collection,
    old_non_draining_tasks: Sequence,
    margin_factor=1.0,
) -> BounceMethodResult:
    """Starts a new app if necessary; only kills old apps once all the requested tasks for the new version are running.

    See the docstring for brutal_bounce() for parameters and return value.
    """
    if new_app_running and len(happy_new_tasks) == new_config["instances"]:
        return {"create_app": False, "tasks_to_drain": set(old_non_draining_tasks)}
    else:
        return {"create_app": not new_app_running, "tasks_to_drain": set()}


@register_bounce_method("crossover")
def crossover_bounce(
    new_config: BounceMethodConfigDict,
    new_app_running: bool,
    happy_new_tasks: Collection,
    old_non_draining_tasks: Sequence,
    margin_factor=1.0,
) -> BounceMethodResult:
    """Starts a new app if necessary; slowly kills old apps as instances of the new app become happy.

    See the docstring for brutal_bounce() for parameters and return value.
    """

    assert margin_factor > 0
    assert margin_factor <= 1

    needed_count = max(
        int(math.ceil(new_config["instances"] * margin_factor)) - len(happy_new_tasks),
        0,
    )

    return {
        "create_app": not new_app_running,
        "tasks_to_drain": set(old_non_draining_tasks[needed_count:]),
    }


@register_bounce_method("downthenup")
def downthenup_bounce(
    new_config: BounceMethodConfigDict,
    new_app_running: bool,
    happy_new_tasks: Collection,
    old_non_draining_tasks: Sequence,
    margin_factor=1.0,
) -> BounceMethodResult:
    """Stops any old apps and waits for them to die before starting a new one.

    See the docstring for brutal_bounce() for parameters and return value.
    """
    return {
        "create_app": not old_non_draining_tasks and not new_app_running,
        "tasks_to_drain": set(old_non_draining_tasks),
    }


@register_bounce_method("down")
def down_bounce(
    new_config: BounceMethodConfigDict,
    new_app_running: bool,
    happy_new_tasks: Collection,
    old_non_draining_tasks: Sequence,
    margin_factor=1.0,
) -> BounceMethodResult:
    """
    Stops old apps, doesn't start any new apps.
    Used for the graceful_app_drain script.
    """
    return {"create_app": False, "tasks_to_drain": set(old_non_draining_tasks)}


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
