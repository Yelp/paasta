import abc
import asyncio
import datetime
import logging
import time
from typing import Callable
from typing import Collection
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

import transitions.extensions
from mypy_extensions import TypedDict

log = logging.getLogger(__name__)


class TransitionDefinitionBase(TypedDict):
    """The required fields for TransitionDefinition; see
    https://mypy.readthedocs.io/en/latest/more_types.html#mixing-required-and-non-required-items"""

    trigger: str
    source: Union[str, List[str], Tuple[str, ...]]
    dest: Union[str, List[str], Tuple[str, ...]]


class TransitionDefinition(TransitionDefinitionBase, total=False):
    unless: List[Union[str, Callable]]
    conditions: List[Union[str, Callable]]
    before: Callable


class DeploymentProcess(abc.ABC):
    if TYPE_CHECKING:
        # These attributes need to be defined in this `if TYPE_CHECKING` block, because if they exist at runtime then
        # transitions will refuse to overwrite them.
        state: str

        def trigger(self, *args, **kwargs):
            ...

    run_timeout: Optional[
        float
    ] = None  # in normal operation, this will be None, but this lets tests set a max time.

    def __init__(self,):

        self.event_loop = asyncio.get_event_loop()
        self.finished_event = asyncio.Event(loop=self.event_loop)
        self.timer_running = False

        self.machine = transitions.extensions.LockedMachine(
            model=self,
            states=list(self.states()),
            transitions=list(self.valid_transitions()),
            initial=self.start_state(),
            after_state_change=self.after_state_change,
            before_state_change=self.before_state_change,
            queued=True,
        )

    @abc.abstractmethod
    def status_code_by_state(self) -> Mapping[str, int]:
        raise NotImplementedError()

    @abc.abstractmethod
    def states(self) -> Collection["str"]:
        raise NotImplementedError()

    @abc.abstractmethod
    def valid_transitions(self) -> Iterator[TransitionDefinition]:
        raise NotImplementedError()

    @abc.abstractmethod
    def start_transition(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def start_state(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def notify_users(self, message: str) -> None:
        """Print a log line somewhere that users can see it, e.g. Slack."""
        raise NotImplementedError()

    def is_terminal_state(self, state: str) -> bool:
        return state in self.status_code_by_state()

    def is_finished(self) -> bool:
        return self.finished_event.is_set()

    def finish(self):
        self.finished_event.set()

    def run(self):
        return self.event_loop.run_until_complete(self.run_async())

    async def run_async(self) -> int:
        self.trigger(self.start_transition())
        await asyncio.wait_for(self.finished_event.wait(), timeout=self.run_timeout)
        return self.status_code_by_state().get(self.state, 3)

    def after_state_change(self):
        if self.state in self.status_code_by_state():
            self.event_loop.call_soon_threadsafe(self.finished_event.set)

    def start_timer(self, timeout, trigger, message_verb):
        self.cancel_timer()

        timer_start = time.time()
        timer_end = timer_start + timeout
        formatted_time = datetime.datetime.fromtimestamp(timer_end)

        self.notify_users(
            f"Will {message_verb} in {timeout} seconds, (at {formatted_time})"
        )

        def times_up():
            self.notify_users(f"Time's up, will now {message_verb}.")
            self.trigger(trigger)
            self.timer_handle = None
            self.timer_running = False

        def schedule_callback():
            """Unfortunately, call_at is not threadsafe, and there's no call_at_threadsafe, so we need to schedule the
            call to call_at with call_soon_threadsafe."""
            self.timer_handle = self.event_loop.call_later(timeout, times_up)
            self.timer_trigger = (
                trigger
            )  # This allows cancel_timer to selectively cancel.
            self.timer_timeout = timeout  # saved for restart_timer
            self.timer_message_verb = message_verb  # saved for restart_timer

        self.event_loop.call_soon_threadsafe(schedule_callback)
        self.timer_running = True

    def cancel_timer(self, trigger=None):
        """Cancel the running timer. If trigger is specified, only cancel the timer if its trigger matches."""
        handle = self.get_timer_handle()
        if handle is None:
            self.timer_running = False
            return
        if trigger is None or trigger == self.timer_trigger:
            self.notify_users(f"Countdown to {self.timer_message_verb} cancelled.")
            handle.cancel()
            self.timer_handle = None
            self.timer_running = False
            self.timer_trigger = None
            self.timer_message_verb = None

    def restart_timer(self):
        self.cancel_timer()
        self.start_timer(
            timeout=self.timer_timeout,
            trigger=self.timer_trigger,
            message_verb=self.timer_message_verb,
        )

    def is_timer_running(self) -> bool:
        return self.timer_running

    def get_timer_handle(self):
        try:
            return self.timer_handle
        except AttributeError:
            return None

    def before_state_change(self):
        pass
