import abc
import asyncio
import datetime
import time
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


class TransitionDefinition(TypedDict):
    trigger: str
    source: Union[str, List[str], Tuple[str, ...]]
    dest: Union[str, List[str], Tuple[str, ...]]


class DeploymentProcess(abc.ABC):
    if TYPE_CHECKING:
        # These attributes need to be defined in this `if TYPE_CHECKING` block, because if they exist at runtime then
        # transitions will refuse to overwrite them.
        state: str

        def trigger(self, *args, **kwargs):
            ...

    run_timeout: Optional[float] = None  # in normal operation, this will be None, but this lets tests set a max time.

    def __init__(
        self,
    ):

        self.event_loop = asyncio.get_event_loop()
        self.finished_event = asyncio.Event(loop=self.event_loop)

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
    def states(self) -> Collection['str']:
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

        self.update_slack_thread(f"Will {message_verb} in {timeout} seconds, (at {formatted_time})")

        def times_up():
            self.update_slack_thread(f"Time's up, will now {message_verb}.")
            self.trigger(trigger)

        def schedule_callback():
            """Unfortunately, call_at is not threadsafe, and there's no call_at_threadsafe, so we need to schedule the
            call to call_at with call_soon_threadsafe."""
            self.timer_handle = self.event_loop.call_later(timeout, times_up)

        self.event_loop.call_soon_threadsafe(schedule_callback)

    def cancel_timer(self):
        try:
            handle = self.timer_handle
        except AttributeError:
            return
        handle.cancel()

    def before_state_change(self):
        self.cancel_timer()
