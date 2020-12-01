import errno
import os
import queue
import signal
import sys
import threading
import time
from functools import wraps
from subprocess import Popen
from types import FrameType
from typing import Any
from typing import Callable
from typing import cast
from typing import Tuple
from typing import TypeVar
from typing import Union

_TimeoutFuncRetType = TypeVar("_TimeoutFuncRetType")


def timeout(
    seconds: int = 10,
    error_message: str = os.strerror(errno.ETIME),
    use_signals: bool = True,
) -> Callable[[Callable[..., _TimeoutFuncRetType]], Callable[..., _TimeoutFuncRetType]]:
    if use_signals:

        def decorate(
            func: Callable[..., _TimeoutFuncRetType]
        ) -> Callable[..., _TimeoutFuncRetType]:
            def _handle_timeout(signum: int, frame: FrameType) -> None:
                raise TimeoutError(error_message)

            def wrapper(*args: Any, **kwargs: Any) -> _TimeoutFuncRetType:
                signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(seconds)
                try:
                    result = func(*args, **kwargs)
                finally:
                    signal.alarm(0)
                return result

            return wraps(func)(wrapper)

    else:

        def decorate(
            func: Callable[..., _TimeoutFuncRetType]
        ) -> Callable[..., _TimeoutFuncRetType]:
            # https://github.com/python/mypy/issues/797
            return _Timeout(func, seconds, error_message)  # type: ignore

    return decorate


class _Timeout:
    def __init__(
        self,
        function: Callable[..., _TimeoutFuncRetType],
        seconds: float,
        error_message: str,
    ) -> None:
        self.seconds = seconds
        self.control: queue.Queue[
            Tuple[bool, Union[_TimeoutFuncRetType, Tuple]]
        ] = queue.Queue()
        self.function = function
        self.error_message = error_message

    def run(self, *args: Any, **kwargs: Any) -> None:
        # Try and put the result of the function into the q
        # if an exception occurs then we put the exc_info instead
        # so that it can be raised in the main thread.
        try:
            self.control.put((True, self.function(*args, **kwargs)))
        except Exception:
            self.control.put((False, sys.exc_info()))

    def __call__(self, *args: Any, **kwargs: Any) -> _TimeoutFuncRetType:
        self.func_thread = threading.Thread(target=self.run, args=args, kwargs=kwargs)
        self.func_thread.daemon = True
        self.timeout = self.seconds + time.time()
        self.func_thread.start()
        return self.get_and_raise()

    def get_and_raise(self) -> _TimeoutFuncRetType:
        while not self.timeout < time.time():
            time.sleep(0.01)
            if not self.func_thread.is_alive():
                ret = self.control.get()
                if ret[0]:
                    return cast(_TimeoutFuncRetType, ret[1])
                else:
                    _, e, tb = cast(Tuple, ret[1])
                    raise e.with_traceback(tb)
        raise TimeoutError(self.error_message)


class TimeoutError(Exception):
    pass


class Timeout:
    # From http://stackoverflow.com/questions/2281850/timeout-function-if-it-takes-too-long-to-finish

    def __init__(self, seconds: int = 1, error_message: str = "Timeout") -> None:
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum: int, frame: FrameType) -> None:
        raise TimeoutError(self.error_message)

    def __enter__(self) -> None:
        self.old_handler = signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self.old_handler)


def _timeout(process: Popen) -> None:
    """Helper function for _run. It terminates the process.
    Doesn't raise OSError, if we try to terminate a non-existing
    process as there can be a very small window between poll() and kill()
    """
    if process.poll() is None:
        try:
            # sending SIGKILL to the process
            process.kill()
        except OSError as e:
            # No such process error
            # The process could have been terminated meanwhile
            if e.errno != errno.ESRCH:
                raise
