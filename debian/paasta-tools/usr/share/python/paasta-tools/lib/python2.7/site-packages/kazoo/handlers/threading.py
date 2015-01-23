"""A threading based handler.

The :class:`SequentialThreadingHandler` is intended for regular Python
environments that use threads.

.. warning::

    Do not use :class:`SequentialThreadingHandler` with applications
    using asynchronous event loops (like gevent). Use the
    :class:`~kazoo.handlers.gevent.SequentialGeventHandler` instead.

"""
from __future__ import absolute_import

import atexit
import logging
import select
import socket
import threading
import time

try:
    import Queue
except ImportError:  # pragma: nocover
    import queue as Queue

from zope.interface import implementer

from kazoo.handlers.utils import create_tcp_socket, create_tcp_connection
from kazoo.interfaces import IAsyncResult
from kazoo.interfaces import IHandler

# sentinel objects
_NONE = object()
_STOP = object()

log = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


@implementer(IAsyncResult)
class AsyncResult(object):
    """A one-time event that stores a value or an exception"""
    def __init__(self, handler):
        self._handler = handler
        self.value = None
        self._exception = _NONE
        self._condition = threading.Condition()
        self._callbacks = []

    def ready(self):
        """Return true if and only if it holds a value or an
        exception"""
        return self._exception is not _NONE

    def successful(self):
        """Return true if and only if it is ready and holds a value"""
        return self._exception is None

    @property
    def exception(self):
        if self._exception is not _NONE:
            return self._exception

    def set(self, value=None):
        """Store the value. Wake up the waiters."""
        with self._condition:
            self.value = value
            self._exception = None

            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()

    def set_exception(self, exception):
        """Store the exception. Wake up the waiters."""
        with self._condition:
            self._exception = exception

            for callback in self._callbacks:
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
            self._condition.notify_all()

    def get(self, block=True, timeout=None):
        """Return the stored value or raise the exception.

        If there is no value raises TimeoutError.

        """
        with self._condition:
            if self._exception is not _NONE:
                if self._exception is None:
                    return self.value
                raise self._exception
            elif block:
                self._condition.wait(timeout)
                if self._exception is not _NONE:
                    if self._exception is None:
                        return self.value
                    raise self._exception

            # if we get to this point we timeout
            raise TimeoutError()

    def get_nowait(self):
        """Return the value or raise the exception without blocking.

        If nothing is available, raises TimeoutError

        """
        return self.get(block=False)

    def wait(self, timeout=None):
        """Block until the instance is ready."""
        with self._condition:
            self._condition.wait(timeout)
        return self._exception is not _NONE

    def rawlink(self, callback):
        """Register a callback to call when a value or an exception is
        set"""
        with self._condition:
            # Are we already set? Dispatch it now
            if self.ready():
                self._handler.completion_queue.put(
                    lambda: callback(self)
                )
                return

            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def unlink(self, callback):
        """Remove the callback set by :meth:`rawlink`"""
        with self._condition:
            if self.ready():
                # Already triggered, ignore
                return

            if callback in self._callbacks:
                self._callbacks.remove(callback)


@implementer(IHandler)
class SequentialThreadingHandler(object):
    """Threading handler for sequentially executing callbacks.

    This handler executes callbacks in a sequential manner. A queue is
    created for each of the callback events, so that each type of event
    has its callback type run sequentially. These are split into two
    queues, one for watch events and one for async result completion
    callbacks.

    Each queue type has a thread worker that pulls the callback event
    off the queue and runs it in the order the client sees it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper
    client call.

    Watch and completion callbacks should avoid blocking behavior as
    the next callback of that type won't be run until it completes. If
    you need to block, spawn a new thread and return immediately so
    callbacks can proceed.

    .. note::

        Completion callbacks can block to wait on Zookeeper calls, but
        no other completion callbacks will execute until the callback
        returns.

    """
    name = "sequential_threading_handler"
    timeout_exception = TimeoutError
    sleep_func = staticmethod(time.sleep)

    def __init__(self):
        """Create a :class:`SequentialThreadingHandler` instance"""
        self.callback_queue = Queue.Queue()
        self.completion_queue = Queue.Queue()
        self._running = False
        self._state_change = threading.Lock()
        self._workers = []
        atexit.register(self.stop)

    def _create_thread_worker(self, queue):
        def thread_worker():  # pragma: nocover
            while True:
                try:
                    func = queue.get()
                    try:
                        if func is _STOP:
                            break
                        func()
                    except Exception:
                        log.exception("Exception in worker queue thread")
                    finally:
                        queue.task_done()
                except Queue.Empty:
                    continue
        t = threading.Thread(target=thread_worker)

        # Even though these should be joined, it's possible stop might
        # not issue in time so we set them to daemon to let the program
        # exit anyways
        t.daemon = True
        t.start()
        return t

    def start(self):
        """Start the worker threads."""
        with self._state_change:
            if self._running:
                return

            # Spawn our worker threads, we have
            # - A callback worker for watch events to be called
            # - A completion worker for completion events to be called
            for queue in (self.completion_queue, self.callback_queue):
                w = self._create_thread_worker(queue)
                self._workers.append(w)
            self._running = True

    def stop(self):
        """Stop the worker threads and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for queue in (self.completion_queue, self.callback_queue):
                queue.put(_STOP)

            self._workers.reverse()
            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self.callback_queue = Queue.Queue()
            self.completion_queue = Queue.Queue()

    def select(self, *args, **kwargs):
        return select.select(*args, **kwargs)

    def socket(self):
        return create_tcp_socket(socket)

    def create_connection(self, *args, **kwargs):
        return create_tcp_connection(socket, *args, **kwargs)

    def event_object(self):
        """Create an appropriate Event object"""
        return threading.Event()

    def lock_object(self):
        """Create a lock object"""
        return threading.Lock()

    def rlock_object(self):
        """Create an appropriate RLock object"""
        return threading.RLock()

    def async_result(self):
        """Create a :class:`AsyncResult` instance"""
        return AsyncResult(self)

    def spawn(self, func, *args, **kwargs):
        t = threading.Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the
        type as documented for the :class:`SequentialThreadingHandler`.

        """
        self.callback_queue.put(lambda: callback.func(*callback.args))
