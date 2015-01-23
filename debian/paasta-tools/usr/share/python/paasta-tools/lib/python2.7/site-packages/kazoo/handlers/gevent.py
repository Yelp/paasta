"""A gevent based handler."""
from __future__ import absolute_import

import atexit
import logging

import gevent
import gevent.coros
import gevent.event
import gevent.queue
import gevent.select
import gevent.thread

from gevent.queue import Empty
from gevent.queue import Queue
from gevent import socket
from zope.interface import implementer

from kazoo.handlers.utils import create_tcp_socket, create_tcp_connection
from kazoo.interfaces import IAsyncResult
from kazoo.interfaces import IHandler

_using_libevent = gevent.__version__.startswith('0.')

log = logging.getLogger(__name__)

_STOP = object()

AsyncResult = implementer(IAsyncResult)(gevent.event.AsyncResult)


@implementer(IHandler)
class SequentialGeventHandler(object):
    """Gevent handler for sequentially executing callbacks.

    This handler executes callbacks in a sequential manner. A queue is
    created for each of the callback events, so that each type of event
    has its callback type run sequentially.

    Each queue type has a greenlet worker that pulls the callback event
    off the queue and runs it in the order the client sees it.

    This split helps ensure that watch callbacks won't block session
    re-establishment should the connection be lost during a Zookeeper
    client call.

    Watch callbacks should avoid blocking behavior as the next callback
    of that type won't be run until it completes. If you need to block,
    spawn a new greenlet and return immediately so callbacks can
    proceed.

    """
    name = "sequential_gevent_handler"
    sleep_func = staticmethod(gevent.sleep)

    def __init__(self):
        """Create a :class:`SequentialGeventHandler` instance"""
        self.callback_queue = Queue()
        self._running = False
        self._async = None
        self._state_change = gevent.coros.Semaphore()
        self._workers = []
        atexit.register(self.stop)

    class timeout_exception(gevent.event.Timeout):
        def __init__(self, msg):
            gevent.event.Timeout.__init__(self, exception=msg)

    def _create_greenlet_worker(self, queue):
        def greenlet_worker():
            while True:
                try:
                    func = queue.get()
                    if func is _STOP:
                        break
                    func()
                except Empty:
                    continue
                except Exception as exc:
                    log.warning("Exception in worker greenlet")
                    log.exception(exc)
        return gevent.spawn(greenlet_worker)

    def start(self):
        """Start the greenlet workers."""
        with self._state_change:
            if self._running:
                return

            self._running = True

            # Spawn our worker greenlets, we have
            # - A callback worker for watch events to be called
            for queue in (self.callback_queue,):
                w = self._create_greenlet_worker(queue)
                self._workers.append(w)

    def stop(self):
        """Stop the greenlet workers and empty all queues."""
        with self._state_change:
            if not self._running:
                return

            self._running = False

            for queue in (self.callback_queue,):
                queue.put(_STOP)

            while self._workers:
                worker = self._workers.pop()
                worker.join()

            # Clear the queues
            self.callback_queue = Queue()  # pragma: nocover

    def select(self, *args, **kwargs):
        return gevent.select.select(*args, **kwargs)

    def socket(self, *args, **kwargs):
        return create_tcp_socket(socket)

    def create_connection(self, *args, **kwargs):
        return create_tcp_connection(socket, *args, **kwargs)

    def event_object(self):
        """Create an appropriate Event object"""
        return gevent.event.Event()

    def lock_object(self):
        """Create an appropriate Lock object"""
        return gevent.thread.allocate_lock()

    def rlock_object(self):
        """Create an appropriate RLock object"""
        return gevent.coros.RLock()

    def async_result(self):
        """Create a :class:`AsyncResult` instance

        The :class:`AsyncResult` instance will have its completion
        callbacks executed in the thread the
        :class:`SequentialGeventHandler` is created in (which should be
        the gevent/main thread).

        """
        return AsyncResult()

    def spawn(self, func, *args, **kwargs):
        """Spawn a function to run asynchronously"""
        return gevent.spawn(func, *args, **kwargs)

    def dispatch_callback(self, callback):
        """Dispatch to the callback object

        The callback is put on separate queues to run depending on the
        type as documented for the :class:`SequentialGeventHandler`.

        """
        self.callback_queue.put(lambda: callback.func(*callback.args))
