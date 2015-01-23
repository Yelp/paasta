"""Kazoo Interfaces"""
from zope.interface import (
    Attribute,
    Interface,
)

# public API


class IHandler(Interface):
    """A Callback Handler for Zookeeper completion and watch callbacks

    This object must implement several methods responsible for
    determining how completion / watch callbacks are handled as well as
    the method for calling :class:`IAsyncResult` callback functions.

    These functions are used to abstract differences between a Python
    threading environment and asynchronous single-threaded environments
    like gevent. The minimum functionality needed for Kazoo to handle
    these differences is encompassed in this interface.

    The Handler should document how callbacks are called for:

    * Zookeeper completion events
    * Zookeeper watch events

    """
    name = Attribute(
        """Human readable name of the Handler interface""")

    timeout_exception = Attribute(
        """Exception class that should be thrown and captured if a
        result is not available within the given time""")

    sleep_func = Attribute(
        """Appropriate sleep function that can be called with a single
        argument and sleep.""")

    def start():
        """Start the handler, used for setting up the handler."""

    def stop():
        """Stop the handler. Should block until the handler is safely
        stopped."""

    def select():
        """A select method that implements Python's select.select
        API"""

    def socket():
        """A socket method that implements Python's socket.socket
        API"""

    def create_connection():
        """A socket method that implements Python's
        socket.create_connection API"""

    def event_object():
        """Return an appropriate object that implements Python's
        threading.Event API"""

    def lock_object():
        """Return an appropriate object that implements Python's
        threading.Lock API"""

    def rlock_object():
        """Return an appropriate object that implements Python's
        threading.RLock API"""

    def async_result():
        """Return an instance that conforms to the
        :class:`~IAsyncResult` interface appropriate for this
        handler"""

    def spawn(func, *args, **kwargs):
        """Spawn a function to run asynchronously

        :param args: args to call the function with.
        :param kwargs: keyword args to call the function with.

        This method should return immediately and execute the function
        with the provided args and kwargs in an asynchronous manner.

        """

    def dispatch_callback(callback):
        """Dispatch to the callback object

        :param callback: A :class:`~kazoo.protocol.states.Callback`
                         object to be called.

        """


class IAsyncResult(Interface):
    """An Async Result object that can be queried for a value that has
    been set asynchronously

    This object is modeled on the ``gevent`` AsyncResult object.

    The implementation must account for the fact that the :meth:`set`
    and :meth:`set_exception` methods will be called from within the
    Zookeeper thread which may require extra care under asynchronous
    environments.

    """
    value = Attribute(
        """Holds the value passed to :meth:`set` if :meth:`set` was
        called. Otherwise `None`""")

    exception = Attribute(
        """Holds the exception instance passed to :meth:`set_exception`
        if :meth:`set_exception` was called. Otherwise `None`""")

    def ready():
        """Return `True` if and only if it holds a value or an
        exception"""

    def successful():
        """Return `True` if and only if it is ready and holds a
        value"""

    def set(value=None):
        """Store the value. Wake up the waiters.

        :param value: Value to store as the result.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken
        up. Sequential calls to :meth:`wait` and :meth:`get` will not
        block at all."""

    def set_exception(exception):
        """Store the exception. Wake up the waiters.

        :param exception: Exception to raise when fetching the value.

        Any waiters blocking on :meth:`get` or :meth:`wait` are woken
        up. Sequential calls to :meth:`wait` and :meth:`get` will not
        block at all."""

    def get(block=True, timeout=None):
        """Return the stored value or raise the exception

        :param block: Whether this method should block or return
                      immediately.
        :type block: bool
        :param timeout: How long to wait for a value when `block` is
                        `True`.
        :type timeout: float

        If this instance already holds a value / an exception, return /
        raise it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional
        timeout occurs."""

    def get_nowait():
        """Return the value or raise the exception without blocking.

        If nothing is available, raise the Timeout exception class on
        the associated :class:`IHandler` interface."""

    def wait(timeout=None):
        """Block until the instance is ready.

        :param timeout: How long to wait for a value when `block` is
                        `True`.
        :type timeout: float

        If this instance already holds a value / an exception, return /
        raise it immediately. Otherwise, block until :meth:`set` or
        :meth:`set_exception` has been called or until the optional
        timeout occurs."""

    def rawlink(callback):
        """Register a callback to call when a value or an exception is
        set

        :param callback:
            A callback function to call after :meth:`set` or
            :meth:`set_exception` has been called. This function will
            be passed a single argument, this instance.
        :type callback: func

        """

    def unlink(callback):
        """Remove the callback set by :meth:`rawlink`

        :param callback: A callback function to remove.
        :type callback: func

        """
