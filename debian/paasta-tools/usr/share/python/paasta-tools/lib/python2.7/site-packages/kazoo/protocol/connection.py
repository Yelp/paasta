"""Zookeeper Protocol Connection Handler"""
import logging
import itertools
import os
import random
import select
import socket
import sys
import time
from binascii import hexlify
from contextlib import contextmanager

from kazoo.exceptions import (
    AuthFailedError,
    ConnectionDropped,
    EXCEPTIONS,
    SessionExpiredError,
    NoNodeError
)
from kazoo.handlers.utils import create_pipe
from kazoo.protocol.serialization import (
    Auth,
    Close,
    Connect,
    Exists,
    GetChildren,
    Ping,
    PingInstance,
    ReplyHeader,
    Transaction,
    Watch,
    int_struct
)
from kazoo.protocol.states import (
    Callback,
    KeeperState,
    WatchedEvent,
    EVENT_TYPE_MAP,
)
from kazoo.retry import (
    ForceRetryError,
    RetryFailedError
)

log = logging.getLogger(__name__)


# Special testing hook objects used to force a session expired error as
# if it came from the server
_SESSION_EXPIRED = object()
_CONNECTION_DROP = object()

STOP_CONNECTING = object()

CREATED_EVENT = 1
DELETED_EVENT = 2
CHANGED_EVENT = 3
CHILD_EVENT = 4

WATCH_XID = -1
PING_XID = -2
AUTH_XID = -4

CLOSE_RESPONSE = Close.type

if sys.version_info > (3, ):  # pragma: nocover
    def buffer(obj, offset=0):
        return memoryview(obj)[offset:]

    advance_iterator = next
else:  # pragma: nocover
    def advance_iterator(it):
        return it.next()


class RWPinger(object):
    """A Read/Write Server Pinger Iterable

    This object is initialized with the hosts iterator object and the
    socket creation function. Anytime `next` is called on its iterator
    it yields either False, or a host, port tuple if it found a r/w
    capable Zookeeper node.

    After the first run-through of hosts, an exponential back-off delay
    is added before the next run. This delay is tracked internally and
    the iterator will yield False if called too soon.

    """
    def __init__(self, hosts, connection_func, socket_handling):
        self.hosts = hosts
        self.connection = connection_func
        self.last_attempt = None
        self.socket_handling = socket_handling

    def __iter__(self):
        if not self.last_attempt:
            self.last_attempt = time.time()
        delay = 0.5
        while True:
            yield self._next_server(delay)

    def _next_server(self, delay):
        jitter = random.randint(0, 100) / 100.0
        while time.time() < self.last_attempt + delay + jitter:
            # Skip rw ping checks if its too soon
            return False
        for host, port in self.hosts:
            log.debug("Pinging server for r/w: %s:%s", host, port)
            self.last_attempt = time.time()
            try:
                with self.socket_handling():
                    sock = self.connection((host, port))
                    sock.sendall(b"isro")
                    result = sock.recv(8192)
                    sock.close()
                    if result == b'rw':
                        return (host, port)
                    else:
                        return False
            except ConnectionDropped:
                return False

            # Add some jitter between host pings
            while time.time() < self.last_attempt + jitter:
                return False
        delay *= 2


class RWServerAvailable(Exception):
    """Thrown if a RW Server becomes available"""


class ConnectionHandler(object):
    """Zookeeper connection handler"""
    def __init__(self, client, retry_sleeper, logger=None):
        self.client = client
        self.handler = client.handler
        self.retry_sleeper = retry_sleeper
        self.logger = logger or log

        # Our event objects
        self.connection_closed = client.handler.event_object()
        self.connection_closed.set()
        self.connection_stopped = client.handler.event_object()
        self.connection_stopped.set()
        self.ping_outstanding = client.handler.event_object()

        self._read_pipe = None
        self._write_pipe = None

        self._socket = None
        self._xid = None
        self._rw_server = None
        self._ro_mode = False

        self._connection_routine = None

    # This is instance specific to avoid odd thread bug issues in Python
    # during shutdown global cleanup
    @contextmanager
    def _socket_error_handling(self):
        try:
            yield
        except (socket.error, select.error) as e:
            err = getattr(e, 'strerror', e)
            raise ConnectionDropped("socket connection error: %s" % (err,))

    def start(self):
        """Start the connection up"""
        if self.connection_closed.is_set():
            self._read_pipe, self._write_pipe = create_pipe()
            self.connection_closed.clear()
        if self._connection_routine:
            raise Exception("Unable to start, connection routine already "
                            "active.")
        self._connection_routine = self.handler.spawn(self.zk_loop)

    def stop(self, timeout=None):
        """Ensure the writer has stopped, wait to see if it does."""
        self.connection_stopped.wait(timeout)
        if self._connection_routine:
            self._connection_routine.join()
            self._connection_routine = None
        return self.connection_stopped.is_set()

    def close(self):
        """Release resources held by the connection

        The connection can be restarted afterwards.
        """
        if not self.connection_stopped.is_set():
            raise Exception("Cannot close connection until it is stopped")
        self.connection_closed.set()
        wp, rp = self._write_pipe, self._read_pipe
        self._write_pipe = self._read_pipe = None
        os.close(wp)
        os.close(rp)

    def _server_pinger(self):
        """Returns a server pinger iterable, that will ping the next
        server in the list, and apply a back-off between attempts."""
        return RWPinger(self.client.hosts, self.handler.create_connection,
                        self._socket_error_handling)

    def _read_header(self, timeout):
        b = self._read(4, timeout)
        length = int_struct.unpack(b)[0]
        b = self._read(length, timeout)
        header, offset = ReplyHeader.deserialize(b, 0)
        return header, b, offset

    def _read(self, length, timeout):
        msgparts = []
        remaining = length
        with self._socket_error_handling():
            while remaining > 0:
                s = self.handler.select([self._socket], [], [], timeout)[0]
                if not s:  # pragma: nocover
                    # If the read list is empty, we got a timeout. We don't
                    # have to check wlist and xlist as we don't set any
                    raise self.handler.timeout_exception("socket time-out")

                chunk = self._socket.recv(remaining)
                if chunk == b'':
                    raise ConnectionDropped('socket connection broken')
                msgparts.append(chunk)
                remaining -= len(chunk)
            return b"".join(msgparts)

    def _invoke(self, timeout, request, xid=None):
        """A special writer used during connection establishment
        only"""
        self._submit(request, timeout, xid)
        zxid = None
        if xid:
            header, buffer, offset = self._read_header(timeout)
            if header.xid != xid:
                raise RuntimeError('xids do not match, expected %r received %r',
                                   xid, header.xid)
            if header.zxid > 0:
                zxid = header.zxid
            if header.err:
                callback_exception = EXCEPTIONS[header.err]()
                self.logger.info('Received error(xid=%s) %r', xid, callback_exception)
                raise callback_exception
            return zxid

        msg = self._read(4, timeout)
        length = int_struct.unpack(msg)[0]
        msg = self._read(length, timeout)

        if hasattr(request, 'deserialize'):
            try:
                obj, _ = request.deserialize(msg, 0)
            except Exception:
                self.logger.exception("Exception raised during deserialization"
                                      " of request: %s", request)

                # raise ConnectionDropped so connect loop will retry
                raise ConnectionDropped('invalid server response')
            self.logger.debug('Read response %s', obj)
            return obj, zxid

        return zxid

    def _submit(self, request, timeout, xid=None):
        """Submit a request object with a timeout value and optional
        xid"""
        b = bytearray()
        if xid:
            b.extend(int_struct.pack(xid))
        if request.type:
            b.extend(int_struct.pack(request.type))
        b += request.serialize()
        self.logger.log((logging.DEBUG if isinstance(request, Ping) else logging.INFO),
                        "Sending request(xid=%s): %s", xid, request)
        self._write(int_struct.pack(len(b)) + b, timeout)

    def _write(self, msg, timeout):
        """Write a raw msg to the socket"""
        sent = 0
        msg_length = len(msg)
        with self._socket_error_handling():
            while sent < msg_length:
                s = self.handler.select([], [self._socket], [], timeout)[1]
                if not s:  # pragma: nocover
                    # If the write list is empty, we got a timeout. We don't
                    # have to check rlist and xlist as we don't set any
                    raise self.handler.timeout_exception("socket time-out")
                msg_slice = buffer(msg, sent)
                bytes_sent = self._socket.send(msg_slice)
                if not bytes_sent:
                    raise ConnectionDropped('socket connection broken')
                sent += bytes_sent

    def _read_watch_event(self, buffer, offset):
        client = self.client
        watch, offset = Watch.deserialize(buffer, offset)
        path = watch.path

        self.logger.info('Received EVENT: %s', watch)

        watchers = []

        if watch.type in (CREATED_EVENT, CHANGED_EVENT):
            watchers.extend(client._data_watchers.pop(path, []))
        elif watch.type == DELETED_EVENT:
            watchers.extend(client._data_watchers.pop(path, []))
            watchers.extend(client._child_watchers.pop(path, []))
        elif watch.type == CHILD_EVENT:
            watchers.extend(client._child_watchers.pop(path, []))
        else:
            self.logger.warn('Received unknown event %r', watch.type)
            return

        # Strip the chroot if needed
        path = client.unchroot(path)
        ev = WatchedEvent(EVENT_TYPE_MAP[watch.type], client._state, path)

        # Last check to ignore watches if we've been stopped
        if client._stopped.is_set():
            return

        # Dump the watchers to the watch thread
        for watch in watchers:
            client.handler.dispatch_callback(Callback('watch', watch, (ev,)))

    def _read_response(self, header, buffer, offset):
        client = self.client
        request, async_object, xid = client._pending.popleft()
        if header.zxid and header.zxid > 0:
            client.last_zxid = header.zxid
        if header.xid != xid:
            raise RuntimeError('xids do not match, expected %r '
                               'received %r', xid, header.xid)

        # Determine if its an exists request and a no node error
        exists_error = (header.err == NoNodeError.code and
                        request.type == Exists.type)

        # Set the exception if its not an exists error
        if header.err and not exists_error:
            callback_exception = EXCEPTIONS[header.err]()
            self.logger.info('Received error(xid=%s) %r', xid, callback_exception)
            if async_object:
                async_object.set_exception(callback_exception)
        elif request and async_object:
            if exists_error:
                # It's a NoNodeError, which is fine for an exists
                # request
                async_object.set(None)
            else:
                try:
                    response = request.deserialize(buffer, offset)
                except Exception as exc:
                    self.logger.exception("Exception raised during deserialization"
                                          " of request: %s", request)
                    async_object.set_exception(exc)
                    return
                self.logger.info('Received response(xid=%s): %r', xid, response)

                # We special case a Transaction as we have to unchroot things
                if request.type == Transaction.type:
                    response = Transaction.unchroot(client, response)

                async_object.set(response)

            # Determine if watchers should be registered
            watcher = getattr(request, 'watcher', None)
            if not client._stopped.is_set() and watcher:
                if isinstance(request, GetChildren):
                    client._child_watchers[request.path].add(watcher)
                else:
                    client._data_watchers[request.path].add(watcher)

        if isinstance(request, Close):
            self.logger.debug('Read close response')
            return CLOSE_RESPONSE

    def _read_socket(self, read_timeout):
        """Called when there's something to read on the socket"""
        client = self.client

        header, buffer, offset = self._read_header(read_timeout)
        if header.xid == PING_XID:
            self.logger.debug('Received Ping')
            self.ping_outstanding.clear()
        elif header.xid == AUTH_XID:
            self.logger.debug('Received AUTH')

            if header.err:
                # We go ahead and fail out the connection, mainly because
                # thats what Zookeeper client docs think is appropriate

                # XXX TODO: Should we fail out? Or handle auth failure
                # differently here since the session id is actually valid!
                client._session_callback(KeeperState.AUTH_FAILED)
        elif header.xid == WATCH_XID:
            self._read_watch_event(buffer, offset)
        else:
            self.logger.debug('Reading for header %r', header)

            return self._read_response(header, buffer, offset)

    def _send_request(self, read_timeout, connect_timeout):
        """Called when we have something to send out on the socket"""
        client = self.client
        try:
            request, async_object = client._queue[0]
        except IndexError:
            # Not actually something on the queue, this can occur if
            # something happens to cancel the request such that we
            # don't clear the pipe below after sending
            try:
                # Clear possible inconsistence (no request in the queue
                # but have data in the read pipe), which causes cpu to spin.
                os.read(self._read_pipe, 1)
            except OSError:
                pass
            return

        # Special case for testing, if this is a _SessionExpire object
        # then throw a SessionExpiration error as if we were dropped
        if request is _SESSION_EXPIRED:
            raise SessionExpiredError("Session expired: Testing")
        if request is _CONNECTION_DROP:
            raise ConnectionDropped("Connection dropped: Testing")

        # Special case for auth packets
        if request.type == Auth.type:
            self._submit(request, connect_timeout, AUTH_XID)
            client._queue.popleft()
            os.read(self._read_pipe, 1)
            return

        self._xid += 1

        self._submit(request, connect_timeout, self._xid)
        client._queue.popleft()
        os.read(self._read_pipe, 1)
        client._pending.append((request, async_object, self._xid))

    def _send_ping(self, connect_timeout):
        self.ping_outstanding.set()
        self._submit(PingInstance, connect_timeout, PING_XID)

        # Determine if we need to check for a r/w server
        if self._ro_mode:
            result = advance_iterator(self._ro_mode)
            if result:
                self._rw_server = result
                raise RWServerAvailable()

    def zk_loop(self):
        """Main Zookeeper handling loop"""
        self.logger.debug('ZK loop started')

        self.connection_stopped.clear()

        retry = self.retry_sleeper.copy()
        try:
            hosts = itertools.cycle(self.client.hosts)
            while not self.client._stopped.is_set():
                # If the connect_loop returns STOP_CONNECTING, stop retrying
                if retry(self._connect_loop, hosts, retry) is STOP_CONNECTING:
                    break
        except RetryFailedError:
            self.logger.warning("Failed connecting to Zookeeper "
                                "within the connection retry policy.")
            self.client._session_callback(KeeperState.CLOSED)
        finally:
            self.connection_stopped.set()
            self.logger.debug('Connection stopped')

    def _connect_loop(self, hosts, retry):
        # Iterate through the hosts a full cycle before starting over
        total_hosts = len(self.client.hosts)
        cur = 0
        status = None
        while cur < total_hosts and status is not STOP_CONNECTING:
            if self.client._stopped.is_set():
                status = STOP_CONNECTING
                break
            status = self._connect_attempt(hosts, retry)
            cur += 1
        if status is STOP_CONNECTING:
            return STOP_CONNECTING
        else:
            raise ForceRetryError('Reconnecting')

    def _connect_attempt(self, hosts, retry):
        client = self.client
        TimeoutError = self.handler.timeout_exception
        close_connection = False

        self._socket = None
        host, port = advance_iterator(hosts)

        # Were we given a r/w server? If so, use that instead
        if self._rw_server:
            self.logger.debug("Found r/w server to use, %s:%s", host, port)
            host, port = self._rw_server
            self._rw_server = None

        if client._state != KeeperState.CONNECTING:
            client._session_callback(KeeperState.CONNECTING)

        try:
            read_timeout, connect_timeout = self._connect(host, port)
            read_timeout = read_timeout / 1000.0
            connect_timeout = connect_timeout / 1000.0
            retry.reset()
            self._xid = 0

            while not close_connection:
                # Watch for something to read or send
                timeout = read_timeout / 2.0 - random.randint(0, 40) / 100.0
                s = self.handler.select([self._socket, self._read_pipe],
                                        [], [], timeout)[0]

                if not s:
                    if self.ping_outstanding.is_set():
                        self.ping_outstanding.clear()
                        raise ConnectionDropped(
                            "outstanding heartbeat ping not received")
                    self._send_ping(connect_timeout)
                elif s[0] == self._socket:
                    response = self._read_socket(read_timeout)
                    close_connection = response == CLOSE_RESPONSE
                else:
                    self._send_request(read_timeout, connect_timeout)

            self.logger.info('Closing connection to %s:%s', host, port)
            client._session_callback(KeeperState.CLOSED)
            return STOP_CONNECTING
        except (ConnectionDropped, TimeoutError) as e:
            if isinstance(e, ConnectionDropped):
                self.logger.warning('Connection dropped: %s', e)
            else:
                self.logger.warning('Connection time-out')
            if client._state != KeeperState.CONNECTING:
                self.logger.warning("Transition to CONNECTING")
                client._session_callback(KeeperState.CONNECTING)
        except AuthFailedError:
            retry.reset()
            self.logger.warning('AUTH_FAILED closing')
            client._session_callback(KeeperState.AUTH_FAILED)
            return STOP_CONNECTING
        except SessionExpiredError:
            retry.reset()
            self.logger.warning('Session has expired')
            client._session_callback(KeeperState.EXPIRED_SESSION)
        except RWServerAvailable:
            retry.reset()
            self.logger.warning('Found a RW server, dropping connection')
            client._session_callback(KeeperState.CONNECTING)
        except Exception:
            self.logger.exception('Unhandled exception in connection loop')
            raise
        finally:
            if self._socket is not None:
                self._socket.close()

    def _connect(self, host, port):
        client = self.client
        self.logger.info('Connecting to %s:%s', host, port)

        self.logger.debug('    Using session_id: %r session_passwd: %s',
                          client._session_id,
                          hexlify(client._session_passwd))

        with self._socket_error_handling():
            self._socket = self.handler.create_connection(
                (host, port), client._session_timeout / 1000.0)

        self._socket.setblocking(0)

        connect = Connect(0, client.last_zxid, client._session_timeout,
                          client._session_id or 0, client._session_passwd,
                          client.read_only)

        connect_result, zxid = self._invoke(client._session_timeout, connect)

        if connect_result.time_out <= 0:
            raise SessionExpiredError("Session has expired")

        if zxid:
            client.last_zxid = zxid

        # Load return values
        client._session_id = connect_result.session_id
        negotiated_session_timeout = connect_result.time_out
        connect_timeout = negotiated_session_timeout / len(client.hosts)
        read_timeout = negotiated_session_timeout * 2.0 / 3.0
        client._session_passwd = connect_result.passwd

        self.logger.debug('Session created, session_id: %r session_passwd: %s\n'
                          '    negotiated session timeout: %s\n'
                          '    connect timeout: %s\n'
                          '    read timeout: %s', client._session_id,
                          hexlify(client._session_passwd),
                          negotiated_session_timeout, connect_timeout,
                          read_timeout)

        if connect_result.read_only:
            client._session_callback(KeeperState.CONNECTED_RO)
            self._ro_mode = iter(self._server_pinger())
        else:
            client._session_callback(KeeperState.CONNECTED)
            self._ro_mode = None

        for scheme, auth in client.auth_data:
            ap = Auth(0, scheme, auth)
            zxid = self._invoke(connect_timeout, ap, xid=AUTH_XID)
            if zxid:
                client.last_zxid = zxid
        return read_timeout, connect_timeout
