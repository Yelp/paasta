import uuid
import sys
import threading

from nose.tools import eq_

from kazoo.testing import KazooTestCase
from kazoo.tests.util import wait


class UniqueError(Exception):
    """Error raised only by test leader function
    """


class KazooElectionTests(KazooTestCase):
    def setUp(self):
        super(KazooElectionTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

        self.condition = threading.Condition()

        # election contenders set these when elected. The exit event is set by
        # the test to make the leader exit.
        self.leader_id = None
        self.exit_event = None

        # tests set this before the event to make the leader raise an error
        self.raise_exception = False

        # set by a worker thread when an unexpected error is hit.
        # better way to do this?
        self.thread_exc_info = None

    def _spawn_contender(self, contender_id, election):
        thread = threading.Thread(target=self._election_thread,
            args=(contender_id, election))
        thread.daemon = True
        thread.start()
        return thread

    def _election_thread(self, contender_id, election):
        try:
            election.run(self._leader_func, contender_id)
        except UniqueError:
            if not self.raise_exception:
                self.thread_exc_info = sys.exc_info()
        except Exception:
            self.thread_exc_info = sys.exc_info()
        else:
            if self.raise_exception:
                e = Exception("expected leader func to raise exception")
                self.thread_exc_info = (Exception, e, None)

    def _leader_func(self, name):
        exit_event = threading.Event()
        with self.condition:
            self.exit_event = exit_event
            self.leader_id = name
            self.condition.notify_all()

        exit_event.wait(45)
        if self.raise_exception:
            raise UniqueError("expected error in the leader function")

    def _check_thread_error(self):
        if self.thread_exc_info:
            t, o, tb = self.thread_exc_info
            raise t(o)

    def test_election(self):
        elections = {}
        threads = {}
        for _ in range(3):
            contender = "c" + uuid.uuid4().hex
            elections[contender] = self.client.Election(self.path, contender)
            threads[contender] = self._spawn_contender(contender,
                elections[contender])

        # wait for a leader to be elected
        times = 0
        with self.condition:
            while not self.leader_id:
                self.condition.wait(5)
                times += 1
                if times > 5:
                    raise Exception("Still not a leader: lid: %s",
                                    self.leader_id)

        election = self.client.Election(self.path)

        # make sure all contenders are in the pool
        wait(lambda: len(election.contenders()) == len(elections))
        contenders = election.contenders()

        eq_(set(contenders), set(elections.keys()))

        # first one in list should be leader
        first_leader = contenders[0]
        eq_(first_leader, self.leader_id)

        # tell second one to cancel election. should never get elected.
        elections[contenders[1]].cancel()

        # make leader exit. third contender should be elected.
        self.exit_event.set()
        with self.condition:
            while self.leader_id == first_leader:
                self.condition.wait(45)
        eq_(self.leader_id, contenders[2])
        self._check_thread_error()

        # make first contender re-enter the race
        threads[first_leader].join()
        threads[first_leader] = self._spawn_contender(first_leader,
            elections[first_leader])

        # contender set should now be the current leader plus the first leader
        wait(lambda: len(election.contenders()) == 2)
        contenders = election.contenders()
        eq_(set(contenders), set([self.leader_id, first_leader]))

        # make current leader raise an exception. first should be reelected
        self.raise_exception = True
        self.exit_event.set()
        with self.condition:
            while self.leader_id != first_leader:
                self.condition.wait(45)
        eq_(self.leader_id, first_leader)
        self._check_thread_error()

        self.exit_event.set()
        for thread in threads.values():
            thread.join()
        self._check_thread_error()

    def test_bad_func(self):
        election = self.client.Election(self.path)
        self.assertRaises(ValueError, election.run, "not a callable")
