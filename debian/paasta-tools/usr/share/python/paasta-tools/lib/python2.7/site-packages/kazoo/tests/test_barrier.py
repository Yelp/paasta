import threading

from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooBarrierTests(KazooTestCase):
    def test_barrier_not_exist(self):
        b = self.client.Barrier("/some/path")
        eq_(b.wait(), True)

    def test_barrier_exists(self):
        b = self.client.Barrier("/some/path")
        b.create()
        eq_(b.wait(0), False)
        b.remove()
        eq_(b.wait(), True)

    def test_remove_nonexistent_barrier(self):
        b = self.client.Barrier("/some/path")
        eq_(b.remove(), False)


class KazooDoubleBarrierTests(KazooTestCase):

    def test_basic_barrier(self):
        b = self.client.DoubleBarrier("/some/path", 1)
        eq_(b.participating, False)
        b.enter()
        eq_(b.participating, True)
        b.leave()
        eq_(b.participating, False)

    def test_two_barrier(self):
        av = threading.Event()
        ev = threading.Event()
        bv = threading.Event()
        release_all = threading.Event()
        b1 = self.client.DoubleBarrier("/some/path", 2)
        b2 = self.client.DoubleBarrier("/some/path", 2)

        def make_barrier_one():
            b1.enter()
            ev.set()
            release_all.wait()
            b1.leave()
            ev.set()

        def make_barrier_two():
            bv.wait()
            b2.enter()
            av.set()
            release_all.wait()
            b2.leave()
            av.set()

        # Spin up both of them
        t1 = threading.Thread(target=make_barrier_one)
        t1.start()
        t2 = threading.Thread(target=make_barrier_two)
        t2.start()

        eq_(b1.participating, False)
        eq_(b2.participating, False)

        bv.set()
        av.wait()
        ev.wait()
        eq_(b1.participating, True)
        eq_(b2.participating, True)

        av.clear()
        ev.clear()

        release_all.set()
        av.wait()
        ev.wait()
        eq_(b1.participating, False)
        eq_(b2.participating, False)
        t1.join()
        t2.join()

    def test_three_barrier(self):
        av = threading.Event()
        ev = threading.Event()
        bv = threading.Event()
        release_all = threading.Event()
        b1 = self.client.DoubleBarrier("/some/path", 3)
        b2 = self.client.DoubleBarrier("/some/path", 3)
        b3 = self.client.DoubleBarrier("/some/path", 3)

        def make_barrier_one():
            b1.enter()
            ev.set()
            release_all.wait()
            b1.leave()
            ev.set()

        def make_barrier_two():
            bv.wait()
            b2.enter()
            av.set()
            release_all.wait()
            b2.leave()
            av.set()

        # Spin up both of them
        t1 = threading.Thread(target=make_barrier_one)
        t1.start()
        t2 = threading.Thread(target=make_barrier_two)
        t2.start()

        eq_(b1.participating, False)
        eq_(b2.participating, False)

        bv.set()
        eq_(b1.participating, False)
        eq_(b2.participating, False)
        b3.enter()
        ev.wait()
        av.wait()

        eq_(b1.participating, True)
        eq_(b2.participating, True)
        eq_(b3.participating, True)

        av.clear()
        ev.clear()

        release_all.set()
        b3.leave()
        av.wait()
        ev.wait()
        eq_(b1.participating, False)
        eq_(b2.participating, False)
        eq_(b3.participating, False)
        t1.join()
        t2.join()

    def test_barrier_existing_parent_node(self):
        b = self.client.DoubleBarrier('/some/path', 1)
        self.assertFalse(b.participating)
        self.client.create('/some', ephemeral=True)
        # the barrier cannot create children under an ephemeral node
        b.enter()
        self.assertFalse(b.participating)

    def test_barrier_existing_node(self):
        b = self.client.DoubleBarrier('/some', 1)
        self.assertFalse(b.participating)
        self.client.ensure_path(b.path)
        self.client.create(b.create_path, ephemeral=True)
        # the barrier will re-use an existing node
        b.enter()
        self.assertTrue(b.participating)
        b.leave()
