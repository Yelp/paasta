import uuid
import time

from nose.tools import eq_

from kazoo.testing import KazooTestCase
from kazoo.recipe.partitioner import PartitionState


class KazooPartitionerTests(KazooTestCase):
    def setUp(self):
        super(KazooPartitionerTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_party_of_one(self):
        partitioner = self.client.SetPartitioner(
            self.path, set=(1, 2, 3), time_boundary=0.2)
        partitioner.wait_for_acquire(14)
        eq_(partitioner.state, PartitionState.ACQUIRED)
        eq_(list(partitioner), [1, 2, 3])
        partitioner.finish()

    def test_party_of_two(self):
        partitioners = [self.client.SetPartitioner(self.path, (1, 2),
                        identifier="p%s" % i, time_boundary=0.2)
                        for i in range(2)]

        partitioners[0].wait_for_acquire(14)
        partitioners[1].wait_for_acquire(14)
        eq_(list(partitioners[0]), [1])
        eq_(list(partitioners[1]), [2])
        partitioners[0].finish()
        time.sleep(0.1)
        eq_(partitioners[1].release, True)
        partitioners[1].finish()

    def test_party_expansion(self):
        partitioners = [self.client.SetPartitioner(self.path, (1, 2, 3),
                        identifier="p%s" % i, time_boundary=0.2)
                        for i in range(2)]

        partitioners[0].wait_for_acquire(14)
        partitioners[1].wait_for_acquire(14)
        eq_(partitioners[0].state, PartitionState.ACQUIRED)
        eq_(partitioners[1].state, PartitionState.ACQUIRED)

        eq_(list(partitioners[0]), [1, 3])
        eq_(list(partitioners[1]), [2])

        # Add another partition, wait till they settle
        partitioners.append(self.client.SetPartitioner(self.path, (1, 2, 3),
                            identifier="p2", time_boundary=0.2))
        time.sleep(0.1)
        eq_(partitioners[0].release, True)
        for p in partitioners[:-1]:
            p.release_set()

        for p in partitioners:
            p.wait_for_acquire(14)

        eq_(list(partitioners[0]), [1])
        eq_(list(partitioners[1]), [2])
        eq_(list(partitioners[2]), [3])

        for p in partitioners:
            p.finish()

    def test_more_members_than_set_items(self):
        partitioners = [self.client.SetPartitioner(self.path, (1,),
                        identifier="p%s" % i, time_boundary=0.2)
                        for i in range(2)]

        partitioners[0].wait_for_acquire(14)
        partitioners[1].wait_for_acquire(14)
        eq_(partitioners[0].state, PartitionState.ACQUIRED)
        eq_(partitioners[1].state, PartitionState.ACQUIRED)

        eq_(list(partitioners[0]), [1])
        eq_(list(partitioners[1]), [])

        for p in partitioners:
            p.finish()

    def test_party_session_failure(self):
        partitioner = self.client.SetPartitioner(
            self.path, set=(1, 2, 3), time_boundary=0.2)
        partitioner.wait_for_acquire(14)
        eq_(partitioner.state, PartitionState.ACQUIRED)
        # simulate session failure
        partitioner._fail_out()
        partitioner.release_set()
        self.assertTrue(partitioner.failed)
