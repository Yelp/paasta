import uuid

from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooPartyTests(KazooTestCase):
    def setUp(self):
        super(KazooPartyTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_party(self):
        parties = [self.client.Party(self.path, "p%s" % i)
                   for i in range(5)]

        one_party = parties[0]

        eq_(list(one_party), [])
        eq_(len(one_party), 0)

        participants = set()
        for party in parties:
            party.join()
            participants.add(party.data.decode('utf-8'))

            eq_(set(party), participants)
            eq_(len(party), len(participants))

        for party in parties:
            party.leave()
            participants.remove(party.data.decode('utf-8'))

            eq_(set(party), participants)
            eq_(len(party), len(participants))

    def test_party_reuse_node(self):
        party = self.client.Party(self.path, "p1")
        self.client.ensure_path(self.path)
        self.client.create(party.create_path)
        party.join()
        self.assertTrue(party.participating)
        party.leave()
        self.assertFalse(party.participating)
        self.assertEqual(len(party), 0)

    def test_party_vanishing_node(self):
        party = self.client.Party(self.path, "p1")
        party.join()
        self.assertTrue(party.participating)
        self.client.delete(party.create_path)
        party.leave()
        self.assertFalse(party.participating)
        self.assertEqual(len(party), 0)


class KazooShallowPartyTests(KazooTestCase):
    def setUp(self):
        super(KazooShallowPartyTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_party(self):
        parties = [self.client.ShallowParty(self.path, "p%s" % i)
                   for i in range(5)]

        one_party = parties[0]

        eq_(list(one_party), [])
        eq_(len(one_party), 0)

        participants = set()
        for party in parties:
            party.join()
            participants.add(party.data.decode('utf-8'))

            eq_(set(party), participants)
            eq_(len(party), len(participants))

        for party in parties:
            party.leave()
            participants.remove(party.data.decode('utf-8'))

            eq_(set(party), participants)
            eq_(len(party), len(participants))
