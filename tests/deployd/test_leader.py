import unittest

import mock
from kazoo.client import KazooState

from paasta_tools.deployd.leader import PaastaLeaderElection


class TestPaastaLeaderElection(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "paasta_tools.deployd.leader.Election.__init__", autospec=False
        ):
            self.mock_client = mock.Mock()
            self.mock_control = mock.Mock()
            self.election = PaastaLeaderElection(
                self.mock_client, control=self.mock_control
            )

    def test_init(self):
        assert not self.election.waiting_for_reconnect

    def test_log(self):
        self.election.log.info("THING")

    def test_run(self):
        mock_fun = mock.Mock()
        mock_arg = mock.Mock()
        with mock.patch(
            "paasta_tools.deployd.leader.Election.run", autospec=True
        ) as mock_kazoo_election:
            self.election.run(mock_fun, mock_arg)
            mock_kazoo_election.assert_called_with(self.election, mock_fun, mock_arg)

    def test_connection_listener(self):
        with mock.patch(
            "paasta_tools.deployd.leader.PaastaThread", autospec=True
        ) as mock_paasta_thread:
            self.election.connection_listener(KazooState.CONNECTED)
            self.election.connection_listener(KazooState.SUSPENDED)
            mock_paasta_thread.assert_called_with(
                target=self.election.reconnection_listener
            )
            assert self.election.waiting_for_reconnect
            self.election.connection_listener(KazooState.LOST)
            self.mock_control.put.assert_called_with("ABORT")
            assert self.mock_client.stop.called

    def test_reconnection_listener(self):
        self.mock_client.state = KazooState.CONNECTED
        self.election.reconnection_listener()
        assert not self.election.waiting_for_reconnect
        assert not self.mock_control.put.called

        self.mock_client.state = KazooState.SUSPENDED
        self.election.waiting_for_reconnect = True
        with mock.patch("time.sleep", autospec=True):
            self.election.reconnection_listener()
        assert self.election.waiting_for_reconnect
        self.mock_control.put.assert_called_with("ABORT")
