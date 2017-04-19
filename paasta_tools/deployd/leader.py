from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time

from kazoo.client import KazooState
from kazoo.exceptions import ConnectionClosedError
from kazoo.recipe.election import Election

from paasta_tools.deployd.common import PaastaThread


class PaastaLeaderElection(Election):

    def __init__(self, client, *args, **kwargs):
        self.client = client
        self.control = kwargs.pop('control')
        super(PaastaLeaderElection, self).__init__(self.client, *args, **kwargs)
        self.client.add_listener(self.connection_listener)
        self.waiting_for_reconnect = False

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def run(self, func, *args, **kwargs):
        try:
            super(PaastaLeaderElection, self).run(func, *args, **kwargs)
        except ConnectionClosedError:
            self.log.error("Zookeeper connection closed so can't tidy up!")
            return

    def connection_listener(self, state):
        self.log.warning("Zookeeper connection transitioned to: {}".format(state))
        if state == KazooState.SUSPENDED:
            self.log.warning("Zookeeper connection suspended, waiting to see if it recovers.")
            if not self.waiting_for_reconnect:
                self.waiting_for_reconnect = True
                reconnection_checker = PaastaThread(target=self.reconnection_listener)
                reconnection_checker.daemon = True
                reconnection_checker.start()
        elif state == KazooState.LOST:
            self.log.error("Leadership lost, quitting!")
            self.control.put("ABORT")
            self.client.stop()

    def reconnection_listener(self):
        attempts = 0
        while attempts < 5:
            if self.client.state == KazooState.CONNECTED:
                self.log.warning("Zookeeper connection recovered!")
                self.waiting_for_reconnect = False
                return
            self.log.warning("Waiting for zookeeper connection to recover")
            time.sleep(5)
            attempts += 1
        self.log.error("Connection did not revocer, abdicating!")
        self.control.put("ABORT")
        self.client.stop()
