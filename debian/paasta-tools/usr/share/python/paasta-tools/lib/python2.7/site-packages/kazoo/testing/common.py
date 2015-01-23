#
#  Copyright (C) 2010-2011, 2011 Canonical Ltd. All Rights Reserved
#
#  This file was originally taken from txzookeeper and modified later.
#
#  Authors:
#   Kapil Thangavelu and the Kazoo team
#
#  txzookeeper is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  txzookeeper is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with txzookeeper.  If not, see <http://www.gnu.org/licenses/>.


import code
import os
import os.path
import shutil
import signal
import subprocess
import tempfile
import traceback

from itertools import chain
from collections import namedtuple
from glob import glob


def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d = {'_frame': frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)


def listen():
    if os.name != 'nt':  # SIGUSR1 is not supported on Windows
        signal.signal(signal.SIGUSR1, debug)  # Register handler
listen()


def to_java_compatible_path(path):
    if os.name == 'nt':
        path = path.replace('\\', '/')
    return path

ServerInfo = namedtuple(
    "ServerInfo", "server_id client_port election_port leader_port")


class ManagedZooKeeper(object):
    """Class to manage the running of a ZooKeeper instance for testing.

    Note: no attempt is made to probe the ZooKeeper instance is
    actually available, or that the selected port is free. In the
    future, we may want to do that, especially when run in a
    Hudson/Buildbot context, to ensure more test robustness."""

    def __init__(self, software_path, server_info, peers=(), classpath=None):
        """Define the ZooKeeper test instance.

        @param install_path: The path to the install for ZK
        @param port: The port to run the managed ZK instance
        """
        self.install_path = software_path
        self._classpath = classpath
        self.server_info = server_info
        self.host = "127.0.0.1"
        self.peers = peers
        self.working_path = tempfile.mkdtemp()
        self._running = False

    def run(self):
        """Run the ZooKeeper instance under a temporary directory.

        Writes ZK log messages to zookeeper.log in the current directory.
        """
        if self.running:
            return
        config_path = os.path.join(self.working_path, "zoo.cfg")
        log_path = os.path.join(self.working_path, "log")
        log4j_path = os.path.join(self.working_path, "log4j.properties")
        data_path = os.path.join(self.working_path, "data")

        # various setup steps
        if not os.path.exists(self.working_path):
            os.mkdir(self.working_path)
        if not os.path.exists(log_path):
            os.mkdir(log_path)
        if not os.path.exists(data_path):
            os.mkdir(data_path)

        with open(config_path, "w") as config:
            config.write("""
tickTime=2000
dataDir=%s
clientPort=%s
maxClientCnxns=0
""" % (to_java_compatible_path(data_path), self.server_info.client_port))

        # setup a replicated setup if peers are specified
        if self.peers:
            servers_cfg = []
            for p in chain((self.server_info,), self.peers):
                servers_cfg.append("server.%s=localhost:%s:%s" % (
                    p.server_id, p.leader_port, p.election_port))

            with open(config_path, "a") as config:
                config.write("""
initLimit=4
syncLimit=2
%s
""" % ("\n".join(servers_cfg)))

        # Write server ids into datadir
        with open(os.path.join(data_path, "myid"), "w") as myid_file:
            myid_file.write(str(self.server_info.server_id))

        with open(log4j_path, "w") as log4j:
            log4j.write("""
# DEFAULT: console appender only
log4j.rootLogger=INFO, ROLLINGFILE
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.ROLLINGFILE=org.apache.log4j.RollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=DEBUG
log4j.appender.ROLLINGFILE.File=""" + to_java_compatible_path(
                self.working_path + os.sep + "zookeeper.log\n"))

        self.process = subprocess.Popen(
            args=["java",
                  "-cp", self.classpath,
                  "-Dreadonlymode.enabled=true",
                  "-Dzookeeper.log.dir=%s" % log_path,
                  "-Dzookeeper.root.logger=INFO,CONSOLE",
                  "-Dlog4j.configuration=file:%s" % log4j_path,
                  # "-Dlog4j.debug",
                  "org.apache.zookeeper.server.quorum.QuorumPeerMain",
                  config_path])
        self._running = True

    @property
    def classpath(self):
        """Get the classpath necessary to run ZooKeeper."""

        if self._classpath:
            return self._classpath

        # Two possibilities, as seen in zkEnv.sh:
        # Check for a release - top-level zookeeper-*.jar?
        jars = glob((os.path.join(
            self.install_path, 'zookeeper-*.jar')))
        if jars:
            # Release build (`ant package`)
            jars.extend(glob(os.path.join(
                self.install_path,
                "lib/*.jar")))
            # support for different file locations on Debian/Ubuntu
            jars.extend(glob(os.path.join(
                self.install_path,
                "log4j-*.jar")))
            jars.extend(glob(os.path.join(
                self.install_path,
                "slf4j-api-*.jar")))
            jars.extend(glob(os.path.join(
                self.install_path,
                "slf4j-log4j-*.jar")))
        else:
            # Development build (plain `ant`)
            jars = glob((os.path.join(
                self.install_path, 'build/zookeeper-*.jar')))
            jars.extend(glob(os.path.join(
                self.install_path,
                "build/lib/*.jar")))

        return os.pathsep.join(jars)

    @property
    def address(self):
        """Get the address of the ZooKeeper instance."""
        return "%s:%s" % (self.host, self.client_port)

    @property
    def running(self):
        return self._running

    @property
    def client_port(self):
        return self.server_info.client_port

    def reset(self):
        """Stop the zookeeper instance, cleaning out its on disk-data."""
        self.stop()
        shutil.rmtree(os.path.join(self.working_path, "data"))
        os.mkdir(os.path.join(self.working_path, "data"))
        with open(os.path.join(self.working_path, "data", "myid"), "w") as fh:
            fh.write(str(self.server_info.server_id))

    def stop(self):
        """Stop the Zookeeper instance, retaining on disk state."""
        if not self.running:
            return
        self.process.terminate()
        self.process.wait()
        self._running = False

    def destroy(self):
        """Stop the ZooKeeper instance and destroy its on disk-state"""
        # called by at exit handler, reimport to avoid cleanup race.
        import shutil
        self.stop()

        shutil.rmtree(self.working_path)


class ZookeeperCluster(object):

    def __init__(self, install_path=None, classpath=None, size=3, port_offset=20000):
        self._install_path = install_path
        self._classpath = classpath
        self._servers = []

        # Calculate ports and peer group
        port = port_offset
        peers = []

        for i in range(size):
            info = ServerInfo(i + 1, port, port + 1, port + 2)
            peers.append(info)
            port += 10

        # Instantiate Managed ZK Servers
        for i in range(size):
            server_peers = list(peers)
            server_info = server_peers.pop(i)
            self._servers.append(
                ManagedZooKeeper(
                    self._install_path, server_info, server_peers, classpath=self._classpath))

    def __getitem__(self, k):
        return self._servers[k]

    def __iter__(self):
        return iter(self._servers)

    def start(self):
        # Zookeeper client expresses a preference for either lower ports or
        # lexicographical ordering of hosts, to ensure that all servers have a
        # chance to startup, start them in reverse order.
        for server in reversed(list(self)):
            server.run()
        # Giving the servers a moment to start, decreases the overall time
        # required for a client to successfully connect (2s vs. 4s without
        # the sleep).
        import time
        time.sleep(2)

    def stop(self):
        for server in self:
            server.stop()
        self._servers = []

    def terminate(self):
        for server in self:
            server.destroy()

    def reset(self):
        for server in self:
            server.reset()
