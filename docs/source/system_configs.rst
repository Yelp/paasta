System Paasta Configs
=====================

The "System Paasta Configs" inform Paasta about your environment and cluster setup, such as how to connect to
Marathon/hacheck/etc, what the cluster name is, etc.


Structure
---------

By default, the System Paasta Configs are read by merging all files under ``/etc/paasta`` that end with ``.json``.
You can override this path by setting the ``PAASTA_SYSTEM_CONFIG_DIR`` environment variable.
This directory is searched recursively, so you may put configuration files in subdirectories as desired.
The dictionaries described by each of these JSON files are deep merged when the config is loaded. If there
are overlapping keys in different files the last file will win.

If a file has permissions that prevent us from reading it, then that file will be ignored.
This is useful for credentials that only some users or scripts need access to.

See `load_system_paasta_config <generated/paasta_tools.utils.html#paasta_tools.utils.load_system_paasta_config>`_ for
more details on how system configs are loaded.


Configuration options
---------------------

These are the keys that may exist in system configs:

  * ``zookeeper``: A zookeeper connection url, used for discovering where the Mesos leader is, and some locks.
    Example: ``"zookeeper": "zk://zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/mesos"``.

  * ``docker_registry``: The name of the docker registry where paasta images will be stored. This can optionally
    be set on a per-service level as well, see `yelpsoa_configs <yelpsoa_configs.html#service-yaml>`_
    Example: ``"docker_registry": "docker-paasta.yelpcorp.com:443"``

  * ``volumes``: The list of volumes that should be bind-mounted into application containers by default.
    Each volume should have attributes ``hostPath``, ``containerPath``, and ``mode`` (which can be RO or RW).
    Example: ``"volumes": [{"hostPath": "/foo", "containerPath": "/foo", "mode": "RO"}]``

  * ``cluster``: The name of the cluster this node is on.
    Example: ``"cluster": "uswest1-prod"``

  * ``dashboard_links``: A nested dictionary of cluster -> description -> URL of dashboard links.
    These are printed at the top of ``paasta metastatus``.
    Example::

      "dashboard_links": {
        "uswest1-prod": {
          "Mesos": "http://mesos.paasta-uswest1-prod.yelpcorp.com",
          "Cluster charts": "http://kibana.yelpcorp.com/something",
        }
      }

  * ``fsm_template``: A path to a cookiecutter template directory for ``paasta fsm``.
    Defaults to ``paasta_tools/cli/fsm/template``, relative to the installed path of Paasta.

  * ``log_writer``: Configuration for how to write per-service logs.
    This should be a dictionary with two keys: ``driver`` and ``options``.
    ``driver`` is a string specifying which log writer you want to use.
    ``options`` is a dictionary, but the values depend on the arguments to the driver you chose.

    There are currently three log_writer drivers available: ``scribe``, ``file``, and ``null``.

    Example::

      "log_writer": {
        "driver": "file",
        "options": {
          "path_format": "/var/log/paasta_logs/{service}.log"
        }
      }

  * ``log_reader``: Configuration for how ``paasta logs`` should read logs.
    This should be a dictionary with two keys: ``driver`` and ``options``.
    ``driver`` is a string specifying which log reader you want to use.
    ``options`` is a dictionary, but the values depend on the arguments to the driver you chose.

    There is currently one log_reader driver available: ``scribereader``, which only really works at Yelp. Sorry.

    Example::

      "log_reader": {
        "driver": "scribereader",
        "options": {
          "cluster_map": {
            "uswest1-prod": "sfo2",
          }
        }
      }

  * ``sensu_host``: The hostname or IP address of a Sensu client that we should send events to.
    Defaults to ``localhost``.

    Example: ``"sensu_host": "169.254.255.254"``

  * ``sensu_port``: The port number of a Sensu client that we should send events to.
    Defaults to ``3030``.

    Example: ``"sensu_port": 3031``

  * ``dockercfg_location``: A URI of a .dockercfg file, to allow mesos slaves
    to authenticate with the docker registry.
    Defaults to ``file:///root/.dockercfg``.
    While this must be set, this file can contain an empty JSON dictionary (``{}``) if your docker registry does not
    require authentication.
    May use any URL scheme supported by Mesos's `fetcher module. <http://mesos.apache.org/documentation/latest/fetcher/>`_

    Example: ``"dockercfg_location": "http://somehost/somepath"``

  * ``synapse_port``: The port that haproxy-synapse exposes its status on.
    Defaults to ``3212``.

    Example: ``"synapse_port": 3213``

  * ``synapse_host``: The default host that paasta should interrogate for haproxy-synapse state.
    Defaults to ``localhost``.
    Primarily used in `check_marathon_services_replication <generated/paasta_tools.check_marathon_services_replication.html>`_.

    Example: ``"synapse_host": 169.254.255.254``

  * ``synapse_haproxy_url_format``: A python format string for constructing the URL of haproxy-synapse's status page.
    This format string gets two parameters, ``host`` and ``port``.
    Defaults to ``"http://{host:s}:{port:d}/;csv;norefresh".``

    Example: ``"synapse_haproxy_url_format": "http://{host:s}:{port:d}/status"``

  * ``cluster_fqdn_format``: A python format string for constructing a hostname that resolves to the masters for a given
    cluster.
    This format string gets one parameter: ``cluster``.
    This is used by ``paasta status`` to know where to SSH to run ``paasta_serviceinit``.
    Defaults to ``paasta-{cluster:s}.yelp``.

    Example: ``"cluster_fqdn_format": "paasta-{cluster:s}.service.dc1.consul"``
