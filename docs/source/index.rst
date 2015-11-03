paasta_tools: PaaSTA Sauce
==========================

paasta_tools is a Python library. It is the sauce that combines the
individual strands of `PaaSTA <http://y/paasta>`_ -- Jenkins, Docker, Mesos,
etc. -- into a cohesive, highly-available, distributed meal.

Directions (For PaaSTA Users)
-----------------------------

.. toctree::
   :maxdepth: 2

   yelpsoa_configs
   workflow


Ingredients
-----------

.. toctree::
   smartstack_interaction

Scripts
~~~~~~~

.. toctree::
   :maxdepth: 2

   generated/paasta_tools.am_i_mesos_leader
   generated/paasta_tools.check_marathon_services_replication
   generated/paasta_tools.cleanup_marathon_jobs
   deploy_marathon_services
   generated/paasta_tools.generate_deployments_for_service
   generated/paasta_tools.generate_services_yaml
   generated/paasta_tools.list_marathon_service_instances
   generated/paasta_tools.paasta_execute_docker_command
   generated/paasta_tools.setup_chronos_job
   generated/paasta_tools.setup_marathon_job
   generated/paasta_tools.synapse_srv_namespaces_fact

Libraries
~~~~~~~~~

.. toctree::
   :maxdepth: 2

   generated/paasta_tools.bounce_lib
   generated/paasta_tools.marathon_tools

Submodules
~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   generated/modules
   monitoring/index
   generated/paasta_tools.paasta_cli

Development
~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   contributing
   style_guide
   upgrading_marathon
   upgrading_mesos

Installing The PaaSTA Infrastructure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   soa_configs
   installation/getting_started


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

