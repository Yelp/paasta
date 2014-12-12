.. service_deployment_tools documentation master file, created by
   sphinx-quickstart on Tue Aug  5 17:43:04 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

service_deployment_tools: PaaSTA sauce
======================================

service_deployment_tools is a Python library. It is the sauce that combines the
individual strands of `PaaSTA <http://y/paasta>`_ -- Jenkins, Docker, Mesos,
etc. -- into a cohesive, highly-available, distributed meal.

Directions
----------

.. toctree::
   :maxdepth: 2

   yelpsoa_configs
   workflow


Ingredients
-----------

Scripts
~~~~~~~

.. toctree::
   :maxdepth: 2

   am_i_mesos_leader
   check_marathon_services_frontends
   check_marathon_services_replication
   cleanup_marathon_jobs
   cleanup_marathon_orphaned_containers
   deploy_marathon_services
   generate_deployments_json
   generate_services_yaml
   list_marathon_service_instances
   setup_chronos_jobs
   setup_marathon_job
   synapse_srv_namespaces_fact

Libraries
~~~~~~~~~

.. toctree::
   :maxdepth: 2

   bounce_lib
   marathon_tools

Submodules
~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   monitoring/index

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

