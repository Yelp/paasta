==================
PaaSTA Development
==================


What is the PaaSTA playground?
------------------------------

PaaSTA playground allows the developer to run and debug PaaSTA locally on an ephemeral Kubernetes cluster. Currently, it includes the following components:

* generating ``etc_paasta_playground`` directory (stores system-level PaaSTA configuration)
* generating ``soa_config_playground`` directory (stores what services PaaSTA should manage)
* running setup_kubernetes_job (the tool we use to deploy services on Kubernetes)
* starting the PaaSTA API (the backend that the PaaSTA CLI talks to)
* configuring the PaaSTA CLI (the interface to interacting with PaaSTA)

PaaSTA playground use cases
^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are mainly two use cases for PaaSTA playground:

1) :ref:`If the developer wants to debug different components of PaaSTA <debugging-paasta>`
2) :ref:`If the developer just wants to run PaaSTA to see a change they developed in PaaSTA components <running-paasta>`

Create a Kind Cluster - K8s Cluster
-----------------------------------

Before running or debugging PaaSTA playground, you will need to create a Kubernetes cluster using ``kind``. You can do so by running the Make target
``make k8s_fake_cluster``.

To delete the cluster, run ``make k8s_clean``.

.. _debugging-paasta:

Debugging PaaSTA playground (in VS Code)
----------------------------------------

All components of PaaSTA playground are easy to debug in VS Code by running the debugging configurations in ``launch.json`` file. Before you start
debugging, first run ``make vscode_settings``. This Make target will add debugging settings in ``launch.json`` file.

Below outlines the steps to run the debugger in VS Code and to debug PaaSTA playground components.

.. _running-debugger:

Running the debugger (in VS Code)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to run the debugger with any of the configurations in ``launch.json``, you can go to ``Run and Debug`` tab in VS Code

.. image:: run_and_debug_tab.png

and select the configuration you want to run the debugger with, then press on ``start debugging`` button

.. image:: start_debugger.png

For more information on how to run the VS Code debugger, please refer to `VS Code Documentation <https://code.visualstudio.com/docs/editor/debugging>`_

.. _debugging-paasta-playground:

Debugging PaaSTA playground components (in VS Code)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Steps below outline running PaaSTA playground components with a debugger attached:

1. Run ``make generate_paasta_playground`` to run the Make target that generates the ``etc_paasta_playground`` and ``soa_config_playground`` directories and ``deployments.json`` file.
2. [This step can be ran one time] From the ``Run and Debug`` tab in VS Code, press on ``Run setup k8s job in playground`` to deploy ``compute-infra-test-service`` service in the cluster, if it's not deployed.

   Note: we're expecting to see the service in ``pending status`` as zookeeper component doesn't exist yet in paaSTA playground:

   .. sourcecode:: yaml

      (py37-linux) user@dev55-uswest1adevc:~/pg/paasta$ KUBECONFIG=./k8s_itests/kubeconfig kubectl get pods -n paasta
      NAME                                                      READY   STATUS    RESTARTS   AGE
      compute-infra-test-service-autoscaling-7999bd8fcf-6hrb6   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-8cdjm   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-8w7rz   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-dgvmd   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-dgwg9   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-fs7hz   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-hxfjp   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-p7gql   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-q5swd   0/1     Pending   0          20s
      compute-infra-test-service-autoscaling-7999bd8fcf-xhlw2   0/1     Pending   0          20s

3. From the ``Run and Debug`` tab in VS Code, press on ``paasta API playground`` to run PaaSTA API
4. Wait until workers are booted in API output then from the ``Run and Debug`` tab in VS Code, press on ``paasta status playground`` to run the PaaSTA ClI client to connect to the API.

Observe paaSTA API logs to check for connection from the client.

Expected output from client:

 .. sourcecode:: yaml

      compute-infra-test-service.autoscaling in kind-emanelsabban-k8s-test
         Git sha:    xxxxxxxx (desired)
         State:      Configured - Desired state: Started
         Kubernetes:   Critical - up with (0/10) instances (0 evicted). Status: Waiting
            ReplicaSets:
            ReplicaSet Name                                    Ready / Desired  Created at what localtime          Service git SHA                           Config hash
            compute-infra-test-service-autoscaling-7999bd8fcf  0/10             2022-10-11T06:07 (15 minutes ago)  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  configxxxx


.. _running-paasta:

Running PaaSTA playground
-------------------------

There are two ways you can run PaaSTA playground:

If you didn't run ``setup_kubernetes_job`` to deploy ``compute-infra-test-service`` service. Please follow step 2 in section :ref:`Debugging PaaSTA playground components <debugging-paasta-playground>`
before proceeding with the steps below.

1. Using ``launch.json`` file

   1. From the ``Run and Debug`` tab in VS Code, press on ``paasta playground``. This will run all PaaSTA components.


2. Using make targets

   1. Run ``make generate_paasta_playground`` to run the Make target that generates the  ``etc_paasta_playground`` and ``soa_config_playground`` directories and ``deployments.json`` file.
   2. Run ``make playground-api`` to run the Make target that runs PaaSTA API.
   3. Wait until workers are booted in API output then from the ``Run and Debug`` tab in VS Code, press on ``paasta status playground`` to run the PaaSTA ClI client to connect to the API.
