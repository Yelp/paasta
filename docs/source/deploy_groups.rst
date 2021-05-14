=============
Deploy Groups
=============

What are deploy groups?
========================

A deploy group is a group of PaaSTA instances that will be deployed together.
The ``kubernetes-cluster.yaml``, ``tron-cluster.yaml``, and ``adhoc-cluster.yaml`` files should have a ``deploy_group`` field on each instance.
The ``paasta mark-for-deployment`` command (usually run by Jenkins) operates on deploy groups -- it tells PaaSTA that you want a deploy group to run a specific version of your service.
In ``deploy.yaml``, you specify the order in which the deploy groups pick up new changes.

As an example, consider a service with the following deploy.yaml:

.. sourcecode:: yaml

   ---
   pipeline:
   - step: itest
   - step: security-check
   - step: push-to-registry
   - step: performance-check
   - step: dev-stage.everything
     trigger_next_step_manually: true
   - step: prod.canary
     trigger_next_step_manually: true
   - step: prod.non_canary

This pipeline will:

1. Run ``itest``, ``security-check``, ``push-to-registry``, and ``performance-check`` steps, which are build and testing steps.
   During ``itest`` phase, a new container image is built (per the `Paasta Contract <about/contract.html>`_).
   This image is pushed to Paasta's Docker registry in the ``push-to-registry`` step.
2. Deploy the new container image to all instances with ``deploy_group: dev-stage.everything``, and wait for someone to click a button in Jenkins before continuing.
3. Deploy the new container image to all instances with ``deploy_group: prod.canary``, and wait for someone to click a button in Jenkins before continuing.
4. Deploy the new container image to all instances with ``prod.non_canary``.


Deploy groups in kubernetes/tron yamls and deploy.yaml should match
-------------------------------------------------------------------

In almost all cases, you want the list of deploy groups in ``deploy.yaml`` (the ``step`` entries under ``pipeline``, except for the special build/test steps) to match the set of ``deploy_group``s defined in your kubernetes.yaml / tron.yaml / adhoc.yaml.
If an instance has a ``deploy_group`` that is not defined in deploy.yaml, or your Jenkins pipeline has not run since you added the deploy.yaml entry, PaaSTA won't know what version of your container image this instance should run.
If a deploy group is specified as a ``step`` in deploy.yaml but is not referenced in any kubernetes/adhoc/tron.yaml, this deployment step will have no effect.

The ``paasta validate`` command can help you check that the ``deploy_group`` parameter on each of your instances is defined in deploy.yaml.

Deploy group names are (mostly) just arbitrary strings
------------------------------------------------------

Deploy group names can be any string you like -- they do not need to match any pattern.
However, some common conventions have arisen with deploy groups at Yelp:

 - Most deploy group names are alphanumeric with underscores, dots, and hyphens.
   This makes them easy to pass to the ``paasta`` command line tool without worrying about quoting whitespace and special characters.
 - A common pattern (seen in the example ``deploy.yaml`` above) is to use ``<environment>.canary`` and ``<environment>.non_canary`` (where ``<environment>`` is ``prod``, ``stage``, ``dev``, a more specific name such as ``pnw-prod`` or ``devc``, or multiple environments, such as ``dev-stage``).
   In this pattern, all your canary instances in an environment should have ``deploy_group: <environment>.canary``.
   Everything else in that environment should have ``deploy_group: <environment>.non_canary``.
   In environments where you only intend to have one deploy group (often dev and stage), you can use ``deploy_group: <environment>.everything``.
 - For simple services, where you want to deploy changes to all instances in all clusters at the same time, use the same deploy group name on all instances, usually ``everything``.

PaaSTA does not parse these names -- to PaaSTA, they are just opaque strings.
This means nothing technically prevents you from putting a prod instance into the ``dev-stage.everything`` deploy group, or making an ``everything`` deploy group that doesn't actually contain everything.

One important caveat is that software besides PaaSTA may try to interpret the deploy group name.
For example, the srv-configs library used by many services at Yelp will pick up new configuration changes earlier for instances that belong to a deploy group containing the string ``canary``.

Canary instances
----------------

Many services at Yelp have canary instances, which are configured similarly to the non-canary instances, but have fewer copies running.
Typically, canaries are put into a separate deploy group, which is listed before the non-canary deploy group in ``deploy.yaml``.
This means the canary will run a new version of code before the non-canary instances do, which allows you to look for errors on canary before deploying to non-canary.
For HTTP or TCP services that use SmartStack, usually the canary and main instance have the same ``registrations``.
Since each running copy with a given registration will receive a roughly equal fraction of traffic, the proportion of traffic sent to canary instances is ``(number of running canary copies) / (total number of running copies)``.
Typically canary instances will be configured to have a smaller number of running copies (``instances``), so that this traffic proportion is small.

In the following example, the proportion of traffic going to ``canary`` is somewhere between 1.5% and 10%, depending on autoscaling on the ``main`` instance.

.. sourcecode:: yaml

   # my-cool-service/kubernetes-pnw-prod.yaml
   main:
     min_instances: 30
     max_instances: 200
     cpus: 2
     mem: 2000
     registrations: ['my-cool-service.main']
     deploy_group: prod.non-canary

   canary:
     instances: 3
     cpus: 2
     mem: 2000
     registrations: ['my-cool-service.main']  # Same as main
     deploy_group: prod.canary  # Different from main

With a ``deploy.yaml`` that looks like this, the ``canary`` instance will deploy before the ``main`` instance.
After deploying the ``canary`` instance, Jenkins will wait until you click a button before starting to deploy the ``main`` instance.

.. sourcecode:: yaml

   # my-cool-service/deploy.yaml
   ---
   pipeline:
   ...
   - step: prod.canary
     trigger_next_step_manually: true
   - step: prod.non_canary

For an HTTP or TCP service, you'll also need a ``main`` entry in ``smartstack.yaml``, corresponding to the ``registrations`` entries in the ``kubernetes-pnw-prod.yaml`` file above.

.. sourcecode:: yaml

   # my-cool-service/smartstack.yaml
   ---
   main:
     advertise: [region]
     discover: [region]
     proxy_port: 19284


String interpolation
--------------------

Deploy groups support string interpolation for the following variables: ``cluster``, ``instance`` and ``service``. String interpolation works by surrounding the variable's name with braces (``{}``) in the ``deploy_group`` field -- this is python's ``str.format`` syntax. E.g. ``deploy_group: '{cluster}.all'``. You must still specify explicit deploy groups in your ``deploy.yaml`` however.
