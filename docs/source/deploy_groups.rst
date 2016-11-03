=============
Deploy Groups
=============

What are deploy groups?:
========================

Deploy groups are a way to allow chronos jobs and marathon services to share docker containers, git branches and deployment steps. Deploy groups also shorten your jenkins pipeline and make it run faster (or save you time if you manually mark your changes for deployment)!

Deploy groups work by linking together multiple paasta instances under a single deployment step in ``deploy.yaml``.

Deploy group tutorial
---------------------

Deploy groups are specified in your soa configs. Let’s take a look in ``example_service`` in ``yelpsoa_configs``. With deploy groups, a ``deploy.yaml`` file can look like this:

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

You can see we have three deploy groups here: ``dev-stage.everything``, ``prod.canary`` and ``prod.non_canary``. There each refer to a number of instances that all share the same docker container, and will all be deployed at the same time.

Now let’s take a look at how instances are linked to a deploy group by taking a look in a sample config, ``marathon-norcal-devc.yaml``:

.. sourcecode:: yaml

   ---
   canary:
     cpus: .1
     mem: 301
     registrations: ['service.main']
     instances: 1
     deploy_group: dev-stage.everything
   main:
     cpus: 1
     mem: 300
     instances: 3
     deploy_group: dev-stage.everything

This is repeated for each ``marathon-*.yaml`` and ``chronos-*.yaml`` file. In example_service, we create three different files - ``marathon-PROD.yaml``, ``marathon-STAGE.yaml`` and ``marathon-DEV.yaml`` and use symlinks to create the files we need. This is similar to the current ``marathon-SHARED.yaml`` file. Feel free to look at example_service in ``yelpsoa_configs`` for an example on how to use deploy groups.

If no value for ``deploy_group`` is given, it defaults to ``CLUSTER_NAME.INSTANCE_NAME``. This is to support legacy services that don't use deploy groups.

What deploy groups aren't
-------------------------

Deploy groups are not a way to control your instances -- tools such as ``paasta start/stop/restart`` do not support deploy groups: you can’t stop all an entire deploy group by running ``paasta stop DEPLOY_GROUP_NAME``. Deploy groups also do not share other configuration elements such as memory and cpu allocation.

How to use deploy groups
========================

New service
-----------

When you run ``paasta fsm`` to generate the boilerplate configs for your paasta service, the default configs will have four deploy groups: ``dev.everything``, ``stage.everything``, ``prod.canary`` and ``prod.non_canary``. You can change these deploy groups around to suit your needs, and also change their deploy order by editing ``deploy.yaml``. Then, edit your soa configs and generate your pipline like normal.

Existing service
----------------

#. Plan out how you want to divide your service into deploy groups

#. Append deployment steps for these deploy groups to the end of your service's ``deploy.yaml`` file

#. Push your changes to the soa configs repo

#. Regenerate your jenkins workflow by running the ``paasta generate-pipeline`` command

#. Make sure your jenkins pipeline marks every deploy group for deployment (i.e. your pipeline runs all the way through)

#. Remove the deployment steps that were replaced by deploy groups from ``deploy.yaml``

#. Edit your service's ``marathon-*.yaml`` and ``chronos-*.yaml`` files to specify which instances belong to which deploy group

#. Push your changes to the soa configs repo

#. Regenerate your jenkins workflow one last time by running ``paasta generate-pipeline``

Alternatively to a jenkins pipeline, you can use ``paasta mark-for-deployment`` with the ``--deploy-group`` flag to manually mark each of your deploy groups.

String interpolation
--------------------

Deploy groups support string interpolation for the following variables: ``cluster``, ``instance`` and ``service``. String interpolation works by surrounding the varbiable's name with braces (``{}``) in the ``deploy_group`` field -- this is python's ``str.format`` syntax. E.g. ``deploy_group: '{cluster}.all'``. You must still specify explicit deploy groups in your ``deploy.yaml`` however.

What if I don’t want to use deploy groups on my existing service?
-----------------------------------------------------------------

No changes are required -- your service should work as-is. Since the default deploy group for an instance is ``CLUSTER_NAME.INSTANCE_NAME``, all of your current configs will work with the new deploy group-aware tools.

How to remove a specific instance from a deploy group
-----------------------------------------------------

Edit that instance's ``marathon-CLUSTER_NAME.yaml`` or ``chronos-CLUSTER_NAME.yaml`` file and remove the ``deploy_group`` line from the instance you want to deploy separately. Then, add another deployment step to ``deploy.yaml`` to deploy the instance using the ``CLUSTER_NAME.INSTANCE_NAME`` idiom. Finally, follow the steps to recreate your jenkins pipeline.

Alternatively, you can assign the instance to a deploy group that only contains that one instance -- this is what the above steps are doing implicitly, as the default deploy group is ``CLUSTER_NAME.INSTANCE_NAME``.
