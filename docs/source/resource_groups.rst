Resource Groups
===============

Resource groups are wrappers around cloud provider APIs to enable scaling up and down groups of machines.  A resource
group implments the :py:class:`.ResourceGroup` interface, which provides the set of required methods for
Clusterman to interact with the resource group.  Currently, Clusterman supports the following types of resource groups:

* :py:class:`.AutoScalingResourceGroup`: `AWS autoscaling groups
  <https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html>`_
* :py:class:`.EC2FleetResourceGroup`: `AWS ec2fleet requests
  <https://aws.amazon.com/about-aws/whats-new/2018/04/introducing-amazon-ec2-fleet/>`_
* :py:class:`.SpotFleetResourceGroup`: `AWS spot fleet requests
  <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-fleet-requests.html>`_
