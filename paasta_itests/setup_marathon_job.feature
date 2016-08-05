Feature: setup_marathon_job can create a "complete" app

  Scenario: complete apps can be deployed
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps
     Then we can run get_app

  Scenario: marathon apps can be scaled up
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
      And we run setup_marathon_job until it has 1 task(s)
      And we set the number of instances to 5
      And we run setup_marathon_job until it has 5 task(s)
     Then we should see the number of instances become 5

  Scenario: marathon apps can be scaled down
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 5 instance(s)
      And we run setup_marathon_job until it has 5 task(s)
      And we set the number of instances to 1
      And we run setup_marathon_job until it has 1 task(s)
     Then we should see the number of instances become 1

  Scenario: zookeeper records can be used to scale services up
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we set up an app to use zookeeper scaling with 5 max instances
      And we create a marathon app called "test-service.main" with 3 instance(s)
      And we set the instance count in zookeeper for service "test-service" instance "main" to 5
      And we run setup_marathon_job until it has 5 task(s)
     Then we should see the number of instances become 5

  Scenario: zookeeper records can be used to scale services down
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we set up an app to use zookeeper scaling with 5 max instances
      And we create a marathon app called "test-service.main" with 5 instance(s)
      And we set the instance count in zookeeper for service "test-service" instance "main" to 3
      And we run setup_marathon_job until it has 3 task(s)
      And we set the instance count in zookeeper for service "test-service" instance "main" to 1
      And we run setup_marathon_job until it has 1 task(s)
     Then we should see the number of instances become 1
