Feature: paasta-deployd deploys apps

  Scenario: deployd will deploy a new app
    Given a working paasta cluster
      And paasta-deployd is running
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     Then we should see "test-service.main" listed in marathon after 60 seconds
     Then we can run get_app

  Scenario: deployd will re-deploy an app if its marathon-*.yaml changes
    Given a working paasta cluster
      And paasta-deployd is running
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     Then we should see "test-service.main" listed in marathon after 60 seconds
     Then we can run get_app
     Then we set a new command for our service instance to "/bin/bash -c 'echo deploy-all-the-things'"
    Given I have yelpsoa-configs for the marathon job "test-service.main"
     Then the appid for "test-service.main" should have changed
     Then we should not see the old version listed in marathon after 70 seconds
     Then we should see "test-service.main" listed in marathon after 60 seconds
     Then we can run get_app

  Scenario: deployd will re-deploy an app if its deployment.json is updated
    Given a working paasta cluster
      And paasta-deployd is running
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     Then we should see "test-service.main" listed in marathon after 60 seconds
     Then we can run get_app
    Given we have a deployments.json for the service "test-service" with enabled instance "main" image "test-image-foobar1"
     Then the appid for "test-service.main" should have changed
     Then we should not see the old version listed in marathon after 70 seconds
     Then we should see "test-service.main" listed in marathon after 60 seconds
     Then we can run get_app

  Scenario: deployd will re-deploy an app if the public config changes
    Given a working paasta cluster
      And paasta-deployd is running
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     Then we should see "test-service.main" listed in marathon after 60 seconds
     Then we can run get_app
    Given we add a new docker volume to the public config
     Then the appid for "test-service.main" should have changed
     Then we should not see the old version listed in marathon after 70 seconds
     Then we should see "test-service.main" listed in marathon after 60 seconds
     Then we can run get_app

  @skip
  Scenario: deployd will scale up an app if the instance count changes in zk
    Given a working paasta cluster
      And paasta-deployd is running
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we set the "min_instances" field of the marathon config for service "test-service" and instance "main" to the integer 2
      And we set the "max_instances" field of the marathon config for service "test-service" and instance "main" to the integer 4
      And we set the "disk" field of the marathon config for service "test-service" and instance "main" to the integer 1
      And we set the instance count in zookeeper for service "test-service" instance "main" to 3
     Then we should see "test-service.main" listed in marathon after 45 seconds
     Then we should see the number of instances become 3
     When we set the instance count in zookeeper for service "test-service" instance "main" to 4
     Then we should see the number of instances become 4

  @skip
  Scenario: deployd will scale down an app if the instance count changes in zk
    Given a working paasta cluster
      And paasta-deployd is running
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main" image "busybox"
     When we set the "min_instances" field of the marathon config for service "test-service" and instance "main" to the integer 2
      And we set the "cmd" field of the marathon config for service "test-service" and instance "main" to "sleep 300"
      And we set the "disk" field of the marathon config for service "test-service" and instance "main" to the integer 1
      And we set the "max_instances" field of the marathon config for service "test-service" and instance "main" to the integer 4
      And we set the instance count in zookeeper for service "test-service" instance "main" to 3
     Then we should see "test-service.main" listed in marathon after 45 seconds
     Then we should see the number of instances become 3
     When we set the instance count in zookeeper for service "test-service" instance "main" to 2
     Then we should see the number of instances become 2

  Scenario: deployd starts and only one leader
    Given a working paasta cluster
      And paasta-deployd is running
     Then a second deployd does not become leader
     Then paasta-deployd can be stopped
     Then a second deployd becomes leader

