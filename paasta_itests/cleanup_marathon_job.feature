Feature: cleanup_marathon_job removes no longer needed jobs

  Scenario: deleted apps are destroyed
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And I have yelpsoa-configs for the marathon job "test-service1.main"
      And I have yelpsoa-configs for the marathon job "test-service2.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
      And we have a deployments.json for the service "test-service1" with enabled instance "main"
      And we have a deployments.json for the service "test-service2" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we create a marathon app called "test-service1.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we create a marathon app called "test-service2.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we delete a marathon app called "test-service.main" from "testcluster" soa configs
     Then we run cleanup_marathon_apps -v which exits with return code "0"
     Then we should not see it in the list of apps

  Scenario: paasta wont cleanup everything in one go
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And I have yelpsoa-configs for the marathon job "test-service1.main"
      And I have yelpsoa-configs for the marathon job "test-service2.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
      And we have a deployments.json for the service "test-service1" with enabled instance "main"
      And we have a deployments.json for the service "test-service2" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we create a marathon app called "test-service1.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we create a marathon app called "test-service2.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we delete a marathon app called "test-service.main" from "testcluster" soa configs
     When we delete a marathon app called "test-service1.main" from "testcluster" soa configs
     When we delete a marathon app called "test-service2.main" from "testcluster" soa configs
     Then we run cleanup_marathon_apps -v which exits with return code "1"
     Then we should see it in the list of apps

  Scenario: paasta will cleanup everything in one go if we force it
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And I have yelpsoa-configs for the marathon job "test-service1.main"
      And I have yelpsoa-configs for the marathon job "test-service2.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
      And we have a deployments.json for the service "test-service1" with enabled instance "main"
      And we have a deployments.json for the service "test-service2" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we create a marathon app called "test-service1.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we create a marathon app called "test-service2.main" with 1 instance(s)
     Then we should see it in the list of apps
     When we delete a marathon app called "test-service.main" from "testcluster" soa configs
     When we delete a marathon app called "test-service1.main" from "testcluster" soa configs
     When we delete a marathon app called "test-service2.main" from "testcluster" soa configs
     Then we run cleanup_marathon_apps -v --force which exits with return code "0"
     Then we should not see it in the list of apps
