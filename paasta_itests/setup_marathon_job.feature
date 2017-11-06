Feature: setup_marathon_job can create a "complete" app

  Scenario: complete apps can be deployed
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps
     Then we can run get_app

  Scenario: duplicate create app does not fail with conflict
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps
     Then we can run get_app
     When we create a marathon app called "test-service.main" with 1 instance(s) with no apps found running

  Scenario: marathon apps can be scaled up
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
      And we run setup_marathon_job until it has 1 task(s)
      And we set the number of instances to 5
      And we run setup_marathon_job until it has 5 task(s)
     Then we should see the number of instances become 5

  @skip
  Scenario: marathon apps can be scaled down
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 5 instance(s)
      And we run setup_marathon_job until it has 5 task(s)
      And we set the number of instances to 1
      And we run setup_marathon_job until it has 1 task(s)
     Then we should see the number of instances become 1

  @skip
  Scenario: marathon apps can be scaled up with at-risk hosts
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "noop" and constraints [["hostname", "UNIQUE"]]
      And an old app to be destroyed with constraints [["hostname", "UNIQUE"]]
     When there are exactly 2 old healthy tasks
     When setup_service is initiated
      And there are exactly 2 new healthy tasks
     When we mark a host it is running on as at-risk
     When setup_service is initiated
      And there are exactly 3 new healthy tasks
      And we wait a bit for the old app to disappear
      And setup_service is initiated
      And there are exactly 2 new healthy tasks
     Then the old app should be gone
      And there should be 0 tasks on that at-risk host

  @skip
  Scenario: marathon apps can be deployed with at-risk hosts
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "noop" and constraints [["hostname", "UNIQUE"]]
     When setup_service is initiated
      And there are exactly 2 new healthy tasks
     When we mark a host it is running on as at-risk
     When setup_service is initiated
      And there are exactly 3 new healthy tasks
     When setup_service is initiated
      And there are exactly 2 new healthy tasks
      And setup_service is initiated
     Then there should be 0 tasks on that at-risk host

  Scenario: marathon shards can be changed
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main" on shard 0, previous shard 0
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps on shard 0
     When there are exactly 1 new healthy tasks
    Given I have yelpsoa-configs for the marathon job "test-service.main" on shard 1, previous shard 1
     When we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps on shard 1
      And we should see it in the list of apps on shard 0
     When there are exactly 1 new healthy tasks
      And we create a marathon app called "test-service.main" with 1 instance(s)
     Then we should see it in the list of apps on shard 1
      And we should not see it in the list of apps on shard 0
