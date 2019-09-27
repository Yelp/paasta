Feature: paasta_api

  Scenario: paasta status for marathon instance via the API
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main" with "1" instances
      And we wait for "test-service.main" to launch exactly 1 tasks
     Then paasta status via the API for "test-service.main" should run successfully

  Scenario: instance GET shows the marathon status of service.instance
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main" with "1" instances
     And we wait for "test-service.main" to launch exactly 1 tasks
    Then instance GET should return app_count "1" and an expected number of running instances for "test-service.main"
     And instance GET should return error code "404" for "test-service.non-existent"

  Scenario: High disk usage
    Given a working paasta cluster
    When an app with id "disktest" using high disk is launched
     And 3 tasks belonging to the app with id "disktest" are in the task list
    Then resources GET should show "disk" has 285 used

  Scenario: High memory usage
    Given a working paasta cluster
     When an app with id "memtest" using high memory is launched
      And 3 tasks belonging to the app with id "memtest" are in the task list
     Then resources GET should show "mem" has 1470 used

  Scenario: High cpu usage
    Given a working paasta cluster
     When an app with id "cputest" using high cpu is launched
      And 3 tasks belonging to the app with id "cputest" are in the task list
     Then resources GET should show "cpus" has 27.3 used

  # Note that the following tests depend on the configuration of docker-compose.yml
  #  in paasta_itests.  This is unfortunate, but seems to be the easiest way to launch
  #  multiple slaves with different attributes
  Scenario: Grouping
    Given a working paasta cluster
     Then resources GET with groupings "pool" should return 2 groups
      And resources GET with groupings "region" should return 2 groups
      And resources GET with groupings "," should return 1 groups
      And resources GET with groupings "noexist" should return 1 groups
      And resources GET with groupings "ssd" should return 2 groups
      And resources GET with groupings "pool,region" should return 4 groups
      And resources GET with groupings "pool,region,ssd" should return 5 groups

  Scenario: Filters
    Given a working paasta cluster
     Then resources GET with groupings "pool" and filters "pool:default" should return 1 groups
      And resources GET with groupings "pool" and filters "region:fakeregion" should return 2 groups
      And resources GET with groupings "pool" and filters "ssd:true|region:fakeregion" should return 1 groups

  Scenario: Marathon Dashboard
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And I have yelpsoa-configs for the marathon job "test-service2.main" on shard 0, previous shard 1
      Then marathon_dashboard GET should return "test-service.main" in cluster "testcluster" with shard 2
      Then marathon_dashboard GET should return "service.instance2" in cluster "testcluster" with shard 0

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
