Feature: Bounces work as expected

  Scenario: The upthendown bounce works
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "upthendown" and drain method "noop"
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running

     When there are 2 new healthy tasks
      And setup_service is initiated
     When we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The upthendown bounce does not kill the old app if the new one is unhealthy
    Given a working paasta cluster
      And a new unhealthy app to be deployed, with bounce strategy "upthendown" and drain method "noop"
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running

     When there are 2 new unhealthy tasks
      And setup_service is initiated
     Then the old app should be configured to have 2 instances
      And the old app should be running

  Scenario: The brutal bounce works
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "brutal" and drain method "noop"
      And an old app to be destroyed
     Then the old app should be running

     When there are 1 old healthy tasks
      And setup_service is initiated
     Then the new app should be running
     When we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The crossover bounce works
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "noop"
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new healthy tasks
      And setup_service is initiated
     Then the old app should be running
      And the old app should be configured to have 1 instances

     When there are 2 new healthy tasks
      And setup_service is initiated
      And we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The crossover bounce does not kill the old app if the new one is unhealthy
    Given a working paasta cluster
      And a new unhealthy app to be deployed, with bounce strategy "crossover" and drain method "noop"
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new unhealthy tasks
      And setup_service is initiated
     Then the old app should be configured to have 2 instances
      And the old app should be running

     When there are 2 new unhealthy tasks
      And setup_service is initiated
     Then the old app should be configured to have 2 instances
      And the old app should be running

  Scenario: The downthenup bounce works
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "downthenup" and drain method "noop"
      And an old app to be destroyed
     Then the old app should be running

     When there are 2 old healthy tasks
      And setup_service is initiated
     When we wait a bit for the new app to disappear
     Then the new app should be gone
     When we wait a bit for the old app to disappear
     Then the old app should be gone

     When setup_service is initiated
     Then the new app should be running

  Scenario: Bounces wait for drain method
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "test"
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running
      # Note: this is different from the crossover bounce scenario because one of the old tasks is still draining.
      And the old app should be configured to have 2 instances

     When a task has drained
      And setup_service is initiated
     Then the old app should be configured to have 1 instances

  Scenario: Bounces make progress even if drain method fails to respond
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "crashy_drain"
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running


     When there are 1 new healthy tasks
      And setup_service is initiated
     Then the new app should be running
      And the old app should be running
      # Note: this is different from the "wait for drain method" scenario because the drain method raises an exception.
      And the old app should be configured to have 1 instances

     When there are 2 new healthy tasks
      And setup_service is initiated
     Then the new app should be running
     When we wait a bit for the old app to disappear
     Then the old app should be gone
