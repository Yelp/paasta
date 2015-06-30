Feature: Bounces work as expected

  Scenario: The upthendown bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed

     When there are 2 old tasks
      And deploy_service with bounce strategy "upthendown", drain method "noop" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new tasks
      And deploy_service with bounce strategy "upthendown", drain method "noop" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 2 new tasks
      And deploy_service with bounce strategy "upthendown", drain method "noop" is initiated
     When we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The brutal bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed
     Then the old app should be running

     When there are 1 old tasks
      And deploy_service with bounce strategy "brutal", drain method "noop" is initiated
     Then the new app should be running
     When we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The crossover bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed

     When there are 2 old tasks
      And deploy_service with bounce strategy "crossover", drain method "noop" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new tasks
      And deploy_service with bounce strategy "crossover", drain method "noop" is initiated
     Then the old app should be running
      And the old app should be configured to have 1 instances

     When there are 2 new tasks
      And deploy_service with bounce strategy "crossover", drain method "noop" is initiated
      And we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The downthenup bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed
     Then the old app should be running

     When there are 2 old tasks
      And deploy_service with bounce strategy "downthenup", drain method "noop" is initiated
     When we wait a bit for the new app to disappear
     Then the new app should be gone
     When we wait a bit for the old app to disappear
     Then the old app should be gone

     When deploy_service with bounce strategy "downthenup", drain method "noop" is initiated
     Then the new app should be running

  Scenario: Bounces wait for drain method
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed

     When there are 2 old tasks
      And deploy_service with bounce strategy "crossover", drain method "test" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new tasks
      And deploy_service with bounce strategy "crossover", drain method "test" is initiated
     Then the new app should be running
      And the old app should be running
      # Note: this is different from the crossover bounce scenario because one of the old tasks is still draining.
      And the old app should be configured to have 2 instances

     When a task has drained
      And deploy_service with bounce strategy "crossover", drain method "test" is initiated
     Then the old app should be configured to have 1 instances

