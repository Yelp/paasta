Feature: Bounces work as expected

  Scenario: The upthendown bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed

     When there are 5 old tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 2 new tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 5 new tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the old app should be gone

  Scenario: The brutal bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed
     Then the old app should be running

     When there are 5 old tasks
      And deploy_service with bounce strategy "brutal" is initiated
     Then the new app should be running
      And the old app should be gone

  Scenario: The crossover bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed

     When there are 5 old tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be running
      And the old app should be configured to have 4 instances

     When there are 3 new tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be running
      And the old app should be configured to have 2 instances

     When there are 5 new tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be running
      And the old app should be configured to have 0 instances

     When the old app is down to 0 instances
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be gone

  Scenario: The downthenup bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed
     Then the old app should be running

     When there are 5 old tasks
      And deploy_service with bounce strategy "downthenup" is initiated
     Then the new app should be gone
      And the old app should be gone

     When deploy_service with bounce strategy "downthenup" is initiated
     Then the new app should be running
