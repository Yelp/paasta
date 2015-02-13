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