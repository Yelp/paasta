Feature: Bounces work as expected

  Scenario: The upthendown bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed
     When we wait for marathon to finish launching the old app

     When deploy_service with bounce strategy "upthendown" is initiated
     Then the new app should be running
      And the old app should be running

     When we wait for marathon to finish launching the new app
      And we consider at most 2 tasks happy
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the new app should be running
      And the old app should be running

     When we consider at most 5 tasks happy
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the old app should be gone
