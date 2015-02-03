Feature: Bounces work as expected

  Scenario: The upthendown bounce works
    Given a working marathon instance
      And a new app to be deployed
      And an old app to be destroyed
     When an upthendown_bounce is intitiated
     Then the new app should be running
      And the old app should be gone
