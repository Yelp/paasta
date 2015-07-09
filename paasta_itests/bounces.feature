Feature: Bounces work as expected

  Scenario: The upthendown bounce works
    Given a working marathon instance
      And a new healthy app to be deployed
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new healthy tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 2 new healthy tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     When we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The upthendown bounce does not kill the old app if the new one is unhealthy
    Given a working marathon instance
      And a new unhealthy app to be deployed
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 2 new unhealthy tasks
      And deploy_service with bounce strategy "upthendown" is initiated
     Then the old app should be configured to have 2 instances
      And the old app should be running

  Scenario: The brutal bounce works
    Given a working marathon instance
      And a new healthy app to be deployed
      And an old app to be destroyed
     Then the old app should be running

     When there are 1 old healthy tasks
      And deploy_service with bounce strategy "brutal" is initiated
     Then the new app should be running
     When we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The crossover bounce works
    Given a working marathon instance
      And a new healthy app to be deployed
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new healthy tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be running
      And the old app should be configured to have 1 instances

     When there are 2 new healthy tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be running
      And the old app should be configured to have 0 instances

     When the old app is down to 0 instances
      And deploy_service with bounce strategy "crossover" is initiated
     When we wait a bit for the old app to disappear
     Then the old app should be gone

  Scenario: The crossover bounce does not kill the old app if the new one is unhealthy
    Given a working marathon instance
      And a new unhealthy app to be deployed
      And an old app to be destroyed

     When there are 2 old healthy tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the new app should be running
      And the old app should be running

     When there are 1 new unhealthy tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be configured to have 2 instances
      And the old app should be running

     When there are 2 new unhealthy tasks
      And deploy_service with bounce strategy "crossover" is initiated
     Then the old app should be configured to have 2 instances
      And the old app should be running

  Scenario: The downthenup bounce works
    Given a working marathon instance
      And a new healthy app to be deployed
      And an old app to be destroyed
     Then the old app should be running

     When there are 2 old healthy tasks
      And deploy_service with bounce strategy "downthenup" is initiated
     When we wait a bit for the new app to disappear
     Then the new app should be gone
     When we wait a bit for the old app to disappear
     Then the old app should be gone

     When deploy_service with bounce strategy "downthenup" is initiated
     Then the new app should be running
