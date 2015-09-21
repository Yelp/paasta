Feature: setup_marathon_job can create a "complete" app

  Scenario: complete apps can be deployed
    Given a working paasta cluster
     When we create a complete app
     Then we should see it in the list of apps
     Then we can run get_app on it
