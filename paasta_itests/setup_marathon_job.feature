Feature: setup_marathon_job can create a "complete" app

  Scenario: complete apps can be deployed
    Given a working paasta cluster
     When we create a complete app with 1 instance(s)
     Then we should see it in the list of apps
     Then we can run get_app on it
     Then we should see the number of instances become 1

  Scenario: marathon apps can be scaled up
    Given a working paasta cluster
     When we create a complete app with 1 instance(s)
      And we change the number of instances to 5
      And we run setup_marathon_job until the instance count is 5
     Then we should see the number of instances become 5

  Scenario: marathon apps can be scaled down
    Given a working paasta cluster
     When we create a complete app with 5 instance(s)
      And we change the number of instances to 1
      And we run setup_marathon_job until the instance count is 1
     Then we should see the number of instances become 1
