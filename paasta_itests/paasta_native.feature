Feature: Paasta native mesos framework
  Scenario: we can start a service
    Given a working paasta cluster
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler
     Then it should eventually start 3 tasks
