Feature: setting host_port in marathon.yaml creates a marathon app that behaves as we expect.
  @skip
  Scenario: static port works with host networking
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "noop" and host_port 31337 and host networking and 1 instances
     When setup_service is initiated
      And there are exactly 1 new healthy tasks
     Then it should be discoverable on port 31337

  @skip
  Scenario: dynamic port works with host networking
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "noop" and host_port 0 and host networking and 1 instances
     When setup_service is initiated
      And there are exactly 1 new healthy tasks
     Then it should be discoverable on any port

  @skip
  Scenario: static port works with bridge networking
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "noop" and host_port 31337 and bridge networking and 1 instances
     When setup_service is initiated
      And there are exactly 1 new healthy tasks
     Then it should be discoverable on port 31337

  @skip
  Scenario: dynamic port works with bridge networking
    Given a working paasta cluster
      And a new healthy app to be deployed, with bounce strategy "crossover" and drain method "noop" and host_port 0 and bridge networking and 1 instances
     When setup_service is initiated
      And there are exactly 1 new healthy tasks
     Then it should be discoverable on any port
