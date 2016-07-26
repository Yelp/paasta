@mesos
Feature: check_mesos_resource_utilization can send event

  Scenario: check_mesos_resource_utilization 101%
    Given a working paasta cluster
     When we check mesos utilization with a threshold of 101 percent
     Then the result is OK

  Scenario: check_mesos_resource_utilization 0%
    Given a working paasta cluster
     When we check mesos utilization with a threshold of 0 percent
     Then the result is CRITICAL
