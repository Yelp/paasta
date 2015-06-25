Feature: check_mesos_resource_utilization can send event

  Scenario: check_mesos_resource_utilization OK on 0%
    Given a working marathon instance
    Then we should get OK when checking mesos utilization 101 percent
    Then we should get CRITICAL when checking mesos utilization 0 percent
