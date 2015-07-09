Feature: paasta_metastatus describes the state of the paasta cluster

  Scenario: Zookeeper unreachable
    Given a working paasta cluster
    When all zookeepers are unavailable
    Then metastatus returns 2

  Scenario: High memory usage
    Given a working paasta cluster
    When a task consuming most available memory is launched
    Then metastatus returns 2

#  Scenario: Mesos master unreachable
#    Given a working paasta cluster
#    When all masters are unavailable
#    Then metastatus returns 2

# vim: set ts=2 sw=2
