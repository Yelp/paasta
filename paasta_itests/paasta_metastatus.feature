Feature: paasta_metastatus describes the state of the paasta cluster

  Scenario: Zookeeper unreachable
    Given a working paasta cluster
    When all zookeepers are unavailable
    Then metastatus returns 2

  Scenario: High memory usage
    Given a working paasta cluster
    When an app called "memtest" using high memory is launched
    And a task with id "memtest" is in the task list
    Then metastatus returns 2

  Scenario: High cpu usage
    Given a working paasta cluster
    When an app called "cputest" using high cpu is launched
    And a task with id "cputest" is in the task list
    Then metastatus returns 2

#  Scenario: Mesos master unreachable
#    Given a working paasta cluster
#    When all masters are unavailable
#    Then metastatus returns 2

# vim: set ts=2 sw=2
