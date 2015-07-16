Feature: paasta_metastatus describes the state of the paasta cluster

  #Scenario: Zookeeper unreachable
    #Given a working paasta cluster
    #When all zookeepers are unavailable
    #Then metastatus returns 2

  Scenario: High memory usage
    Given a working paasta cluster
    When an app with id "memtest" using high memory is launched
    And a task belonging to the app with id "memtest" is in the task list
    Then paasta_metastatus exits with return code "2" and output "CRITICAL: Less than 10% memory available."

  Scenario: High cpu usage
    Given a working paasta cluster
    When an app with id "cputest" using high cpu is launched
    And a task belonging to the app with id "cputest" is in the task list
    Then paasta_metastatus exits with return code "2" and output "CRITICAL: Less than 10% CPUs available."

#  Scenario: Mesos master unreachable
#    Given a working paasta cluster
#    When all masters are unavailable
#    Then metastatus returns 2

# vim: set ts=2 sw=2
