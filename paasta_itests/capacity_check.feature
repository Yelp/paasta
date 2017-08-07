Feature: capacity_check

  Scenario: High disk usage crit
   Given a working paasta cluster
    When an app with id "disktest" using high disk is launched
     And 3 tasks belonging to the app with id "disktest" are in the task list
    Then capacity_check "disk" should return "CRITICAL" with code "2"

  Scenario: High memory usage crit
    Given a working paasta cluster
     When an app with id "memtest" using high memory is launched
      And 3 tasks belonging to the app with id "memtest" are in the task list
     Then capacity_check "mem" should return "CRITICAL" with code "2"

  Scenario: High cpu usage crit
    Given a working paasta cluster
     When an app with id "cputest" using high cpu is launched
      And 3 tasks belonging to the app with id "cputest" are in the task list
     Then capacity_check "cpus" should return "CRITICAL" with code "2"

  Scenario: High disk usage warn
   Given a working paasta cluster
    When an app with id "disktest" using high disk is launched
     And 3 tasks belonging to the app with id "disktest" are in the task list
    Then capacity_check "disk" --crit "99" --warn "90" should return "WARNING" with code "1"

  Scenario: High memory usage warn
    Given a working paasta cluster
     When an app with id "memtest" using high memory is launched
      And 3 tasks belonging to the app with id "memtest" are in the task list
     Then capacity_check "mem" --crit "99" --warn "90" should return "WARNING" with code "1"

  Scenario: High cpu usage warn
    Given a working paasta cluster
     When an app with id "cputest" using high cpu is launched
      And 3 tasks belonging to the app with id "cputest" are in the task list
     Then capacity_check "cpus" --crit "99" --warn "90" should return "WARNING" with code "1"

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
