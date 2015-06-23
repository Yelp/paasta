Feature: paasta_metastatus describes the state of the paasta cluster

  Scenario: Mesos master unreachable
    Given a working paasta cluster
    When we kill a master
    Then metastatus returns 2

# vim: set ts=2 sw=2
