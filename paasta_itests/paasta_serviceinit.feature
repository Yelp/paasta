Feature: marathon_serviceinit

  Scenario: marathon_serviceinit can run status
    Given a working paasta cluster
    When we run the marathon job test-service.main
    And we wait for it to be deployed
    Then marathon_serviceinit status_marathon_job should return "Healthy"

  Scenario: marathon_serviceinit can restart tasks
    Given a working paasta cluster
    When we run the marathon job test-service.main
    And we wait for it to be deployed
    Then marathon_serviceinit restart should get new task_ids


Feature: paasta_chronos_serviceinit

  Scenario: paasta_chronos_serviceinit can run status
    Given a working paasta cluster
    When we create a trivial chronos job
    And the trivial chronos job appears in the job list
    Then paasta_chronos_serviceinit status should return "Healthy"

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
