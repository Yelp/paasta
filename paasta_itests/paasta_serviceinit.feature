Feature: paasta_serviceinit

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

  Scenario: paasta_serviceinit can run status on chronos jobs
    Given a working paasta cluster
    And I have yelpsoa-configs for the service "test-service" with chronos instance "job" and command "echo hello"
    When we create a trivial chronos job
    And the trivial chronos job appears in the job list
    Then paasta_serviceinit status exits with return code 0 and the correct output

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
