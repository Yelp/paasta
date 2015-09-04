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
    And I have yelpsoa-configs for the service "test-service" with disabled chronos instance "job"
    And I have a deployments.json for the service "test-service" with chronos instance "job"
    When we create a chronos job from the configs for instance "job" of service "test-service"
    And the chronos job appears in the job list
    Then paasta_serviceinit status exits with return code 0 and the correct output

#  Scenario: paasta_serviceinit can (emergency) start a chronos job
#    Given a working paasta cluster
#    And I have yelpsoa-configs for the service "test-service" with chronos instance "job"
#    And I have a deployments.json for the service "test-service" with chronos instance "job"
#    When paasta_serviceinit (emergency) starts a chronos job
#    And the trivial chronos job appears in the job list
#    Then paasta_serviceinit status exits with return code 0 and the correct output

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
