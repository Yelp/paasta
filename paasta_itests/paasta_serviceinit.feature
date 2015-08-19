Feature: marathon_serviceinit can control marathon tasks

  Scenario: marathon_serviceinit can run status
    Given a working paasta cluster
    When we run the job test-service.main
    And we wait for it to be deployed
    Then marathon_serviceinit status_marathon_job should return "Healthy"

  Scenario: marathon_serviceinit can restart tasks
    Given a working paasta cluster
    When we run the job test-service.main
    And we wait for it to be deployed
    Then marathon_serviceinit restart should get new task_ids
