Feature: setup_chronos_job can create a "complete" job

  Scenario: complete jobs can be deployed
    Given a working paasta cluster
    When we create a complete chronos job
    Then we should see it in the list of jobs
