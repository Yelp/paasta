Feature: setup_chronos_job can create and bounce jobs

  Scenario: complete jobs can be deployed
    Given a working paasta cluster
     When we create a complete chronos job
     Then we should see it in the list of jobs

  Scenario: jobs can be bounced using the "graceful" method
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with enabled chronos instance "test-instance"
      And I have a deployments.json for the service "test-service" with enabled chronos instance "test-instance"
     When we load the configs for instance "test-instance" of service "test-service" into a ChronosJobConfig
      And we set the bounce_method of the ChronosJobConfig to "graceful"
      And we create a chronos job dict from the configs for instance "test-instance" of service "test-service"
      And we run setup_chronos_job
      And we manually start the job
     Then the job is enabled in chronos
      And the job has running tasks

     When we update the tag for the service "test-service" with enabled chronos instance "test-instance"
      And we create a chronos job dict from the configs for instance "test-instance" of service "test-service"
      And we run setup_chronos_job
     Then the old job is disabled in chronos
      And the old job has running tasks
      And the new job is enabled in chronos

     When we update the tag for the service "test-service" with enabled chronos instance "test-instance"
      And we create a chronos job dict from the configs for instance "test-instance" of service "test-service"
      And we run setup_chronos_job
     Then the old job is disabled in chronos
      And the old job has no running tasks
      And the new job is enabled in chronos
      And the new job has running tasks
