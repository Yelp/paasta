Feature: setup_chronos_job can create and bounce jobs

  Scenario: complete jobs can be deployed
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should see a job for the service "testservice" and instance "testinstance" in the job list

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

  Scenario: jobs will be cleaned up by the "graceful" bounce
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with enabled chronos instance "test-instance"
      And I have a deployments.json for the service "test-service" with enabled chronos instance "test-instance"
     When we create a chronos job dict from the configs for instance "test-instance" of service "test-service"
      And we load the configs for instance "test-instance" of service "test-service" into a ChronosJobConfig
      And 10 old jobs are left over from previous bounces
      And we run setup_chronos_job
     Then there should be 1 enabled jobs
      And there should be 5 disabled jobs
