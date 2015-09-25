Feature: paasta_serviceinit

  Scenario: marathon_serviceinit can run status
    Given a working paasta cluster
     When we run the trivial marathon job "test-service.main"
      And we wait for it to be deployed
     Then marathon_serviceinit status_marathon_job should return "Healthy" for "test-service.main"

  Scenario: marathon_serviceinit can restart tasks
    Given a working paasta cluster
     When we run the trivial marathon job "test-service.main"
      And we wait for it to be deployed
     Then marathon_serviceinit restart should get new task_ids

  Scenario: paasta_serviceinit can run status on chronos jobs
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with disabled chronos instance "job"
      And I have a deployments.json for the service "test-service" with disabled chronos instance "job"
     When we create a chronos job dict from the configs for instance "job" of service "test-service"
      And we send the job to chronos
      And we wait for the chronos job to appear in the job list
     Then paasta_serviceinit status exits with return code 0 and the correct output

  Scenario: paasta_serviceinit can run emergency-stop on an enabled chronos job
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with enabled chronos instance "job"
      And I have a deployments.json for the service "test-service" with enabled chronos instance "job"
     When we create a chronos job dict from the configs for instance "job" of service "test-service"
      And we send the job to chronos
      And we wait for the chronos job to appear in the job list
      And we paasta_serviceinit emergency-stop the chronos job
     Then the job is disabled in chronos
      And the job has no running tasks

  Scenario: paasta_serviceinit can run emergency-start on an enabled chronos job
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with enabled chronos instance "job"
      And I have a deployments.json for the service "test-service" with enabled chronos instance "job"
     When we create a chronos job dict from the configs for instance "job" of service "test-service"
      And we paasta_serviceinit emergency-start the chronos job
      And we wait for the chronos job to appear in the job list
     Then the job is enabled in chronos
      And the job has running tasks

  Scenario: paasta_serviceinit can run emergency-start on a disabled chronos job
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with disabled chronos instance "job"
      And I have a deployments.json for the service "test-service" with disabled chronos instance "job"
     When we create a chronos job dict from the configs for instance "job" of service "test-service"
      And we paasta_serviceinit emergency-start the chronos job
      And we wait for the chronos job to appear in the job list
     Then the job is disabled in chronos
      And the job has no running tasks

  Scenario: paasta_serviceinit can run emergency-restart on an enabled chronos job
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with enabled chronos instance "job"
      And I have a deployments.json for the service "test-service" with enabled chronos instance "job"
     When we create a chronos job dict from the configs for instance "job" of service "test-service"
      And we send the job to chronos
      And we wait for the chronos job to appear in the job list
      And we update the tag for the service "test-service" with enabled chronos instance "job"
      And we create a chronos job dict from the configs for instance "job" of service "test-service"
      And we paasta_serviceinit emergency-restart the chronos job
     Then the old job is disabled in chronos
      And the old job has no running tasks
      And the new job is enabled in chronos
      And the new job has running tasks

  Scenario: paasta_serviceinit can run emergency-stop on a marathon app
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "test-service" with marathon instance "main"
      And I have a deployments.json for the service "test-service" with marathon instance "main"
     When we run the marathon job "test-service.main"
       And we wait for it to be deployed
      And we run paasta serviceinit "stop" on "test-service.main"
     Then "test-service.main" has exactly 0 requested tasks in marathon

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
