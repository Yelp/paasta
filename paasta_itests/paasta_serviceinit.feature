Feature: paasta_serviceinit

  Scenario: marathon_serviceinit can run status
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main" with "1" instances
      And we wait for "test-service.main" to launch exactly 1 tasks
     Then marathon_serviceinit status_marathon_job should return "Healthy" for "test-service.main"

  Scenario: paasta_serviceinit can run status on chronos jobs
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with disabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with disabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
     Then paasta_serviceinit status for the service_instance "testservice.testinstance" exits with return code 0 and the correct output

  Scenario: paasta_serviceinit can run status --verbose on chronos jobs
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with disabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with disabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
     Then paasta_serviceinit status --verbose for the service_instance "testservice.testinstance" exits with return code 0 and the correct output

  @skip
  Scenario: paasta_serviceinit can run status -vv to tail a mesos task stdout/stderr
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main" with "1" instances
      And we wait for "test-service.main" to launch exactly 1 tasks
     Then paasta_serviceinit status -vv for the service_instance "test-service.main" exits with return code 0 and the correct output
      And paasta_serviceinit status -s "test-service" -i "main" exits with return code 0 and the correct output
      And paasta_serviceinit status -s "test-service" -i "main,test" has the correct output for instance main and exits with non-zero return code for instance test

  @skip
  Scenario: paasta_serviceinit can run status on native jobs
    Given a working paasta cluster
      And we have yelpsoa-configs for native service "testservice.testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance" image "busybox"
      And we load_paasta_native_job_config
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
      And we wait for our native scheduler to launch exactly 1 tasks
     Then paasta_serviceinit status for the native service "testservice.testinstance" exits with return code 0
      And the output matches regex "^    testservice.testinstance.git"

  @skip
  Scenario: paasta_serviceinit can run emergency-stop on an enabled chronos job
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
          # Force job to be scheduled so that we can check that emergency-stop is in fact working
      And we paasta_serviceinit emergency-start the service_instance "testservice.testinstance"
      And we paasta_serviceinit emergency-stop the service_instance "testservice.testinstance"
     Then the job stored as "myjob" is disabled in chronos
      And the job stored as "myjob" has no running tasks

  Scenario: paasta_serviceinit emergency-stop kills rerun jobs
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
     When we run chronos_rerun for service_instance "testservice testinstance"
     Then we should get exit code 0
      And there is a temporary job for the service testservice and instance testinstance
     When we store the name of the rerun job for the service testservice and instance testinstance as rerunjob
      And we paasta_serviceinit emergency-stop the service_instance "testservice.testinstance"
     Then the job stored as "rerunjob" is disabled in chronos
      And the job stored as "rerunjob" has no running tasks

  Scenario: paasta_serviceinit can run emergency-start on an enabled chronos job
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
      And we paasta_serviceinit emergency-start the service_instance "testservice.testinstance"
     Then the job stored as "myjob" is enabled in chronos
      And the job stored as "myjob" has running tasks

  Scenario: paasta_serviceinit can run emergency-start on a disabled chronos job that results in noop
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with disabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
      And we paasta_serviceinit emergency-start the service_instance "testservice.testinstance"
     Then the job stored as "myjob" is disabled in chronos
      And the job stored as "myjob" has no running tasks

  Scenario: paasta_serviceinit can run emergency-start on a stopped chronos job that results in noop
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with disabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
      And we paasta_serviceinit emergency-start the service_instance "testservice.testinstance"
     Then the job stored as "myjob" is disabled in chronos
      And the job stored as "myjob" has no running tasks

  Scenario: paasta_serviceinit can run emergency-restart on an enabled chronos job
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
      And we wait for the chronos job stored as "myjob" to appear in the job list
      And we paasta_serviceinit emergency-restart the service_instance "testservice.testinstance"
     Then the job stored as "myjob" is enabled in chronos
      And the job stored as "myjob" has running tasks

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
