Feature: paasta_serviceinit

  Scenario: marathon_serviceinit can run status
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main"
      And we wait for it to be deployed
     Then marathon_serviceinit status_marathon_job should return "Healthy" for "test-service.main"

  Scenario: marathon_serviceinit can restart tasks
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main"
      And we wait for it to be deployed
     Then marathon_serviceinit restart should get new task_ids for "test-service.main"

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

  Scenario: paasta_serviceinit can run status -vv to tail a mesos task stdout/stderr
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main"
      And we wait for it to be deployed
     Then paasta_serviceinit status -vv for the service_instance "test-service.main" exits with return code 0 and the correct output
      And paasta_serviceinit status -s "test-service" -i "main" exits with return code 0 and the correct output
      And paasta_serviceinit status -s "test-service" -i "main,test" has the correct output for instance main and exits with non-zero return code for instance test

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

  Scenario: paasta_serviceinit can run emergency-stop on a marathon app
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main"
      And we wait for it to be deployed
      And we run paasta serviceinit "stop" on "test-service.main"
      And we wait for "test-service.main" to launch exactly 0 tasks
     Then "test-service.main" has exactly 0 requested tasks in marathon

  Scenario: paasta_serviceinit can run emergency-stop on a marathon app via appid
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main"
      And we wait for it to be deployed
      And we run paasta serviceinit --appid "stop" on "test-service.main"
      And we wait for "test-service.main" to launch exactly 0 tasks
     Then "test-service.main" has exactly 0 requested tasks in marathon

  Scenario: paasta_serviceinit can run emergency-scale on a marathon app
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main"
      And we wait for it to be deployed
      And we run paasta serviceinit scale --delta "1" on "test-service.main"
      And we wait for "test-service.main" to launch exactly 3 tasks
     Then "test-service.main" has exactly 3 requested tasks in marathon

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
