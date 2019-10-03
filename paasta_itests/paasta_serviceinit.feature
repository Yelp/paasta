Feature: paasta_serviceinit

  Scenario: marathon_serviceinit can run status
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main" with "1" instances
      And we wait for "test-service.main" to launch exactly 1 tasks
     Then marathon_serviceinit status_marathon_job should return "Healthy" for "test-service.main"

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

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
