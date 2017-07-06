Feature: paasta_api

  Scenario: instance GET shows the status of service.instance
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main" with "1" instances
     And we wait for "test-service.main" to launch exactly 1 tasks
     Then instance GET should return app_count "1" and an expected number of running instances for "test-service.main"
      And instance GET should return error code "404" for "test-service.non-existent"

  Scenario: High disk usage
    Given a working paasta cluster
    When an app with id "disktest" using high disk is launched
     And 3 tasks belonging to the app with id "disktest" are in the task list
    Then paasta_metastatus -v exits with return code "2" and output "CRITICAL: Less than 10% disk available."
     And resources GET should show "disk" has 285 used

  Scenario: High memory usage
    Given a working paasta cluster
     When an app with id "memtest" using high memory is launched
      And 3 tasks belonging to the app with id "memtest" are in the task list
     Then paasta_metastatus -v exits with return code "2" and output "CRITICAL: Less than 10% memory available."
      And resources GET should show "mem" has 1470 used

  Scenario: High cpu usage
    Given a working paasta cluster
     When an app with id "cputest" using high cpu is launched
      And 3 tasks belonging to the app with id "cputest" are in the task list
     Then paasta_metastatus -v exits with return code "2" and output "CRITICAL: Less than 10% CPUs available."
      And resources GET should show "cpus" has 27 used

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
