Feature: cleanup_chronos_jobs removes chronos jobs no longer in the config

 Scenario: Cleanup chronos jobs removes jobs
   Given a working paasta cluster
     And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
    When I launch 1 configured jobs for the service "testservice" with scheduled chronos instance "testinstance"
     And I launch 1 unconfigured jobs for the service "oldservice" with scheduled chronos instance "othertestinstance"
     And I launch 3 non-paasta jobs
    Then cleanup_chronos_jobs exits with return code "0" and the correct output
     And the non-paasta jobs are not in the job list
     And the configured chronos jobs are in the job list
     And the unconfigured chronos jobs are not in the job list

 Scenario: Cleanup chronos jobs ignores temporary jobs
   Given a working paasta cluster
     And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
     And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
    When we run chronos_rerun for service_instance "testservice testinstance"
     And we store the name of the job for the service testservice and instance testinstance as myjob
     And we run cleanup_chronos_jobs
    Then we should get exit code 0
     And we should see a job for the service "testservice" and instance "testinstance" in the job list
     And there is a temporary job for the service testservice and instance testinstance
