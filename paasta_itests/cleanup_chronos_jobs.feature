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
