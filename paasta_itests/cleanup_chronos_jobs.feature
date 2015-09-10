Feature: cleanup_chronos_jobs removes chronos jobs no longer in the config

 Scenario: Cleanup chronos jobs removes jobs
    Given a working paasta cluster
      And I have yelpsoa-configs for the service "myservice" with chronos instance "UNUSED" and command "echo hello"
     When I launch 3 configured chronos jobs with service "myservice" with chronos instance "UNUSED" and differing tags
      And I launch 1 unconfigured chronos jobs with service "oldservice" with chronos instance "foo" and differing tags
      And I launch 3 non-paasta jobs
     Then cleanup_chronos_jobs exits with return code "0" and the correct output
      And the non chronos jobs are still in the job list
      And the configured chronos jobs are in the job list
      And the unconfigured chronos jobs are not in the job list
