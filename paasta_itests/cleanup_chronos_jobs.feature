Feature: cleanup_chronos_jobs removes chronos jobs no longer in the config

 Scenario: Cleanup chronos jobs removes jobs
    Given a working paasta cluster
     When I have no jobs listed in config
      And I launch "3" chronos jobs
     Then cleanup_chronos_jobs exits with return code "0" and the correct output
      And the jobs are no longer listed in chronos
