Feature: cleanup_chronos_jobs removes chronos jobs no longer in the config

 Scenario: Cleanup chronos jobs removes jobs
    Given a working paasta cluster
      And I have config for the service "myservice"
     When I launch "3" chronos jobs
     Then cleanup_chronos_jobs exits with return code "0" and the correct output
      And the launched jobs are no longer listed in chronos
