Feature: Paasta remote-run
  Scenario: We can run a job
    Given a working paasta cluster, with docker registry docker.io
      And a new adhoc config to be deployed
     When we start a adhoc scheduler with reconcile_backoff 10 and name test
     Then it should eventually start 1 tasks
