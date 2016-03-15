Feature: chronos_rerun can rerun old jobs

  Scenario: a job can be rerun
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run chronos_rerun for service_instance testservice testinstance
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
     Then the field "disabled" for the job stored as "myjob" is set to "False"

