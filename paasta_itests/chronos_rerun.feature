Feature: chronos_rerun can rerun old jobs

  Scenario: a job can be rerun
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run chronos_rerun for service_instance "testservice testinstance"
     Then we should get exit code 0
     And there is a temporary job for the service testservice and instance testinstance

  Scenario: a dependent job is converted to a scheduled job
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0

    Given we have yelpsoa-configs for the service "testservice" with enabled dependent chronos instance "dependentjob" and parent "testservice.testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "dependentjob"
     When we run chronos_rerun for service_instance "testservice dependentjob"
     Then we should get exit code 0
     And there is a temporary job for the service testservice and instance dependentjob


  Scenario: dates are properly interpolated
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we set the "cmd" field of the chronos config for service "testservice" and instance "testinstance" to "echo '%(shortdate)s'"
     When we run chronos_rerun for service_instance "testservice testinstance"
     Then we should get exit code 0
     When we store the name of the job for the service testservice and instance testinstance as myjob
     Then the field "disabled" for the job stored as "myjob" is set to "False"
      And the field "command" for the job stored as "myjob" is set to "echo '2016-03-13'"
     And there is a temporary job for the service testservice and instance testinstance


  Scenario: a dependent job is rerun with all his graph
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should get exit code 0

    Given we have yelpsoa-configs for the service "testservice" with enabled dependent chronos instance "dependentjob" and parent "testservice.testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "dependentjob,testinstance"
     When we run chronos_rerun for service_instance "testservice dependentjob" with args --run-all-related-jobs
     Then we should get exit code 0
     And there is a temporary job for the service testservice and instance testinstance
     And there is a temporary job for the service testservice and instance dependentjob
