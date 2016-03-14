Feature: chronos_rerun can rerun old jobs

  Scenario: a job can be rerun
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
      And we run chronos_rerun for service_instance testservice testinstance

