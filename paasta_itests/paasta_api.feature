Feature: paasta_api

  Scenario: instance GET shows the status of service.instance
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "test-service.main"
      And we have a deployments.json for the service "test-service" with enabled instance "main"
     When we run the marathon app "test-service.main" with "1" instances
      And we wait for it to be deployed
     Then instance GET should return app_count "1" and an expected number of running instances for "test-service.main"
      And instance GET should return error code "404" for "test-service.non-existent"

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
