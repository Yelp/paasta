Feature: paasta_chronos_serviceinit
  Scenario: paasta_chronos_serviceinit can run status
    Given a working paasta cluster
    And a working chronos instance
    When we run the chronos job test-service.job
    And we wait for it to be deployed
    Then paasta_chronos_serviceinit status should return "Healthy"
