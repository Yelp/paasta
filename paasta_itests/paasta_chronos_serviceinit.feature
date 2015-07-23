Feature: paasta_chronos_serviceinit
  Scenario: paasta_chronos_serviceinit can run status
    Given a working paasta cluster
    When we run the chronos job test-service.main
    And we wait for it to be deployed
    Then paasta_chronos_serviceinit status should return "Healthy"
