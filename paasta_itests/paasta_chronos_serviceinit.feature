Feature: paasta_chronos_serviceinit
  Scenario: paasta_chronos_serviceinit can run status
    Given a working paasta cluster
    When we create a trivial chronos job
    And the trivial chronos job appears in the job list
    Then paasta_chronos_serviceinit status should return "Healthy"
