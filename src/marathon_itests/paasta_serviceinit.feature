Feature: paasta_serviceinit can control marathon tasks

  Scenario: paasta_serviceinit can run status
    Given a working marathon instance
    Given a currently running job - test-service.main
    When we wait a bit for it to be deployed
    Then paasta_serviceinit status should try to exit 0
