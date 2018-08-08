Feature: paasta tools can transmogrify tronfigs

  Scenario: paasta_setup_tron_namespace works
    Given some tronfig
    When we run paasta_setup_tron_namespace in dry-run mode
    Then it should have a return code of "0"
     And the output should contain "['fake_simple_service'], failed: [], skipped: 0"
