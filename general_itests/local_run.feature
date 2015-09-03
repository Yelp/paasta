Feature: paasta local-run can be used

  Scenario: Running paasta local-run in non-interactive mode
    Given a simple service to test
     When we run paasta local-run in non-interactive mode
     Then we should see the expected return code
      And we should see the correct output
