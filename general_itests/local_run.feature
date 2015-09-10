Feature: paasta local-run can be used

  Scenario: Running paasta local-run in non-interactive mode
    Given a simple service to test
     When we run paasta local-run in non-interactive mode with environment variable "FOO" set to "BAR"
     Then we should see the expected return code
      And we should see the environment variable "FOO" with the value "BAR" in the ouput
