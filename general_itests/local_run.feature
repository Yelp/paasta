Feature: paasta local-run can be used

  Scenario: Running paasta local-run in non-interactive mode on a Marathon service
    Given Docker is available
      And a simple service to test
     When we run paasta local-run on a Marathon service in non-interactive mode with environment variable "FOO" set to "BAR"
     Then we should see the expected return code
      And we should see the environment variable "FOO" with the value "BAR" in the ouput

  Scenario: Running paasta local-run in non-interactive mode on a Chronos job
    Given Docker is available
      And a simple service to test
     When we run paasta local-run in non-interactive mode on a chronos job
     Then we should see the expected return code

  Scenario: Running paasta local-run in non-interactive mode on a Chronos job with a complex command
    Given Docker is available
      And a simple service to test
     When we run paasta local-run in non-interactive mode on a chronos job with cmd set to 'echo hello && sleep 5'
     Then we should see the expected return code