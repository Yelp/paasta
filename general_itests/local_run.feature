Feature: paasta local-run can be used

  Scenario: Running paasta local-run in non-interactive mode on a Marathon service
    Given Docker is available
      And a simple service to test
     When we run paasta local-run on a Marathon service in non-interactive mode with environment variable "FOO" set to "BAR"
     Then it should have a return code of "42"
      And we should see the environment variable "FOO" with the value "BAR" in the output

  Scenario: Running paasta local-run against an adhoc job
     Given Docker is available
       And a simple service to test
      When we run paasta local-run on an interactive job
      Then it should have a return code of "42"

  Scenario: Running paasta local-run against a tron action
     Given Docker is available
       And a simple service to test
      When we run paasta local-run on a tron action
      Then it should have a return code of "42"
