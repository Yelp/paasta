Feature: paasta validate can be used

  Scenario: Running paasta validate against a valid service
    Given a "valid" service
    When we run paasta validate
    Then it should have a return code of "0"
     And everything should pass

  Scenario: Running paasta validate against an invalid service
    Given an "invalid" service
    When we run paasta validate
    Then it should have a return code of "1"
     And it should report an error in the output
     And the output should contain "Failed to"
