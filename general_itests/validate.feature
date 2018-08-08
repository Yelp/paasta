Feature: paasta validate can be used

  Scenario: Running paasta validate against a valid service
    Given a "valid" service
    When we run paasta validate
    Then it should have a return code of "0"
     And everything should pass
     And the output should contain "has a valid instance"

  Scenario: Running paasta validate against an invalid service
    Given an "invalid" service
    When we run paasta validate
    Then it should have a return code of "1"
     And it should report an error in the output
     And the output should contain "The specified epsilon value "foo" does not conform to the ISO8601 format"
