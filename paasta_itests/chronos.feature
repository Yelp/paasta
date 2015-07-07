Feature: paasta_tools can interact with chronos

  Scenario: Listing chronos jobs
    Given a working chronos instance
    Then we should be able to list jobs
