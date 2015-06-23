Feature: paasta_tools can create marathon apps

  Scenario: Trivial apps can be deployed
    Given a working paasta cluster
    When we create a trivial new app
    Then we should see it running via the marathon api

# vim: set ts=2 sw=2
