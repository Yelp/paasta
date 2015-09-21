Feature: paasta_tools can create marathon apps

  Scenario: Trivial marathon apps can be deployed
    Given a working paasta cluster
     When we create a trivial marathon app
     Then we should see it running in marathon

# vim: set ts=2 sw=2
