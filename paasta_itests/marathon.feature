@marathon
Feature: paasta_tools can create marathon apps

  Scenario: Trivial marathon apps can be deployed
    Given a working paasta cluster
     When we create a trivial marathon app
     Then we should see it running in marathon

  Scenario: get_marathon_services_running_here_for_nerve works
    Given a working paasta cluster
     When we create a trivial marathon app
     Then we should see it running in marathon
     When the task has started
     Then it should show up in marathon_services_running_here

# vim: set ts=2 sw=2
