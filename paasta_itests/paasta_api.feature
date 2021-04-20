Feature: paasta_api

  # Note that the following tests depend on the configuration of docker-compose.yml
  #  in paasta_itests.  This is unfortunate, but seems to be the easiest way to launch
  #  multiple slaves with different attributes
  Scenario: Grouping
    Given a working paasta cluster
     Then resources GET with groupings "pool" should return 2 groups
      And resources GET with groupings "region" should return 2 groups
      And resources GET with groupings "," should return 1 groups
      And resources GET with groupings "noexist" should return 1 groups
      And resources GET with groupings "ssd" should return 2 groups
      And resources GET with groupings "pool,region" should return 4 groups
      And resources GET with groupings "pool,region,ssd" should return 5 groups

  Scenario: Filters
    Given a working paasta cluster
     Then resources GET with groupings "pool" and filters "pool:default" should return 1 groups
      And resources GET with groupings "pool" and filters "region:fakeregion" should return 2 groups
      And resources GET with groupings "pool" and filters "ssd:true|region:fakeregion" should return 1 groups

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2
