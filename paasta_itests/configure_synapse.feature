Feature: configure_synapse
  Scenario: smoke test
    Given a working synapse-tools configuration and a blank synapse config
      And a zookeeper discovery file
      And yelpsoa-configs with smartstack.yaml for service "configure_synapse" and namespace "test"
      And environment_tools data
      And we have run configure_synapse
      And we have started synapse
      And we wait for haproxy to start
     Then we should see configure_synapse.test backend in the haproxy status
