Feature: Paasta native mesos framework
  Scenario: we can start a service
    Given a working paasta cluster, with docker registry docker.io
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler
     Then it should eventually start 3 tasks

  Scenario: native_mesos_scheduler.main() works
    Given a working paasta cluster, with docker registry docker.io
      And a fresh soa_dir
      And paasta_native-cluster.yaml and deployments.json files for service foo with instance one
      And paasta_native-cluster.yaml and deployments.json files for service bar with instance two
      And paasta_native-cluster.yaml and deployments.json files for service baz with instance three
     When we run native_mesos_scheduler.main()
     Then there should be a framework registered with id paasta foo.one
     Then there should be a framework registered with id paasta bar.two
     Then there should be a framework registered with id paasta baz.three
