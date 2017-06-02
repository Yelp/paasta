Feature: Paasta native mesos framework
  Scenario: we can start a service
    Given a working paasta cluster, with docker registry docker.io
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then it should eventually start 3 tasks

  Scenario: native_mesos_scheduler.main() works
    Given a working paasta cluster, with docker registry docker.io
      And a fresh soa_dir
      And paasta_native-cluster.yaml and deployments.json files for service foo with instance one
      And paasta_native-cluster.yaml and deployments.json files for service bar with instance two
      And paasta_native-cluster.yaml and deployments.json files for service baz with instance three
     When we run native_mesos_scheduler.main()
     Then there should be a framework registered with name paasta_native foo.one
     Then there should be a framework registered with name paasta_native bar.two
     Then there should be a framework registered with name paasta_native baz.three

  Scenario: native_mesos_scheduler.main() calls periodic
    Given a working paasta cluster, with docker registry docker.io
      And a fresh soa_dir
      And paasta_native-cluster.yaml and deployments.json files for service foo with instance one
     When we run native_mesos_scheduler.main()
     Then there should be a framework registered with name paasta_native foo.one
      And periodic() should eventually be called

  Scenario: reuse same framework ID
    Given a working paasta cluster
      And a new paasta_native config to be deployed, with 1 instances
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then there should be a framework registered with name test
     When we stop that framework without terminating
      And we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then there should be a framework registered with name test
      And it should have the same ID as before

  Scenario: new framework ID after termination
    Given a working paasta cluster
      And a new paasta_native config to be deployed, with 1 instances
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then there should be a framework registered with name test
     When we terminate that framework
     Then there should not be a framework registered with name test
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then there should be a framework registered with name test
      And it should have a different ID than before

  Scenario: native_mesos_scheduler bounces when config changes
    Given a working paasta cluster, with docker registry docker.io
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then it should eventually start 3 tasks
     When we change force_bounce
     Then it should eventually start 6 tasks
      And it should eventually drain 3 tasks
     When a task has drained
      And we call periodic
     Then it should eventually have only 5 tasks
     When a task has drained
      And we call periodic
      And we call periodic
     Then it should eventually have only 4 tasks
     When a task has drained
      And we call periodic
     Then it should eventually have only 3 tasks

  Scenario: native_mesos_scheduler scales down when instances decreases
    Given a working paasta cluster, with docker registry docker.io
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then it should eventually start 3 tasks
     When we change instances to 2
      And we call periodic
     Then it should eventually drain 1 tasks
     When a task has drained
      And we call periodic
     Then it should eventually have only 2 tasks

  Scenario: native_mesos_scheduler undrains when rolling back
    Given a working paasta cluster, with docker registry docker.io
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler with reconcile_backoff 0 and name test
     Then it should eventually start 3 tasks
     When we change force_bounce
     Then it should eventually start 6 tasks
      And it should eventually drain 3 tasks
     When we change force_bounce back
      And we call periodic
     Then it should undrain 3 tasks and drain 0 more
     When we call periodic
     Then it should undrain 0 tasks and drain 3 more

  Scenario: native_mesos_scheduler waits for task reconciliation before accepting offers
    Given a working paasta cluster, with docker registry docker.io
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler with reconcile_backoff 10 and name test
     Then it should not start tasks for 8 seconds
      And it should eventually start 3 tasks

  Scenario: get_paasta_native_services_running_here_for_nerve works
    Given a working paasta cluster, with docker registry docker.io
      And a new paasta_native config to be deployed, with 3 instances
     When we start a paasta_native scheduler with reconcile_backoff 0 and name paasta_native fake_service.fake_instance
     Then it should eventually start 3 tasks
      And our service should show up in paasta_native_services_running_here 3 times on any of our slaves
