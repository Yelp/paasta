Feature: paasta_metastatus describes the state of the paasta cluster

#  Scenario: Zookeeper unreachable
#    Given a working paasta cluster
#     When all zookeepers are unavailable
#     Then metastatus returns 2

  # paasta_metastatus defines 'high' memory usage as > 90% of the total cluster
  # capacity. In docker-compose.yml, we set memory at 512MB for the 1 mesos slave in use;
  # this value was chosen as a tradeoff between reducing the requirements of
  # the machine running the itests and providing enough resources for the tests
  # to be performant. If you set it to a lower value, other tests seem to run
  # slowly and often fail.
  Scenario: High memory usage
    Given a working paasta cluster
     When an app with id "memtest" using high memory is launched
      And 3 tasks belonging to the app with id "memtest" are in the task list
     Then paasta_metastatus -v exits with return code "2" and output "CRITICAL: Less than 10% memory available."

  # paasta_metastatus defines "high" disk usage as > 90% of the total cluster
  # capacity. In docker-compose.yml, we set disk at 10240MB for the 1 mesos slave in use.
  Scenario: High disk usage
    Given a working paasta cluster
    When an app with id "disktest" using high disk is launched
     And 3 tasks belonging to the app with id "disktest" are in the task list
    Then paasta_metastatus -v exits with return code "2" and output "CRITICAL: Less than 10% disk available."

  # paasta_metastatus defines 'high' cpu usage as > 90% of the total cluster
  # capacity. in docker-compose.yml, we set cpus at 10 for the 1 mesos slave in use;
  # mainly this is just to use a round number. It's important to note that this
  # is a *limit* on the number of cpus used. The app that is launched in this
  # task is set to require 9 cpus, though again, it doesn't use this number,
  # just asks that mesos allocates 9 cpus of it's capacity to this task. As a
  # result, we have an easy way of simulating an app using 90% of the CPUs
  # available to mesos, whilst still consuming minimal resources whilst running
  # the test.
  Scenario: High cpu usage
    Given a working paasta cluster
     When an app with id "cputest" using high cpu is launched
      And 3 tasks belonging to the app with id "cputest" are in the task list
     Then paasta_metastatus -v exits with return code "2" and output "CRITICAL: Less than 10% CPUs available."

  Scenario: With a launched chronos job
    Given a working paasta cluster
     When we create a trivial marathon app
      And we create a trivial chronos job called "testjob"
      And we wait for the chronos job stored as "testjob" to appear in the job list
     Then paasta_metastatus -v exits with return code "0" and output "Enabled chronos jobs: 1"

#  Scenario: Mesos master unreachable
#    Given a working paasta cluster
#     When all masters are unavailable
#     Then metastatus returns 2

  Scenario: paasta metastatus verbose succeeds
    Given a working paasta cluster
     When we create a trivial marathon app
     Then paasta_metastatus -v exits with return code "0" and output " "
     Then paasta_metastatus -vv exits with return code "0" and output " "
     Then paasta_metastatus -vvv exits with return code "0" and output "mesosslave.test_hostname"

# vim: set ts=2 sw=2
