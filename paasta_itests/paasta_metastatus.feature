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
      And a task belonging to the app with id "memtest" is in the task list
     Then paasta_metastatus -v exits with return code "2" and output "CRITICAL: Less than 10% memory available."

  # paasta_metastatus defines "high" disk usage as > 90% of the total cluster
  # capacity. In docker-compose.yml, we set disk at 10240MB for the 1 mesos slave in use.
  Scenario: High disk usage
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "myservice.myinstance"
      And I have yelpsoa-configs for the marathon job "myservice.myotherinstance"
      And we have a deployments.json for the service "myservice" with enabled instance "myinstance"
      And we have a deployments.json for the service "myservice" with enabled instance "myotherinstance"
     When we set the constraints field of the marathon config for service "myservice" and instance "myinstance" to arrays "region,LIKE,region-1"
     When we set the constraints field of the marathon config for service "myservice" and instance "myotherinstance" to arrays "region,LIKE,region-2"
      And we set the "disk" field of the marathon config for service "myservice" and instance "myinstance" to integer 99
      And we set the "disk" field of the marathon config for service "myservice" and instance "myotherinstance" to integer 99
      And we run setup_marathon_job for service_instance "myservice.myinstance"
      And we run setup_marathon_job for service_instance "myservice.myotherinstance"
      And we wait for the service_instance "myservice.myinstance" to have the correct number of marathon tasks
      And we wait for the service_instance "myservice.myotherinstance" to have the correct number of marathon tasks
    Then paasta_metastatus exits with return code "2""

  Scenario: paasta metastatus alerts on high cpu usage
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "myservice.myinstance"
      And I have yelpsoa-configs for the marathon job "myservice.myotherinstance"
      And we have a deployments.json for the service "myservice" with enabled instance "myinstance"
      And we have a deployments.json for the service "myservice" with enabled instance "myotherinstance"
     When we set the constraints field of the marathon config for service "myservice" and instance "myinstance" to arrays "region,LIKE,region-1"
     When we set the constraints field of the marathon config for service "myservice" and instance "myotherinstance" to arrays "region,LIKE,region-2"
      And we set the "cpus" field of the marathon config for service "myservice" and instance "myinstance" to integer 9.9
      And we set the "cpus" field of the marathon config for service "myservice" and instance "myotherinstance" to integer 9.9
      And we run setup_marathon_job for service_instance "myservice.myinstance"
      And we run setup_marathon_job for service_instance "myservice.myotherinstance"
      And we wait for the service_instance "myservice.myinstance" to have the correct number of marathon tasks
      And we wait for the service_instance "myservice.myotherinstance" to have the correct number of marathon tasks
    Then paasta_metastatus exits with return code "2""

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


  Scenario: paasta metastatus vv shows information for both regions
    Given a working paasta cluster
    Then paasta_metastatus -vv exits with return code "0" and output "    region-1  10.0/10.0         512.0/512.0       100.0/100.0"
     And paasta_metastatus -vv exits with return code "0" and output "    region-2  10.0/10.0         512.0/512.0       100.0/100.0"

  Scenario: paasta metastatus vv -g region alerts on per-region and has the correct output
    Given a working paasta cluster
      And I have yelpsoa-configs for the marathon job "myservice.myinstance"
      And we have a deployments.json for the service "myservice" with enabled instance "myinstance"
     When we set the constraints field of the marathon config for service "myservice" and instance "myinstance" to arrays "region,LIKE,region-1"
      And we set the "cpus" field of the marathon config for service "myservice" and instance "myinstance" to integer 9.9
      And we run setup_marathon_job for service_instance "myservice.myinstance"
      And we wait for the service_instance "myservice.myinstance" to have the correct number of marathon tasks
    Then paasta_metastatus -vv exits with return code "2" and output "    region-1  0.1/10.0          502.0/512.0       99.9/100.0"
     And paasta_metastatus -vv exits with return code "2" and output "    region-2  10.0/10.0         512.0/512.0       100.0/100.0"
