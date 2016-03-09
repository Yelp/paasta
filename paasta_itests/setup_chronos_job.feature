Feature: setup_chronos_job can create and bounce jobs

  Scenario: complete jobs can be deployed
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should see a job for the service "testservice" and instance "testinstance" in the job list

  Scenario: jobs can be bounced using the "graceful" method
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we set the "bounce" field of the chronos config for service "testservice" and instance "testinstance" to "graceful"
      And we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as myfirstjob
     Then the field "disabled" for the job stored as "myfirstjob" is set to "False"

     When we set the "cmd" field of the chronos config for service "testservice" and instance "testinstance" to "sleep 60m"
      And we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as mynewjob
     Then the field "command" for the job stored as "mynewjob" is set to "sleep 60m"

  Scenario: dependent jobs can be launched
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as myjob

    Given we have yelpsoa-configs for the service "testservice" with enabled dependent chronos instance "dependentjob" and parent "testservice.testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "dependentjob"
     When we run setup_chronos_job for service_instance "testservice.dependentjob"
     Then there should be 1 enabled jobs for the service "testservice" and instance "dependentjob"

  Scenario: dependent jobs created before their parent are skipped
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled dependent chronos instance "dependentjob" and parent "testservice.testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "dependentjob"
     When we run setup_chronos_job for service_instance "testservice.dependentjob"
     Then setup_chronos_job exits with return code "0" and the output contains "Parent job could not be found"

  Scenario: stopped jobs are disabled
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as myjob
     Then the field "disabled" for the job stored as "myjob" is set to "False"

    Given we have a deployments.json for the service "testservice" with disabled instance "testinstance"
    When we store the name of the job for the service testservice and instance testinstance as mydisabledjob
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then the field "disabled" for the job stored as "mydisabledjob" is set to "True"
      And the job stored as "mydisabledjob" is disabled in chronos
