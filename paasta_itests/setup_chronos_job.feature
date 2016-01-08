Feature: setup_chronos_job can create and bounce jobs

  Scenario: complete jobs can be deployed
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
     Then we should see a job for the service "testservice" and instance "testinstance" in the job list

  Scenario: jobs can be bounced using the "graceful" method
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we set the "bounce" field of the chronos config for service "testservice" and instance "testinstance" to "graceful"
      And we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as myfirstjob
     Then the field "disabled" for the job stored as "myfirstjob" is set to "False"

     When we set the "cmd" field of the chronos config for service "testservice" and instance "testinstance" to "sleep 60m"
      And we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as mysecondjob
     Then the field "disabled" for the job stored as "myfirstjob" is set to "True"
      And the field "disabled" for the job stored as "mysecondjob" is set to "False"

     When we set the "cmd" field of the chronos config for service "testservice" and instance "testinstance" to "echo hello && sleep 60m"
      And we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as mythirdjob
     Then the field "disabled" for the job stored as "mysecondjob" is set to "True"
      And the field "disabled" for the job stored as "mythirdjob" is set to "False"

  Scenario: jobs will be cleaned up by the "graceful" bounce
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we set the "bounce" field of the chronos config for service "testservice" and instance "testinstance" to "graceful"
      And we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as myjob
     When we create 10 disabled jobs that look like the job stored as "myjob"
     When we set the "cmd" field of the chronos config for service "testservice" and instance "testinstance" to "sleep 60m"
      And we run setup_chronos_job for service_instance "testservice.testinstance"
     Then there should be 1 enabled jobs for the service "testservice" and instance "testinstance"
      And there should be 5 disabled jobs for the service "testservice" and instance "testinstance"

  Scenario: dependent jobs can be launched
    Given a working paasta cluster
      And we have yelpsoa-configs for the service "testservice" with enabled scheduled chronos instance "testinstance"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "testinstance"
     When we run setup_chronos_job for service_instance "testservice.testinstance"
      And we store the name of the job for the service testservice and instance testinstance as myjob

    Given we have yelpsoa-configs for the service "testservice" with enabled dependent chronos instance "dependentjob" and parent "testservice.testjob"
      And we have a deployments.json for the service "testservice" with enabled chronos instance "dependentjob"
     When we run setup_chronos_job for service_instance "testservice.dependentjob"
     Then there should be 1 enabled jobs for the service "testservice" and instance "dependentjob"
