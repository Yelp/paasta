Feature: paasta_execute_docker_command can find and run commands inside a docker container

  Scenario: paasta_execute_docker_command can run a trivial command
   Given Docker is available
    And a running docker container with task id foo and image ubuntu:xenial
    When we paasta_execute_docker_command a command with exit code 0 in container with task id foo
    Then the exit code is 0

  Scenario: paasta_execute_docker_command exits when it cannot find the container
   Given Docker is available
    And a running docker container with task id foo and image ubuntu:xenial
    When we paasta_execute_docker_command a command with exit code 0 in container with task id bar
    Then the exit code is 1

  Scenario: paasta_execute_docker_command reuses exec instances
   Given Docker is available
    And a running docker container with task id foo and image ubuntu:xenial
    When we paasta_execute_docker_command a command with exit code 0 in container with task id foo
     And we paasta_execute_docker_command a command with exit code 0 in container with task id foo
    Then the docker container has at most 1 exec instances
