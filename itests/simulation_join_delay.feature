Feature: make sure the simulator join-delay params work correctly

    Scenario Outline: instances should wait to join the cluster
        Given market A has 1 instance at time 0
         When the instance takes <time> seconds to join
          And the simulator runs for 2 hours
         Then the instance start time should be 0
          And the instance join time should be <time>

      Examples:
        | time |
        | 0    |
        | 300  |

    Scenario: instances should join the cluster immediate if the override is set
        Given market A has 1 instance at time 0
         When the instance takes 300 seconds to join
          And the join-delay override flag is set
          And the simulator runs for 2 hours
         Then the instance start time should be 0
          And the instance join time should be 0

    Scenario: the instance is terminated before it joins
        Given market A has 1 instance at time 0
          And market A has 0 instances at time 120
         When the instance takes 300 seconds to join
          And the simulator runs for 2 hours
         Then no instances should join the Mesos cluster

    Scenario: the instance is terminated after it joins
        Given market A has 1 instance at time 0
          And market A has 0 instances at time 1800
         When the instance takes 300 seconds to join
          And the simulator runs for 2 hours
         Then instances should join the Mesos cluster
