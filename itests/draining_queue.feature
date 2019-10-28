Feature: make sure the drainer is working properly

    Scenario: process the draining queue
        Given a draining client
          And a message in the draining queue
         When the draining queue is processed
         Then the host should be submitted for termination
          And all queues are empty

    Scenario: process the termination queue
        Given a draining client
          And a message in the termination queue
         When the termination queue is processed
         Then the host should be terminated
          And all queues are empty

    Scenario: process the warning queue
        Given a draining client
          And a message in the warning queue
         When the warning queue is processed
         Then the host should be submitted for draining
          And all queues are empty
