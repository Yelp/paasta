Feature: HacheckDrainManager can talk to hacheck correctly
  Scenario: HacheckDrainManager can drain a task
    Given a working hacheck container
      And a fake task to drain
      And a HacheckDrainMethod object with delay 10
     When we down a task
     Then the task should be downed
      And the task should not be safe to kill after 9 seconds
      And the task should be safe to kill after 11 seconds
