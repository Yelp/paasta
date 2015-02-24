Feature: _run will timeout

  Scenario: Trivial command will timeout
    When we run a trivial command with timeout 0.01 seconds
    Then the command is killed with signal SIGKILL
