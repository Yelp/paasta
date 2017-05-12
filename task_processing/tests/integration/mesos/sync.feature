Feature: Mesos Sync runner

Scenario: Running single task
  Given mesos platform
  And 1 mesos slave
  And mesos executor with sync runner
  When I launch a task
  Then it should block until finished
  And print status running
  And print status finished
