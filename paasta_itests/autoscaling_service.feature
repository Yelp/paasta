Feature: service autoscaling works as expected
  Scenario: save_historical_load and fetch_historical_load can save and fetch data.
    Given a working paasta cluster, with docker registry doesntmatter.lol
    Given some fake historical load data
     When I save the fake historical load data
     Then I should get the same fake historical load data back when I fetch it
