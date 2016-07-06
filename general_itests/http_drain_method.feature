Feature: HTTPDrainMethod

  Scenario: HTTPDrainMethod correctly sends requests
    Given a fake HTTP server
      And a HTTPDrainMethod configured to point at that server
     When we call drain() and get status code 200
     Then the server should see a request to /drain
      And the return value should be None

  Scenario: is_draining returns False when status code is not expected
    Given a fake HTTP server
      And a HTTPDrainMethod configured to point at that server
     When we call is_draining() and get status code 404
     Then the server should see a request to /is_draining
      And the return value should be False

  Scenario: is_draining returns True when status code is not expected
    Given a fake HTTP server
      And a HTTPDrainMethod configured to point at that server
     When we call is_draining() and get status code 200
     Then the server should see a request to /is_draining
      And the return value should be True
