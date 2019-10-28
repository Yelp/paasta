Feature: make sure the autoscaler pages the right people

    @yelp
    Scenario: the autoscaler is broken
        Given the autoscaler batch
         When the autoscaler fails
         Then the application owner should not get paged for evaluation
          And the service owner should get paged for evaluation

    @yelp
    Scenario Outline: a signal is broken
        Given the autoscaler batch
         When the <signal_type> signal fails
         Then the application owner should <page_application?> for evaluation
          And the service owner should <page_service?> for evaluation

      Examples: Signal Types
        | signal_type | page_application? | page_service? |
        | application | get paged         | not get paged |
        | default     | not get paged     | get paged     |

    @yelp
    Scenario: everything is fine
        Given the autoscaler batch
         When signal evaluation succeeds
         Then the application owner should not get paged for evaluation
          And the service owner should not get paged for evaluation

    @yelp
    Scenario: RequestLimitExceeded errors are ignored
        Given the autoscaler batch
         When a RequestLimitExceeded error occurs
         Then the application owner should not get paged for evaluation
          And the service owner should not get paged for evaluation
