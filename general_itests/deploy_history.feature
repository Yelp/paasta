Feature: paasta deploy-history shows the deploy history of a service

    Scenario: deploy-history lists every deploy of every deploy group by default
        Given a service with a git repo that has deploy tags
         When we run paasta deploy-history
         Then it should have a return code of "0"
          And the history should list, newest first:
              | timestamp       | deploy_group       | commit |
              | 20260401T000000 | test-cluster.main  | first  |
              | 20260301T000000 | test-cluster.main  | second |
              | 20260201T000000 | test-cluster.canary | second |
              | 20260101T000000 | test-cluster.main  | first  |
          And the output should not contain the rollback tag

    Scenario: deploy-history can be filtered to one deploy group and limited
        Given a service with a git repo that has deploy tags
         When we run paasta deploy-history with "-l test-cluster.main -n 2"
         Then it should have a return code of "0"
          And the history should list, newest first:
              | timestamp       | deploy_group      | commit |
              | 20260401T000000 | test-cluster.main | first  |
              | 20260301T000000 | test-cluster.main | second |
          And the output should contain "Showing the 2 most recent of 3 deploys"

    Scenario: deploy-history fails when no valid deploy group is given
        Given a service with a git repo that has deploy tags
         When we run paasta deploy-history with "-l test-cluster.typo"
         Then it should have a return code of "1"
          And the output should contain "No valid deploy groups found"
