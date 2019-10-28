Feature: make sure we're pruning the right instances on scale-down

    Scenario: target capacity equals fulfilled capacity
        Given a pool manager with 1 sfr resource group
          And the fulfilled capacity of resource group 1 is 10
         When we prune excess fulfilled capacity to 10
         Then 0 instances should be killed

    Scenario: no eligible instances to kill
        Given a pool manager with 1 sfr resource group
          And the fulfilled capacity of resource group 1 is 11
          And there are no killable instances
         When we prune excess fulfilled capacity to 10
         Then 0 instances should be killed

    Scenario: the killable instance would remove too much capacity
        Given a pool manager with 1 sfr resource group
          And the fulfilled capacity of resource group 1 is 11
          And the killable instance has weight 2
         When we prune excess fulfilled capacity to 10
         Then 0 instances should be killed
          And the log should contain "is at target capacity"

    Scenario: the killable instance would remove too many tasks
        Given a pool manager with 1 sfr resource group
          And the fulfilled capacity of resource group 1 is 11
          And we can kill at most 1 task
          And the killable instance has 2 tasks
         When we prune excess fulfilled capacity to 10
         Then 0 instances should be killed
          And the log should contain "would take us over our max_tasks_to_kill"

    Scenario: the killable instance would reduce the non-orphaned capacity too much
        Given a pool manager with 1 sfr resource group
          And the fulfilled capacity of resource group 1 is 11
          And the non-orphaned fulfilled capacity is 9
         When we prune excess fulfilled capacity to 10
         Then 0 instances should be killed
          And the log should contain "would take us under our target_capacity"

    Scenario: the killable instance can be pruned
        Given a pool manager with 1 sfr resource group
          And the fulfilled capacity of resource group 1 is 11
         When we prune excess fulfilled capacity to 10
         Then 1 instance should be killed

    Scenario: don't kill stale instances until non-stale instances are up
        Given a pool manager with 2 sfr resource groups
          And the fulfilled capacity of resource group 1 is 9
          And the fulfilled capacity of resource group 2 is 5
          And resource group 2 is stale
         When we prune excess fulfilled capacity to 11
         Then 3 instances should be killed
          And the killed instances are from resource group 2

    Scenario: don't kill everything when all resource groups are stale
        Given a pool manager with 1 sfr resource group
          And the fulfilled capacity of resource group 1 is 9
          And resource group 1 is stale
         When we prune excess fulfilled capacity to 9
         Then 0 instances should be killed

    Scenario: kill stale instances in an ASG
        Given a pool manager with 1 asg resource group
          And the fulfilled capacity of resource group 1 is 10
          And we mark resource group 1 as stale
          And the fulfilled capacity of resource group 1 is 15
          And the non-orphaned fulfilled capacity is 15
         When we prune excess fulfilled capacity to 10
         Then 5 instances should be killed
          And the killed instances should be stale
