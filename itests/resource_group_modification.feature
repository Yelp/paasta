Feature: make sure the MesosPoolManager is requesting the right capacities

    Scenario Outline: initialization at minimum capacity
        Given a pool manager with 5 <rg_type> resource groups
         Then the resource groups should be at minimum capacity

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario Outline: balanced scale-up
        Given a pool manager with 5 <rg_type> resource groups
         When we request 53 capacity
         Then the resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario Outline: balanced scale-up with dry-run
        Given a pool manager with 5 <rg_type> resource groups
         When we request 53 capacity and dry-run is active
         Then the resource group capacities should not change

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario Outline: balanced scale-up after external modification
        Given a pool manager with 5 <rg_type> resource groups
          And the fulfilled capacity of resource group 1 is 13
         When we request 76 capacity
         Then the resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario Outline: imbalanced scale-up
        Given a pool manager with 5 <rg_type> resource groups
          And the fulfilled capacity of resource group 1 is 30
         When we request 1000 capacity
         Then the first resource group's capacity should not change
          And the remaining resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario Outline: balanced scale-down
        Given a pool manager with 5 <rg_type> resource groups
          And we request 1000 capacity
         When we request 80 capacity
         Then the resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario Outline: balanced scale-down with dry-run
        Given a pool manager with 5 <rg_type> resource groups
          And we request 1000 capacity
         When we request 80 capacity and dry-run is active
         Then the resource group capacities should not change

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario Outline: imbalanced scale-down
        Given a pool manager with 5 <rg_type> resource groups
          And we request 1000 capacity
          And the fulfilled capacity of resource group 1 is 1
         When we request 22 capacity
         Then the first resource group's capacity should not change
          And the remaining resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asg     |
        | sfr     |

    Scenario: one sfr is broken
        Given a pool manager with 5 sfr resource groups
         When resource group 1 is broken
          And we request 100 capacity
         Then the first resource group's capacity should not change
          And the remaining resource groups should have evenly-balanced capacity
          And the log should contain "resource group is broken"

    Scenario Outline: An ASG is marked stale
        Given a pool manager with 1 asg resource group
         When we request 10 capacity
          And we mark resource group 1 as stale
          And we request <capacity> capacity
         # the stale instances should get replaced on the next call
         Then resource group 1 should have <instance_count> instances

      Examples:
        | capacity | instance_count |
        | 10       | 20             |
        | 5        | 15             |
