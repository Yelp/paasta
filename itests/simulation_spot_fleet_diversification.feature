Feature: make sure simulated spot fleets diversify properly

    Scenario Outline: the simulated spot fleet should be diversified
        Given a simulated spot fleet resource group
         When we request <quantity> target capacity
         Then the simulated spot fleet should be diversified
          And the fulfilled capacity should be above the target capacity

      Examples:
        | quantity |
        | 200      |
        | 750      |
        | 1500     |

    Scenario Outline: the simulated spot fleet should refill empty markets
        Given a simulated spot fleet resource group
         When capacity in one market drops
          And we request <quantity> target capacity
         Then the spot fleet should have no instances from the empty market
          And the fulfilled capacity should be above the target capacity

      Examples:
        | quantity |
        | 100      |
        | 1000     |

    Scenario Outline: the simulated spot fleet should not fill markets that are over their target
        Given a simulated spot fleet resource group
         When capacity in one market is high
          And we request <quantity> target capacity
         Then the spot fleet should not add instances from the high market
          And the fulfilled capacity should be above the target capacity

      Examples:
        | quantity |
        | 100      |
        | 500      |
