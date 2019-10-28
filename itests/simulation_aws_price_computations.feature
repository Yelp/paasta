Feature: make sure we're computing spot prices correctly

    Scenario: one instance with constant price
       Given market A has 1 instance at time 0
         And market A costs $1/hour at time 0
        When the simulator runs for 2 hours
        Then the simulated cluster costs $2 total

    Scenario: one instance with price increase
       Given market A has 1 instance at time 0
         And market A costs $1/hour at time 0
         And market A costs $2/hour at time 1800
        When the simulator runs for 2 hours
        Then the simulated cluster costs $3 total

    Scenario: two instances in the same market are launched at the same time
       Given market A has 2 instances at time 0
         And market A costs $1/hour at time 0
         And market A costs $2/hour at time 1800
        When the simulator runs for 2 hours
        Then the simulated cluster costs $6 total

    Scenario: two instances in the same market are launched at different times
       Given market A has 1 instances at time 0
         And market A has 2 instances at time 1800
         And market A costs $1/hour at time 0
         And market A costs $2/hour at time 1200
        When the simulator runs for 2 hours
        Then the simulated cluster costs $6 total

    Scenario: two instances in different markets are launched at different times
       Given market A has 1 instance at time 0
         And market B has 1 instance at time 1800
         And market A costs $1/hour at time 0
         And market A costs $2/hour at time 1200
         And market B costs $0.50/hour at time 0
         And market B costs $0.75/hour at time 4500
        When the simulator runs for 2 hours
        Then the simulated cluster costs $3.875 total

    Scenario: (per-hour billing) two instances in different markets are launched at diff. times and one is terminated
       Given market A has 1 instance at time 0
         And market B has 1 instance at time 1920
         And market B has 0 instances at time 5400
         And market A costs $1/hour at time 0
         And market A costs $2/hour at time 1800
         And market B costs $0.50/hour at time 0
         And market B costs $0.75/hour at time 4500
        When the simulator runs for 2 hours
        Then the simulated cluster costs $3.5 total

    Scenario: (per-sec billing) two instances in different markets are launched at diff. times and one is terminated
       Given market A has 1 instance at time 0
         And market B has 1 instance at time 1920
         And market B has 0 instances at time 5400
         And market A costs $1/hour at time 0
         And market A costs $2/hour at time 1800
         And market B costs $0.50/hour at time 0
         And market B costs $0.75/hour at time 4500
        When the simulator runs for 2 hours and billing is per-second
        Then the simulated cluster costs $4.05 total
