Feature: 'paasta fsm' generates sane yelpsoa-configs entries

    Scenario: --auto
        Given a fake yelpsoa-config-root
        When we fsm a new service with --auto
        Then the new yelpsoa-configs directory has sane values

#    Scenario: everything specified
