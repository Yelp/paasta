Feature: 'paasta fsm' generates sane yelpsoa-configs entries

    Scenario: by default
        Given a fake yelpsoa-config-root with an existing service
        When we fsm a new service
        Then the new yelpsoa-configs directory has a valid smartstack proxy_port
