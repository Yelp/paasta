Feature: tail_paasta_logs

    Scenario: Happy path with real threads
        When we tail paasta logs and let threads run
        Then one message is displayed from each scribe env
