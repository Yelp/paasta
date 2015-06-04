Feature: paasta_metastatus can describe health of cluster

    Scenario: paasta_metastatus exits cleanly if it can't connect to mesos
        Given an unresponsive mesos instance
        Then mesos_tools.fetch_mesos_stats() should raise a MissingMasterException
