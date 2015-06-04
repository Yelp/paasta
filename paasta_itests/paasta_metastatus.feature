Feature: paasta_metastatus can describe health of cluster

    Scenario: paasta_metastatus handles exceptional case of mesos not being available
        Given an unresponsive mesos instance
        Then mesos_tools.fetch_mesos_stats() should raise a MissingMasterException
