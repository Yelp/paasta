from behave import then 
import sys
sys.path.append('../')

import paasta_tools
from paasta_tools import mesos_tools, paasta_metastatus

@then('mesos_tools.fetch_mesos_stats() should raise a MissingMasterException')
def fetch_mesos_stats_missing_mesos(context):
    try:
        paasta_metastatus.get_mesos_status()
    except mesos_tools.MissingMasterException:
        assert True
    else:
        assert False

