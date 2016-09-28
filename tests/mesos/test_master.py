from mock import patch

from paasta_tools.mesos import framework
from paasta_tools.mesos import master


@patch.object(master.MesosMaster, '_framework_list', autospec=True)
def test_frameworks(mock_framework_list):
    fake_frameworks = [
        {
            'name': 'test_framework1',
        },
        {
            'name': 'test_framework2',
        },
    ]
    mock_framework_list.return_value = fake_frameworks
    expected_frameworks = [framework.Framework(config) for config in fake_frameworks]
    mesos_master = master.MesosMaster({})
    assert expected_frameworks == mesos_master.frameworks()


@patch.object(master.MesosMaster, '_framework_list', autospec=True)
def test_framework_list_includes_completed_frameworks(mock_framework_list):
    fake_frameworks = [
        {
            'name': 'test_framework1',
        },
        {
            'name': 'test_framework2',
        },
    ]
    mock_framework_list.return_value = fake_frameworks
    expected_frameworks = [framework.Framework(config) for config in fake_frameworks]
    mesos_master = master.MesosMaster({})
    assert expected_frameworks == mesos_master.frameworks()
