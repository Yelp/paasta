from mock import Mock
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


@patch.object(master.MesosMaster, 'fetch', autospec=True)
def test__frameworks(mock_fetch):
    mesos_master = master.MesosMaster({})
    mock_frameworks = Mock()
    mock_fetch.return_value = Mock(json=Mock(return_value=mock_frameworks))
    ret = mesos_master._frameworks
    mock_fetch.assert_called_with(mesos_master, "/master/frameworks")
    assert ret == mock_frameworks


@patch.object(master.MesosMaster, '_frameworks', autospec=True)
def test__framework_list(mock__frameworks):
    mock_frameworks = Mock()
    mock_completed = Mock()
    mock__frameworks.__get__ = Mock(return_value={'frameworks': [mock_frameworks],
                                                  'completed_frameworks': [mock_completed]})
    mesos_master = master.MesosMaster({})
    ret = mesos_master._framework_list()
    expected = [mock_frameworks, mock_completed]
    assert list(ret) == expected

    ret = mesos_master._framework_list(active_only=True)
    expected = [mock_frameworks]
    assert list(ret) == expected
