from __future__ import absolute_import
from __future__ import unicode_literals

from mock import MagicMock
from mock import Mock
from pytest import raises

from paasta_tools.mesos import cluster
from paasta_tools.mesos import exceptions


def test_get_files_for_tasks_no_files():
    attrs = {'id': 'foo'}
    mock_task = MagicMock()
    mock_task.__getitem__.side_effect = lambda x: attrs[x]
    mock_file = Mock()
    mock_file.exists.return_value = False
    mock_task.file.return_value = mock_file
    files = cluster.get_files_for_tasks([mock_task], ['myfile'], 1)
    with raises(exceptions.FileNotFoundForTaskException) as excinfo:
        files = list(files)
    assert 'None of the tasks in foo contain the files in list myfile' in str(excinfo.value)


def test_get_files_for_tasks_all():
    mock_task = MagicMock()
    mock_file = Mock()
    mock_file.exists.return_value = True
    mock_task.file.return_value = mock_file
    files = cluster.get_files_for_tasks([mock_task], ['myfile'], 1)
    files = list(files)
    assert files == [mock_file]


def test_get_files_for_tasks_some():
    mock_task = MagicMock()
    mock_file = Mock()
    mock_file_2 = Mock()
    mock_file.exists.side_effect = [False, True]
    mock_task.file.side_effect = [mock_file, mock_file_2]
    files = cluster.get_files_for_tasks([mock_task], ['myfile', 'myotherfile'], 1)
    files = list(files)
    assert files == [mock_file_2]
