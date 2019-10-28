from argparse import Namespace

import mock
import pytest

from clusterman.args import _get_validated_args


@pytest.fixture
def mock_args():
    args = Namespace()
    args.log_level = 'debug'
    return args


@mock.patch('clusterman.args.argparse.ArgumentParser', autospec=True)
class TestArgumentParser:
    def test_no_subcommand(self, mock_parser, mock_args):
        mock_args.subcommand = None
        mock_parser.parse_args.return_value = mock_args
        with pytest.raises(SystemExit):
            _get_validated_args(mock_parser)

    def test_no_entrypoint(self, mock_parser, mock_args):
        mock_args.subcommand = 'foo'
        mock_parser.parse_args.return_value = mock_args
        with pytest.raises(SystemExit):
            _get_validated_args(mock_parser)

    def test_no_cluster(self, mock_parser, mock_args):
        mock_args.subcommand = 'foo'
        mock_args.cluster = None
        mock_parser.parse_args.return_value = mock_args
        with pytest.raises(SystemExit):
            _get_validated_args(mock_parser)
