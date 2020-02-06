# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
            _get_validated_args(None, mock_parser)

    def test_no_entrypoint(self, mock_parser, mock_args):
        mock_args.subcommand = 'foo'
        mock_parser.parse_args.return_value = mock_args
        with pytest.raises(SystemExit):
            _get_validated_args(None, mock_parser)

    def test_no_cluster(self, mock_parser, mock_args):
        mock_args.subcommand = 'foo'
        mock_args.cluster = None
        mock_parser.parse_args.return_value = mock_args
        with pytest.raises(SystemExit):
            _get_validated_args(None, mock_parser)
