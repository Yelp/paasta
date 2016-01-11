# Copyright 2015 Yelp Inc.
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
import argparse

import pytest

from paasta_tools.cli.cli import get_argparser
from paasta_tools.cli.cli import main


def each_command():
    parser = get_argparser()
    # We're doing some wacky inspection here, let's make sure things are sane
    subparsers, = [
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    ]
    choices = tuple(subparsers.choices)
    assert choices
    assert 'local-run' in choices
    return choices


@pytest.mark.parametrize('cmd', each_command())
def test_help(cmd, capsys):
    # Should pass and produce something
    with pytest.raises(SystemExit) as excinfo:
        main((cmd, '--help'))
    assert excinfo.value.code == 0
    assert cmd in capsys.readouterr()[0]
