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

from paasta_tools import remote_git


def test_make_determine_wants_func():
    refs = {
        'refs/heads/foo': 'abcde',
        'refs/tags/blah': '12345',
    }
    # nothing changed, so nothing should change
    determine_wants = remote_git._make_determine_wants_func(lambda x: x)
    assert determine_wants(refs) == refs

    # don't delete anything.
    determine_wants = remote_git._make_determine_wants_func(lambda x: {})
    assert determine_wants(refs) == refs

    # don't modify anything existing.
    determine_wants = remote_git._make_determine_wants_func(
        lambda x: dict((k, v[::-1]) for k, v in x.items())
    )
    assert determine_wants(refs) == refs

    # only allow new things
    determine_wants = remote_git._make_determine_wants_func(
        lambda x: {'foo': 'bar'}
    )
    actual = determine_wants(refs)
    expected = dict(refs.items() + [('foo', 'bar')])
    assert actual == expected
