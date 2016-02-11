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
import mock

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


def test_make_force_push_mutate_refs_func_overwrites_shas():
    target_branches = ['targeta', 'targetb']
    newsha = 'newsha'
    input_refs = {
        'refs/heads/foo': '12345',
        'refs/heads/targeta': '12345',
        'refs/heads/targetb': '12345',
        'refs/heads/ignored': '12345',
        'refs/tags/blah': '12345',
    }
    expected = {
        'refs/heads/foo': '12345',
        'refs/heads/targeta': newsha,
        'refs/heads/targetb': newsha,
        'refs/heads/ignored': '12345',
        'refs/tags/blah': '12345',
    }

    mutate_refs_func = remote_git.make_force_push_mutate_refs_func(
        target_branches=target_branches,
        sha=newsha,
    )
    actual = mutate_refs_func(input_refs)
    assert actual == expected


def identity(x):
    return x


@mock.patch('dulwich.client', autospec=True)
@mock.patch('paasta_tools.remote_git._make_determine_wants_func', autospec=True)
def test_create_remote_refs_is_safe_by_default(mock_make_determine_wants_func, mock_dulwich_client):
    git_url = 'fake_git_url'
    ref_mutator = identity
    fake_git_client = mock.Mock()
    mock_dulwich_client.get_transport_and_path.return_value = fake_git_client, 'fake_path'
    remote_git.create_remote_refs(
        git_url=git_url,
        ref_mutator=ref_mutator,
    )
    fake_git_client.send_pack.assert_called_once_with(
        'fake_path', mock_make_determine_wants_func.return_value, mock.ANY)


@mock.patch('dulwich.client', autospec=True)
def test_create_remote_refs_allows_force_and_uses_the_provided_mutator(mock_dulwich_client):
    git_url = 'fake_git_url'
    ref_mutator = identity
    fake_git_client = mock.Mock()
    mock_dulwich_client.get_transport_and_path.return_value = fake_git_client, 'fake_path'
    remote_git.create_remote_refs(
        git_url=git_url,
        ref_mutator=ref_mutator,
        force=True,
    )
    fake_git_client.send_pack.assert_called_once_with(
        'fake_path', ref_mutator, mock.ANY)
