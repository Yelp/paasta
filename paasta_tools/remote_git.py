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

import dulwich.client
import dulwich.errors


def _make_determine_wants_func(ref_mutator):
    """Returns a safer version of ref_mutator, suitable for passing as the
    determine_wants argument to dulwich's send_pack method. The returned
    function will not delete or modify any existing refs."""
    def determine_wants(old_refs):
        refs = dict(old_refs)
        new_refs = ref_mutator(refs)
        new_refs.update(old_refs)  # Make sure we don't delete/modify anything.
        return new_refs
    return determine_wants


def create_remote_refs(git_url, ref_mutator):
    """Creates refs (tags, branches) on a remote git repo.

    :param git_url: the URL or path to the remote git repo.
    :param ref_mutator: A function that determines the new refs to create on
                        the remote repo. This gets passed a dictionary of the
                        remote server's refs in the format {name : hash, ...},
                        and should return a dictionary of the same format.
    :returns: The map of refs, with our changes applied.
    """
    client, path = dulwich.client.get_transport_and_path(git_url)

    determine_wants = _make_determine_wants_func(ref_mutator)
    # We know we don't need to push any objects.
    generate_pack_contents = lambda have, want: []

    return client.send_pack(path, determine_wants, generate_pack_contents)


class LSRemoteException(Exception):
    pass


def list_remote_refs(git_url):
    """Get the refs from a remote git repo as a dictionary of name->hash."""
    client, path = dulwich.client.get_transport_and_path(git_url)
    try:
        return client.fetch_pack(path, lambda refs: [], None, None)
    except dulwich.errors.HangupException as e:
        raise LSRemoteException("Unable to fetch remote refs: %s" % e)
