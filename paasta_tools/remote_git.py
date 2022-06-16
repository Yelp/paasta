# Copyright 2015-2016 Yelp Inc.
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
import re

import dulwich.client
import dulwich.errors

from paasta_tools.utils import _run
from paasta_tools.utils import timeout


def _make_determine_wants_func(ref_mutator):
    """Returns a safer version of ref_mutator, suitable for passing as the
    determine_wants argument to dulwich's send_pack method. The returned
    function will not delete or modify any existing refs."""

    def determine_wants(old_refs):
        refs = {k.decode("UTF-8"): v.decode("UTF-8") for k, v in old_refs.items()}
        new_refs = ref_mutator(refs)
        new_refs = {k.encode("UTF-8"): v.encode("UTF-8") for k, v in new_refs.items()}
        new_refs.update(old_refs)  # Make sure we don't delete/modify anything.
        return new_refs

    return determine_wants


def make_force_push_mutate_refs_func(targets, sha):
    """Create a 'force push' function that will inform send_pack that we want
    to mark a certain list of target branches/tags to point to a particular
    git_sha.

    :param targets: List of branches/tags to point at the input sha
    :param sha: The git sha to point the branches/tags at
    :returns: A function to do the ref manipulation that a dulwich client can use"""

    def mutate_refs(refs):
        for target in targets:
            refs[target.encode("UTF-8")] = sha.encode("UTF-8")
        return refs

    return mutate_refs


def create_remote_refs(git_url, ref_mutator, force=False):
    """Creates refs (tags, branches) on a remote git repo.

    :param git_url: the URL or path to the remote git repo.
    :param ref_mutator: A function that determines the new refs to create on
                        the remote repo. This gets passed a dictionary of the
                        remote server's refs in the format {name : hash, ...},
                        and should return a dictionary of the same format.
    :param force: Bool, defaults to false. If true we will overwrite
                  refs even if they are already set.
    :returns: The map of refs, with our changes applied.
    """
    client, path = dulwich.client.get_transport_and_path(git_url)

    if force is False:
        determine_wants = _make_determine_wants_func(ref_mutator)
    else:
        determine_wants = ref_mutator
    # We know we don't need to push any objects.

    def generate_pack_contents(have, want):
        return []

    return client.send_pack(path, determine_wants, generate_pack_contents)


class LSRemoteException(Exception):
    pass


@timeout(
    seconds=60,
    error_message="Timed out connecting to git server, is it reachable from where you are?",
    use_signals=False,
)
def list_remote_refs(git_url):
    """Get the refs from a remote git repo as a dictionary of name->hash."""
    client, path = dulwich.client.get_transport_and_path(git_url)
    try:
        refs = client.fetch_pack(path, lambda refs: [], None, lambda data: None)
        return {k.decode("UTF-8"): v.decode("UTF-8") for k, v in refs.items()}
    except dulwich.errors.HangupException as e:
        raise LSRemoteException(f"Unable to fetch remote refs from {git_url}: {e}")


def get_authors(git_url, from_sha, to_sha):
    """Gets the list of authors who contributed to a git changeset.
    Currently only supports fetching this in a very "yelpy" way by
    executing a gitolite command"""
    matches = re.match("(?P<git_server>.*):(?P<git_repo>.*)", git_url)
    if matches is None:
        return (1, f"could not understand the git url {git_url} for authors detection")
    git_server = matches.group("git_server")
    git_repo = matches.group("git_repo")
    if git_server is None:
        return (
            1,
            f"could not understand the git server in {git_url} for authors detection",
        )
    if git_repo is None:
        return (
            1,
            f"could not understand the git repo in {git_url} for authors detection",
        )

    if "git.yelpcorp.com" in git_server:
        ssh_command = (
            f"ssh {git_server} authors-of-changeset {git_repo} {from_sha} {to_sha}"
        )
        return _run(command=ssh_command, timeout=5.0)
    else:
        # TODO: PAASTA-16927: support getting authors for services on GHE
        return 1, f"Fetching authors not supported for {git_server}"
