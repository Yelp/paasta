#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
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
import getpass
import json
import os
from pathlib import Path

import hvac

from paasta_tools.utils import load_system_paasta_config


def add_subparser(subparsers):
    auth_parser = subparsers.add_parser(
        'auth',
        help="",
        description=(
            ""
        ),
    )
    auth_parser.add_argument(
        '-c', '--cluster',
        help="",
        required=True,
    )
    auth_parser.set_defaults(command=paasta_auth)


def paasta_auth(args):
    get_token(cluster=args.cluster)


def get_token(cluster):
    token = get_token_from_file(cluster)
    if token:
        return token
    else:
        token = get_token_from_vault(cluster)
        write_token_to_disk(cluster, token)


def write_token_to_disk(cluster, token):
    if not os.path.isfile(os.path.join(str(Path.home()), ".paasta_token.json")):
        with open(os.path.join(str(Path.home()), ".paasta_token.json"), 'w') as token_file:
            json.dump({}, token_file)
    with open(os.path.join(str(Path.home()), ".paasta_token.json"), 'r+') as token_file:
        tokens = json.load(token_file)
        tokens[cluster] = token
        token_file.seek(0)
        json.dump(tokens, token_file)


def get_token_from_vault(cluster):
    system_paasta_config = load_system_paasta_config()
    vault_cluster_map = system_paasta_config.get_vault_cluster_config()
    vault_url = system_paasta_config.get_vault_host_template().format(vault_cluster_map[cluster])
    vault_ca = system_paasta_config.get_vault_ca_template().format(vault_cluster_map[cluster])
    client = hvac.Client(url=vault_url, verify=vault_ca)
    username = getpass.getuser()
    client.auth_ldap(username, getpass.getpass())
    # token_data = client.create_token()
    # token = token_data['auth']['client_token']
    # print(token_data)
    # client = hvac.Client(url=vault_url, verify=vault_ca, token=token)
    token_data = client.create_token(
        meta={'username': username},
        # period='120s',
        ttl='120s',
        # no_parent=False,
        orphan=False,
        role='paasta_api',
        renewable=True,
    )
    print(token_data)
    token = token_data['auth']['client_token']
    return token


def get_token_from_file(cluster):
    try:
        with open(os.path.join(str(Path.home()), ".paasta_token.json")) as token_file:
            tokens = json.load(token_file)
            return tokens.get(cluster, None)
    except IOError:
        return None
