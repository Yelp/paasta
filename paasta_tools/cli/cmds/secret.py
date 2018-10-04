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
import os
import sys

from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.secret_tools import get_secret_provider
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    secret_parser = subparsers.add_parser(
        'secret',
        help="Add/update PaaSTA service secrets",
        description=(
            "This script allows you to add secrets to your services "
            "as environment variables. This script modifies your local "
            "checkout of yelpsoa-configs and you must then commit and "
            "push the changes back to git."
        ),
    )
    secret_parser.add_argument(
        "action",
        help="should be add/update",
        choices=["add", "update", "decrypt"],
    )
    secret_parser.add_argument(
        "-n",
        "--secret-name",
        required=True,
        help="The name of the secret to create/update, "
             "this is the name you will reference in your "
             "services marathon/chronos yaml files and should "
             "be unique per service. Please prefix with "
             "PAASTA_SECRET_ if you are going to use the "
             "yelp_servlib client library.",
    )
    secret_parser.add_argument(
        '-s', '--service',
        help='The name of the service on which you wish to act',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    secret_parser.add_argument(
        '-c', '--clusters',
        help="A comma-separated list of clusters to create secrets for. "
             "Note: this is translated to ecosystems because Vault is run "
             "at an ecosystem level. As a result you can only have different "
             "secrets per ecosystem. (it is not possible for example to encrypt "
             "a different value for norcal-prod vs nova-prod. "
             "Defaults to all clusters in which the service runs. "
             "For example: --clusters norcal-prod,nova-prod ",
    ).completer = lazy_choices_completer(list_clusters)
    secret_parser.add_argument(
        "-p",
        "--plain-text",
        required=False,
        type=str,
        help="Optionally specify the secret as a command line argument",
    )
    secret_parser.add_argument(
        "-i",
        "--stdin",
        required=False,
        action="store_true",
        default=False,
        help="Optionally pass the plaintext from stdin",
    )
    secret_parser.set_defaults(command=paasta_secret)


def print_paasta_helper(secret_path, secret_name):
    print(
        "\nYou have successfully encrypted your new secret and it\n"
        "has been stored at {}\n"
        "To use the secret in a service you can add it to your PaaSTA service\n"
        "as an environment variable.\n"
        "You do so by referencing it in the env dict in your yaml config:\n\n"
        "main:\n"
        "  cpus: 1\n"
        "  env:\n"
        "    PAASTA_SECRET_{}: SECRET({})\n\n"
        "Once you have referenced the secret you must commit the newly\n"
        "created/updated json file and your changes to your yaml config. When\n"
        "you push to master PaaSTA will bounce your service and the new\n"
        "secrets plaintext will be in the environment variable you have\n"
        "specified. The PAASTA_SECRET_ prefix is optional but necessary\n"
        "for the yelp_servlib client library".format(
            secret_path,
            secret_name.upper(),
            secret_name,
        ),
    )


def get_plaintext_input(args):
    if args.stdin:
        plaintext = sys.stdin.buffer.read()
    elif args.plain_text:
        plaintext = args.plain_text.encode('utf-8')
    else:
        print("Please enter the plaintext for the secret, press Ctrl-D when done.")
        lines = []
        while True:
            try:
                line = input()
            except EOFError:
                break
            lines.append(line)
        plaintext = '\n'.join(lines).encode('utf-8')
    return plaintext


def is_service_folder(soa_dir, service_name):
    return os.path.isfile(os.path.join(soa_dir, service_name, "service.yaml"))


def _get_secret_provider_for_service(service_name, cluster_names=None):
    if not is_service_folder(os.getcwd(), service_name):
        paasta_print(
            "You must run this tool from the root of your local yelpsoa checkout\n"
            "The tool modifies files in yelpsoa-configs that you must then commit\n"
            "and push back to git.",
        )
        sys.exit(1)
    system_paasta_config = load_system_paasta_config()
    secret_provider_kwargs = {
        'vault_cluster_config': system_paasta_config.get_vault_cluster_config(),
    }
    clusters = cluster_names.split(',') if cluster_names else list_clusters(
        service=service_name,
        soa_dir=os.getcwd(),
    )
    return get_secret_provider(
        secret_provider_name=system_paasta_config.get_secret_provider_name(),
        soa_dir=os.getcwd(),
        service_name=service_name,
        cluster_names=clusters,
        secret_provider_kwargs=secret_provider_kwargs,
    )


def paasta_secret(args):
    secret_provider = _get_secret_provider_for_service(args.service, cluster_names=args.clusters)
    if args.action in ["add", "update"]:
        secret_provider.write_secret(
            action=args.action,
            secret_name=args.secret_name,
            plaintext=get_plaintext_input(args),
        )
        secret_path = os.path.join(secret_provider.secret_dir, f"{args.secret_name}.json")
        print_paasta_helper(secret_path, args.secret_name)
    elif args.action == "decrypt":
        print(decrypt_secret(
            secret_provider=secret_provider,
            secret_name=args.secret_name,
        ))
    else:
        print("Unknown action")
        sys.exit(1)


def decrypt_secret(secret_provider, secret_name):
    if len(secret_provider.cluster_names) > 1:
        paasta_print(
            "Can only decrypt for one cluster at a time!\nFor example, try '-c norcal-devc'"
            " to decrypt the secret for this service in norcal-devc.",
        )
        sys.exit(1)
    return secret_provider.decrypt_secret(secret_name)
