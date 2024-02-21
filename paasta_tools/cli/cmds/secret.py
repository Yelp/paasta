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
import argparse
import os
import re
import sys
from typing import Any
from typing import Dict
from typing import Optional

from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import get_namespaces_for_secret
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import select_k8s_secret_namespace
from paasta_tools.kubernetes_tools import get_paasta_secret_name
from paasta_tools.kubernetes_tools import get_secret
from paasta_tools.kubernetes_tools import KUBE_CONFIG_USER_PATH
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.secret_providers import SecretProvider
from paasta_tools.secret_tools import decrypt_secret_environment_variables
from paasta_tools.secret_tools import get_secret_provider
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.utils import _log_audit
from paasta_tools.utils import is_secrets_for_teams_enabled
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config

SECRET_NAME_REGEX = r"([A-Za-z0-9_-]*)"


def check_secret_name(secret_name_arg: str):
    pattern = re.compile(SECRET_NAME_REGEX)
    if (
        not secret_name_arg.startswith("-")
        and not secret_name_arg.startswith("_")
        and "".join(pattern.findall(secret_name_arg)) == secret_name_arg
    ):
        return secret_name_arg
    raise argparse.ArgumentTypeError(
        "--secret-name argument should only contain letters, numbers, "
        "dashes and underscores characters and cannot start from latter two"
    )


def add_add_subparser(subparsers):
    secret_parser_add = subparsers.add_parser("add", help="adds a paasta secret")
    _add_common_args(secret_parser_add)
    _add_and_update_args(secret_parser_add)


def add_update_subparser(subparsers):
    secret_parser_update = subparsers.add_parser(
        "update", help="updates a paasta secret"
    )
    _add_common_args(secret_parser_update)
    _add_and_update_args(secret_parser_update)


def add_decrypt_subparser(subparsers):
    secret_parser_decrypt = subparsers.add_parser(
        "decrypt", help="decrypts a single paasta secret"
    )
    _add_common_args(secret_parser_decrypt)

    secret_parser_decrypt.add_argument(
        "-n",
        "--secret-name",
        type=check_secret_name,
        required=True,
        help="The name of the secret to decrypt, this is the secret filename without the extension.",
    )

    secret_parser_decrypt.add_argument(
        "-c",
        "--clusters",
        required=False,
        type=_validate_single_cluster,
        help=(
            "The cluster to decrypt for, e.g. pnw-prod. "
            "Note: for decrypt only one cluster is allowed. "
            "This argument is required unless the secret is only defined in one cluster."
        ),
    ).completer = lazy_choices_completer(list_clusters)


def _validate_single_cluster(arg: str) -> str:
    if len(arg.split(",")) > 1:
        raise argparse.ArgumentTypeError("can only decrypt from one cluster at a time")
    return arg


def add_run_subparser(subparsers):
    secret_parser_run = subparsers.add_parser(
        "run",
        help="runs a command with paasta secrets",
        description=(
            "Runs a command with the secret environment variables from "
            "a given service instance. The command is run directly, not "
            "in a Docker container. "
            "Only the environment variables containing secrets are included. "
            "No attempt at redacting secrets appearing in the output is made."
        ),
        conflict_handler="resolve",
    )
    _add_common_args(secret_parser_run, allow_shared=False)

    secret_parser_run.add_argument(
        "-i",
        "--instance",
        help=(
            "Instance of the service to retrieve secret environment variables "
            "for, such as 'main' or 'canary'. Secrets will be selected and "
            "mapped to environment variables based on the configs for the instance."
        ),
        required=True,
    ).completer = lazy_choices_completer(list_instances)

    secret_parser_run.add_argument(
        "-c",
        "--clusters",
        required=True,
        type=_validate_single_cluster,
        help=(
            "The cluster to retrieve secrets for, e.g. norcal-devc. "
            "A list of clusters is not supported for this command."
        ),
    ).completer = lazy_choices_completer(list_clusters)

    secret_parser_run.add_argument(
        "cmd",
        nargs="*",
        default=["bash"],
        help=(
            "The command to run with the specified PaaSTA secrets. "
            "If not given, starts an interactive bash shell."
        ),
    )


def _add_and_update_args(parser: argparse.ArgumentParser):
    """common args for `add` and `update`."""
    parser.add_argument(
        "-p",
        "--plain-text",
        required=False,
        type=str,
        help="Optionally specify the secret as a command line argument",
    )
    parser.add_argument(
        "-i",
        "--stdin",
        required=False,
        action="store_true",
        default=False,
        help="Optionally pass the plaintext from stdin",
    )
    parser.add_argument(
        "--cross-env-motivation",
        required=False,
        type=str,
        help=(
            "Provide motivation in case the same value is being duplicated "
            "across multiple runtime environments when adding or updating a secret"
        ),
        metavar="MOTIVATION",
    )
    parser.add_argument(
        "-n",
        "--secret-name",
        type=check_secret_name,
        required=True,
        help="The name of the secret to create/update, "
        "this is the name you will reference in your "
        "services yaml files and should "
        "be unique per service.",
    )

    parser.add_argument(  # type: ignore
        "-c",
        "--clusters",
        help="A comma-separated list of clusters to create secrets for. "
        "Note: this is translated to ecosystems because Vault is run "
        "at an ecosystem level. As a result you can only have different "
        "secrets per ecosystem. (it is not possible for example to encrypt "
        "a different value for pnw-prod vs nova-prod. "
        "Defaults to all clusters in which the service runs. "
        "For example: --clusters pnw-prod,nova-prod ",
    ).completer = lazy_choices_completer(list_clusters)


def _add_vault_auth_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--vault-auth-method",
        help="Override how we auth with vault, defaults to token if not present",
        type=str,
        dest="vault_auth_method",
        required=False,
        default="token",
        choices=["token", "ldap"],
    )
    parser.add_argument(
        "--vault-token-file",
        help="Override vault token file, defaults to %(default)s",
        type=str,
        dest="vault_token_file",
        required=False,
        default="/var/spool/.paasta_vault_token",
    )


def _add_common_args(parser: argparse.ArgumentParser, allow_shared: bool = True):
    # available from any subcommand
    parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        help="A directory from which yelpsoa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )

    _add_vault_auth_args(parser)

    if allow_shared:
        service_group = parser.add_mutually_exclusive_group(required=True)
    else:
        service_group = parser  # type: ignore

    service_group.add_argument(  # type: ignore
        "-s",
        "--service",
        required=not allow_shared,
        help="The name of the service on which you wish to act",
    ).completer = lazy_choices_completer(list_services)

    if allow_shared:
        service_group.add_argument(
            "--shared",
            help="Act on a secret that can be shared by all services",
            action="store_true",
        )
    else:
        service_group.add_argument(
            "--shared",
            action="store_false",
            help=argparse.SUPPRESS,
        )


def add_subparser(subparsers):
    secret_parser = subparsers.add_parser(
        "secret",
        help="Add/update/read PaaSTA service secrets",
        description=(
            "This set of commands allows you to add, update, or read secrets for your services, "
            "configured as environment variables. If adding or updating, it modifies your local "
            "checkout of yelpsoa-configs and you must then commit and "
            "push the changes back to git."
        ),
    )
    secret_parser.set_defaults(command=paasta_secret)

    secret_subparsers = secret_parser.add_subparsers(
        dest="action",
        title="paasta secret subcommands",
        help=(
            "Run paasta secret <SUBCOMMAND> --help for information on per-subcommand arguments. "
            "Not all arguments are available on all subcommands."
        ),
    )

    add_add_subparser(secret_subparsers)
    add_decrypt_subparser(secret_subparsers)
    add_update_subparser(secret_subparsers)
    add_run_subparser(secret_subparsers)


def secret_name_for_env(secret_name):
    secret_name = secret_name.upper()
    valid_parts = re.findall(r"[a-zA-Z0-9_]+", secret_name)
    return "_".join(valid_parts)


def print_paasta_helper(secret_path, secret_name, is_shared):
    print(
        "\nYou have successfully encrypted your new secret and it\n"
        "has been stored at {}\n"
        "To use the secret in a service you can add it to your PaaSTA service\n"
        "as an environment variable.\n"
        "You do so by referencing it in the env dict in your yaml config:\n\n"
        "main:\n"
        "  cpus: 1\n"
        "  env:\n"
        "    PAASTA_SECRET_{}: {}SECRET({})\n\n"
        "Once you have referenced the secret you must commit the newly\n"
        "created/updated json file and your changes to your yaml config. When\n"
        "you push to master PaaSTA will bounce your service and the new\n"
        "secrets plaintext will be in the environment variable you have\n"
        "specified. The PAASTA_SECRET_ prefix is optional but necessary\n"
        "for the yelp_servlib client library".format(
            secret_path,
            secret_name_for_env(secret_name),
            "SHARED_" if is_shared else "",
            secret_name,
        )
    )


def get_plaintext_input(args):
    if args.stdin:
        plaintext = sys.stdin.buffer.read()
    elif args.plain_text:
        plaintext = args.plain_text.encode("utf-8")
    else:
        print(
            "Please enter the plaintext for the secret, then enter a newline and Ctrl-D when done."
        )
        lines = []
        while True:
            try:
                line = input()
            except EOFError:
                break
            lines.append(line)
        plaintext = "\n".join(lines).encode("utf-8")
        print("The secret as Python bytes is:", repr(plaintext))
        print("Please make sure the value inside the quotes is correct.")
    return plaintext


def is_service_folder(soa_dir, service_name):
    return os.path.isfile(os.path.join(soa_dir, service_name, "service.yaml"))


def _get_secret_provider_for_service(
    service_name: str,
    cluster_names: Optional[str] = None,
    soa_dir: Optional[str] = None,
    secret_provider_extra_kwargs: Optional[Dict[str, Any]] = None,
) -> SecretProvider:
    secret_provider_extra_kwargs = secret_provider_extra_kwargs or {}
    soa_dir = soa_dir or os.getcwd()

    if not is_service_folder(soa_dir, service_name):
        print(
            "{} not found.\n"
            "You must run this tool from the root of your local yelpsoa checkout\n"
            "The tool modifies files in yelpsoa-configs that you must then commit\n"
            "and push back to git.".format(os.path.join(service_name, "service.yaml"))
        )
        sys.exit(1)
    system_paasta_config = load_system_paasta_config()
    secret_provider_kwargs = {
        "vault_cluster_config": system_paasta_config.get_vault_cluster_config(),
        **secret_provider_extra_kwargs,
    }
    clusters = (
        cluster_names.split(",")
        if cluster_names
        else list_clusters(service=service_name, soa_dir=soa_dir)
    )

    return get_secret_provider(
        secret_provider_name=system_paasta_config.get_secret_provider_name(),
        soa_dir=soa_dir,
        service_name=service_name,
        cluster_names=clusters,
        secret_provider_kwargs=secret_provider_kwargs,
    )


def paasta_secret(args):
    if args.shared:
        service = SHARED_SECRET_SERVICE
        if not args.clusters:
            print("A list of clusters is required for shared secrets.")
            sys.exit(1)
    else:
        service = args.service

    # Check if "decrypt" and "secrets_for_teams" first to avoid vault auth
    if args.action == "decrypt" and is_secrets_for_teams_enabled(
        service, args.yelpsoa_config_root
    ):
        clusters = (
            args.clusters.split(",")
            if args.clusters
            else list_clusters(service=service, soa_dir=os.getcwd())
        )

        if len(clusters) > 1 or not clusters:
            print(
                "Can only decrypt for one specified cluster at a time!\nFor example,"
                " try '-c norcal-devc' to decrypt the secret for this service in norcal-devc."
            )
            sys.exit(1)

        kube_client = KubeClient(config_file=KUBE_CONFIG_USER_PATH, context=clusters[0])

        secret_to_k8s_mapping = get_namespaces_for_secret(
            service, clusters[0], args.secret_name, args.yelpsoa_config_root
        )

        namespace = select_k8s_secret_namespace(secret_to_k8s_mapping)

        if namespace:
            print(
                get_secret(
                    kube_client,
                    get_paasta_secret_name(namespace, service, args.secret_name),
                    key_name=args.secret_name,
                    namespace=namespace,
                )
            )
        # fallback to default in case mapping fails
        else:
            print(
                get_secret(
                    kube_client,
                    get_paasta_secret_name("paasta", service, args.secret_name),
                    key_name=args.secret_name,
                    namespace="paasta",
                )
            )
        return

    if args.action in ["add", "update"]:
        plaintext = get_plaintext_input(args)
        if not plaintext:
            print("Warning: Given plaintext is an empty string.")
        secret_provider = _get_secret_provider_for_service(
            service,
            cluster_names=args.clusters,
            # this will only be invoked on a devbox
            # and only in a context where we certainly
            # want to use the working directory rather
            # than whatever the actual soa_dir path is
            # configured as
            soa_dir=os.getcwd(),
            secret_provider_extra_kwargs={
                "vault_token_file": args.vault_token_file,
                # best solution so far is to change the below string to "token",
                # so that token file is picked up from argparse
                "vault_auth_method": "ldap",  # must have LDAP to get 2FA push for prod
            },
        )
        secret_provider.write_secret(
            action=args.action,
            secret_name=args.secret_name,
            plaintext=plaintext,
            cross_environment_motivation=args.cross_env_motivation,
        )
        secret_path = os.path.join(
            secret_provider.secret_dir, f"{args.secret_name}.json"
        )
        _log_audit(
            action=f"{args.action}-secret",
            action_details={"secret_name": args.secret_name, "clusters": args.clusters},
            service=service,
        )

        print_paasta_helper(secret_path, args.secret_name, args.shared)
    elif args.action == "decrypt":
        secret_provider_extra_kwargs = {
            "vault_auth_method": args.vault_auth_method,
            "vault_token_file": args.vault_token_file,
        }
        secret_provider = _get_secret_provider_for_service(
            service,
            cluster_names=args.clusters,
            # `decrypt` does not require the current working directory
            # to be a writeable git checkout of yelpsoa-configs
            soa_dir=args.yelpsoa_config_root,
            secret_provider_extra_kwargs=secret_provider_extra_kwargs,
        )
        print(
            decrypt_secret(
                secret_provider=secret_provider, secret_name=args.secret_name
            ),
            end="",
        )
    elif args.action == "run":
        new_environ = os.environ.copy()

        system_paasta_config = load_system_paasta_config()
        secret_provider_kwargs = {
            "vault_cluster_config": system_paasta_config.get_vault_cluster_config(),
            "vault_auth_method": args.vault_auth_method,
            "vault_token_file": args.vault_token_file,
        }

        # This includes only the environment variables mapped to secrets,
        # other environment variables are not included.
        # All environment variables set in the current shell will also
        # be passed through.
        new_secret_vars = decrypt_secret_environment_variables(
            secret_provider_name=system_paasta_config.get_secret_provider_name(),
            environment=get_instance_config(
                service=args.service,
                instance=args.instance,
                cluster=args.clusters,
                soa_dir=args.yelpsoa_config_root,
            ).get_env(),
            soa_dir=args.yelpsoa_config_root,
            service_name=args.service,
            cluster_name=args.clusters,
            secret_provider_kwargs=secret_provider_kwargs,
        )

        for var_name, var_value in new_secret_vars.items():
            new_environ[var_name] = var_value

        if args.cmd == ["bash"]:
            # make it clear that we're running in a sub-shell
            new_environ["PS1"] = r"(paasta secret run) \$ "

        # This is like subprocess.run but never returns (it replaces the current process),
        # we do this to play nicely with pgctl:
        # https://pgctl.readthedocs.io/en/latest/user/quickstart.html#writing-playground-services
        os.execvpe(args.cmd[0], args.cmd, new_environ)
    else:
        print("Unknown action")
        sys.exit(1)


def decrypt_secret(secret_provider, secret_name):
    if len(secret_provider.cluster_names) > 1:
        print(
            "Can only decrypt for one cluster at a time!\nFor example, try '-c norcal-devc'"
            " to decrypt the secret for this service in norcal-devc."
        )
        sys.exit(1)

    return secret_provider.decrypt_secret(secret_name)
