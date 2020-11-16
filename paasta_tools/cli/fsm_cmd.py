#!/usr/bin/env python
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
import argparse
import contextlib
import shutil
import sys

from cookiecutter.main import cookiecutter

from paasta_tools.cli.fsm.autosuggest import suggest_smartstack_proxy_port
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors


@contextlib.contextmanager
def make_copyfile_symlink_aware():
    """The reasoning behind this monkeypatch is that cookiecutter doesn't
    respect symlinks at all, and at Yelp we use symlinks to reduce duplication
    in the soa configs. Maybe cookie-cutter will accept a symlink-aware PR?
    """
    orig_copyfile = shutil.copyfile
    orig_copymode = shutil.copymode

    def symlink_aware_copyfile(*args, **kwargs):
        kwargs.setdefault("follow_symlinks", False)
        orig_copyfile(*args, **kwargs)

    def symlink_aware_copymode(*args, **kwargs):
        kwargs.setdefault("follow_symlinks", False)
        orig_copymode(*args, **kwargs)

    shutil.copyfile = symlink_aware_copyfile
    shutil.copymode = symlink_aware_copymode
    try:
        yield
    finally:
        shutil.copyfile = orig_copyfile
        shutil.copymode = orig_copymode


def parse_args():
    fsm_parser = argparse.ArgumentParser(
        "fsm",
        description=(
            "'paasta fsm' is used to generate example soa-configs, which is useful during initial "
            "service creation. Currently 'fsm' generates 'yelp-specific' configuration, but can still "
            "be used as an example of a fully working PaaSTA service.\n\n"
            "After 'paasta fsm' is run, the operator should inspect the generated boilerplate configuration "
            "and adjust it to meet the particular needs of the new service."
        ),
    )
    fsm_parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        default=".",
        help=(
            "Path to root of yelpsoa-configs checkout\n"
            "Defaults to current working directory"
        ),
    )
    fsm_parser.set_defaults(command=paasta_fsm)
    return fsm_parser.parse_args()


def get_paasta_config(yelpsoa_config_root):
    variables = {"proxy_port": suggest_smartstack_proxy_port(yelpsoa_config_root)}
    return variables


def write_paasta_config(variables, template, destination):
    print("Using cookiecutter template from %s" % template)
    with make_copyfile_symlink_aware():
        cookiecutter(
            template=template,
            extra_context=variables,
            output_dir=destination,
            overwrite_if_exists=True,
            no_input=not sys.stdout.isatty(),
        )


def paasta_fsm(args):
    variables = get_paasta_config(yelpsoa_config_root=args.yelpsoa_config_root)
    destination = args.yelpsoa_config_root

    paasta_config = load_system_paasta_config()
    template = paasta_config.get_fsm_template()

    write_paasta_config(variables=variables, template=template, destination=destination)

    print(PaastaColors.yellow("               _  _(o)_(o)_  _"))
    print(PaastaColors.red(r"             ._\`:_ F S M _:' \_,"))
    print(PaastaColors.green(r"                 / (`---'\ `-."))
    print(PaastaColors.cyan("              ,-`  _)    (_,"))
    print("With My Noodly Appendage I Have Written Configs!")
    print()
    print("Customize Them If It Makes You Happy -- http://y/paasta For Details")
    print("Remember To Add, Commit, And Push When You're Done:")
    print()


def main():
    args = parse_args()
    paasta_fsm(args)


if __name__ == "__main__":
    main()
