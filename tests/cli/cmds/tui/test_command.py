import argparse

from paasta_tools.cli.cmds.tui import add_subparser
from paasta_tools.cli.cmds.tui import paasta_tui


def test_add_subparser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_subparser(subparsers)
    args = parser.parse_args(["tui"])
    assert args.command == paasta_tui


def test_add_subparser_with_cluster():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_subparser(subparsers)
    args = parser.parse_args(["tui", "--cluster", "prod"])
    assert args.cluster == "prod"
