import argparse

from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import list_clusters


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    tui_parser = subparsers.add_parser(
        "tui",
        help="Interactive terminal UI for PaaSTA (k9s-style)",
        description="Browse clusters, services, instances, and pods interactively.",
    )
    tui_parser.add_argument(
        "-c",
        "--cluster",
        help="Start directly in this cluster",
        default=None,
    ).completer = lazy_choices_completer(list_clusters)
    tui_parser.set_defaults(command=paasta_tui)


def paasta_tui(args: argparse.Namespace, **kwargs: object) -> int:
    try:
        from paasta_tools.cli.cmds.tui.app import PaastaApp
    except ImportError:
        print(
            "The TUI requires the 'textual' package.\n"
            "Install with: pip install 'paasta-tools[tui]'"
        )
        return 1
    app = PaastaApp()
    app.run()
    return 0
