from textual.app import App
from textual.app import ComposeResult
from textual.binding import Binding

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher
from paasta_tools.cli.cmds.tui.screens.services import ServicesScreen
from paasta_tools.cli.cmds.tui.widgets.breadcrumb import Breadcrumb


class PaastaApp(App):
    TITLE = "PaaSTA TUI"
    theme = "textual-dark"
    CSS = """
    Screen {
        layout: vertical;
    }
    #filter-input {
        dock: top;
        height: 1;
        border: none;
        margin: 0;
        padding: 0 1;
    }
    #filter-status {
        dock: top;
        height: 1;
        color: $text-muted;
        padding: 0 1;
    }
    LoadingIndicator {
        height: 1fr;
    }
    FilterableTable {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self, fetcher: PaastaDataFetcher | None = None) -> None:
        super().__init__()
        self._fetcher = fetcher or PaastaDataFetcher()

    def compose(self) -> ComposeResult:
        yield Breadcrumb()

    @property
    def breadcrumb(self) -> Breadcrumb:
        return self.query_one(Breadcrumb)

    def on_mount(self) -> None:
        self.breadcrumb.push("Services")
        self.push_screen(ServicesScreen(self._fetcher))
