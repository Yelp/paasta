from __future__ import annotations

from typing import TYPE_CHECKING

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer
from textual.widgets import LoadingIndicator
from textual.worker import get_current_worker

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher
from paasta_tools.cli.cmds.tui.widgets.filterable_table import FilterableTable

if TYPE_CHECKING:
    from paasta_tools.cli.cmds.tui.app import PaastaApp


class ServicesScreen(Screen):
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("question_mark", "help", "Help"),
    ]

    def __init__(self, fetcher: PaastaDataFetcher, cluster: str) -> None:
        super().__init__()
        self._fetcher = fetcher
        self._cluster = cluster

    def compose(self) -> ComposeResult:
        yield LoadingIndicator()
        yield FilterableTable("Service", id="services-table")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(FilterableTable).display = False
        self.load_services()

    @work(exclusive=True, thread=True)
    def load_services(self) -> None:
        worker = get_current_worker()
        try:
            services = self._fetcher.get_services(self._cluster)
        except Exception as e:
            if not worker.is_cancelled:
                self.app.call_from_thread(self._show_error, str(e))
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._populate_table, services)

    def _populate_table(self, services: list) -> None:
        self.query_one(LoadingIndicator).display = False
        table = self.query_one(FilterableTable)
        table.display = True
        table.set_rows([(s.name,) for s in services])
        table.query_one("DataTable").focus()

    def _show_error(self, error: str) -> None:
        self.query_one(LoadingIndicator).display = False
        self.notify(f"Error loading services: {error}", severity="error")

    def action_go_back(self) -> None:
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.pop()
        app.pop_screen()

    def action_refresh(self) -> None:
        self.query_one(LoadingIndicator).display = True
        self.query_one(FilterableTable).display = False
        self.load_services()

    def action_help(self) -> None:
        self.notify(
            "Enter: select | /: filter | Esc: back | r: refresh | q: quit",
            timeout=5,
        )
