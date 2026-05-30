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
from paasta_tools.cli.cmds.tui.screens.clusters import ClusterScreen
from paasta_tools.cli.cmds.tui.widgets.filterable_table import FilterableTable

if TYPE_CHECKING:
    from paasta_tools.cli.cmds.tui.app import PaastaApp


class ServicesScreen(Screen):
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("question_mark", "help", "Help"),
    ]

    def __init__(self, fetcher: PaastaDataFetcher) -> None:
        super().__init__()
        self._fetcher = fetcher

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
            services = self._fetcher.get_all_services()
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

    def on_filterable_table_row_selected(
        self, event: FilterableTable.RowSelected
    ) -> None:
        service_name = event.row_key
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.push(service_name)
        app.push_screen(ClusterScreen(self._fetcher, service_name))

    def action_refresh(self) -> None:
        self.query_one(LoadingIndicator).display = True
        self.query_one(FilterableTable).display = False
        self.load_services()

    def action_help(self) -> None:
        self.notify(
            "Enter: select | /: filter | r: refresh | q: quit",
            timeout=5,
        )
