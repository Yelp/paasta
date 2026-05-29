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


class ClusterScreen(Screen):
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
        yield FilterableTable("Cluster", "API Endpoint", id="cluster-table")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(FilterableTable).display = False
        self.load_clusters()

    @work(exclusive=True, thread=True)
    def load_clusters(self) -> None:
        worker = get_current_worker()
        try:
            clusters = self._fetcher.get_clusters()
        except Exception as e:
            if not worker.is_cancelled:
                self.app.call_from_thread(self._show_error, str(e))
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._populate_table, clusters)

    def _populate_table(self, clusters: list) -> None:
        self.query_one(LoadingIndicator).display = False
        table = self.query_one(FilterableTable)
        table.display = True
        table.set_rows([(c.name, c.api_endpoint) for c in clusters])
        table.query_one("DataTable").focus()

    def _show_error(self, error: str) -> None:
        self.query_one(LoadingIndicator).display = False
        self.notify(f"Error loading clusters: {error}", severity="error")

    def on_filterable_table_row_selected(
        self, event: FilterableTable.RowSelected
    ) -> None:
        from paasta_tools.cli.cmds.tui.screens.services import ServicesScreen

        cluster_name = event.row_key
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.push(cluster_name)
        app.push_screen(ServicesScreen(self._fetcher, cluster_name))

    def action_refresh(self) -> None:
        self.query_one(LoadingIndicator).display = True
        self.query_one(FilterableTable).display = False
        self.load_clusters()

    def action_help(self) -> None:
        self.notify(
            "Enter: select | /: filter | Esc: back | r: refresh | q: quit",
            timeout=5,
        )
