from __future__ import annotations

from typing import TYPE_CHECKING

from rich.table import Table
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer
from textual.widgets import LoadingIndicator
from textual.widgets import Static
from textual.worker import get_current_worker

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher
from paasta_tools.cli.cmds.tui.data.models import ClusterInfo
from paasta_tools.cli.cmds.tui.screens.instances import InstancesScreen
from paasta_tools.cli.cmds.tui.widgets.filterable_table import FilterableTable
from paasta_tools.monitoring_tools import get_runbook
from paasta_tools.monitoring_tools import get_team
from paasta_tools.monitoring_tools import monitoring_defaults
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url

if TYPE_CHECKING:
    from paasta_tools.cli.cmds.tui.app import PaastaApp


class ClusterScreen(Screen):
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("question_mark", "help", "Help"),
    ]

    DEFAULT_CSS = """
    #service-info-block {
        height: auto;
        margin: 1 0 1 0;
    }
    """

    def __init__(self, fetcher: PaastaDataFetcher, service: str) -> None:
        super().__init__()
        self._fetcher = fetcher
        self._service = service

    def compose(self) -> ComposeResult:
        yield Static("", id="service-info-block", markup=True)
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
            clusters = self._fetcher.get_clusters_for_service(self._service)
            header = self._build_header()
        except Exception as e:
            if not worker.is_cancelled:
                self.app.call_from_thread(self._show_error, str(e))
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._populate, clusters, header)

    def _build_header(self) -> Table:
        soa_dir = DEFAULT_SOA_DIR
        team = get_team(service=self._service, overrides={}, soa_dir=soa_dir)
        runbook = get_runbook(service=self._service, overrides={}, soa_dir=soa_dir)
        if runbook == monitoring_defaults("runbook"):
            runbook = ""
        git_repo = get_git_url(self._service, soa_dir)
        dashboard = f"http://y/{self._service}_load"
        table = Table(show_header=False, box=None, padding=(0, 2), expand=True)
        table.add_column(style="dim")
        table.add_column()
        table.add_column(style="dim")
        table.add_column()
        table.add_row(
            "Service:",
            f"[bold white]{self._service}[/bold white]",
            "Team:",
            f"[cyan]{team}[/cyan]",
        )
        table.add_row(
            "Runbook:",
            f"[magenta]{runbook or 'N/A'}[/magenta]",
            "Dashboard:",
            f"[green]{dashboard}[/green]",
        )
        table.add_row(
            "Repo:",
            f"[yellow]{git_repo}[/yellow]",
            "",
            "",
        )
        return table

    def _populate(self, clusters: list[ClusterInfo], header: Table) -> None:
        self.query_one(LoadingIndicator).display = False
        self.query_one("#service-info-block", Static).update(header)
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
        cluster_name = event.row_key
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.push(cluster_name)
        app.push_screen(InstancesScreen(self._fetcher, cluster_name, self._service))

    def action_go_back(self) -> None:
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.pop()
        app.pop_screen()

    def action_refresh(self) -> None:
        self.query_one(LoadingIndicator).display = True
        self.query_one(FilterableTable).display = False
        self.load_clusters()

    def action_help(self) -> None:
        self.notify(
            "Enter: select | /: filter | Esc: back | r: refresh | q: quit",
            timeout=5,
        )
