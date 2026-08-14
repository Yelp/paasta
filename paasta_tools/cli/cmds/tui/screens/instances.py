from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable
from textual.widgets import Footer
from textual.widgets import LoadingIndicator
from textual.worker import get_current_worker

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher
from paasta_tools.cli.cmds.tui.data.models import InstanceInfo
from paasta_tools.cli.cmds.tui.screens.describe import DescribeScreen
from paasta_tools.cli.cmds.tui.screens.logs import LogsScreen
from paasta_tools.cli.cmds.tui.widgets.filterable_table import FilterableTable

if TYPE_CHECKING:
    from paasta_tools.cli.cmds.tui.app import PaastaApp


STATE_COLORS = {
    "Running": "green",
    "Bouncing": "yellow",
    "Deploying": "yellow",
    "Starting": "yellow",
    "Launching replicas": "yellow",
    "Stopping": "red",
    "Stopped": "red",
    "Unknown": "red",
}


class InstancesScreen(Screen):
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("d", "describe", "Describe"),
        Binding("l", "logs", "Logs"),
        Binding("question_mark", "help", "Help"),
    ]

    def __init__(self, fetcher: PaastaDataFetcher, cluster: str, service: str) -> None:
        super().__init__()
        self._fetcher = fetcher
        self._cluster = cluster
        self._service = service

    def compose(self) -> ComposeResult:
        yield LoadingIndicator()
        yield FilterableTable(
            "Instance",
            "Type",
            "State",
            "Ready",
            "Versions",
            "Git SHA",
            "Error",
            id="instances-table",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(FilterableTable).display = False
        self.load_instance_names()

    @work(exclusive=True, thread=True)
    def load_instance_names(self) -> None:
        worker = get_current_worker()
        try:
            names = self._fetcher.list_instance_names(self._cluster, self._service)
        except Exception as e:
            if not worker.is_cancelled:
                self.app.call_from_thread(self._show_error, str(e))
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._show_names, names)
            self._load_statuses(names, worker)

    def _show_names(self, names: list[str]) -> None:
        self.query_one(LoadingIndicator).display = False
        table = self.query_one(FilterableTable)
        table.display = True
        rows: list[tuple[str, ...]] = [
            (name, "", "...", "", "", "", "") for name in names
        ]
        table.set_rows(rows)
        table.query_one("DataTable").focus()

    def _load_statuses(self, names: list[str], worker) -> None:
        client = self._fetcher._get_client_for_cluster(self._cluster)
        if client is None:
            return
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {
                name: pool.submit(
                    self._fetcher._get_instance_info, client, self._service, name
                )
                for name in names
            }
            for name in names:
                if worker.is_cancelled:
                    return
                result = futures[name].result()
                if result is not None and not worker.is_cancelled:
                    self.app.call_from_thread(self._update_row, result)

    def _update_row(self, inst: InstanceInfo) -> None:
        table = self.query_one(FilterableTable)
        color = STATE_COLORS.get(inst.state, "white")
        state_display = f"[{color}]{inst.state}[/{color}]"
        ready_display = f"{inst.ready}/{inst.desired}"
        versions_display = str(inst.num_versions)
        error_display = f"[red]{inst.error}[/red]" if inst.error else ""
        new_row = (
            inst.name,
            inst.instance_type,
            state_display,
            ready_display,
            versions_display,
            inst.git_sha,
            error_display,
        )
        table.update_row(inst.name, new_row)

    def _show_error(self, error: str) -> None:
        self.query_one(LoadingIndicator).display = False
        self.notify(f"Error loading instances: {error}", severity="error")

    def action_go_back(self) -> None:
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.pop()
        app.pop_screen()

    def action_refresh(self) -> None:
        self.query_one(LoadingIndicator).display = True
        self.query_one(FilterableTable).display = False
        self.load_instance_names()

    def on_filterable_table_row_selected(
        self, event: FilterableTable.RowSelected
    ) -> None:
        self._open_describe(event.row_key)

    def action_describe(self) -> None:
        table = self.query_one(FilterableTable)
        dt = table.query_one(DataTable)
        if dt.row_count == 0:
            return
        row_key = dt.coordinate_to_cell_key(dt.cursor_coordinate).row_key
        self._open_describe(str(row_key.value))

    def _open_describe(self, instance_name: str) -> None:
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.push(instance_name)
        app.push_screen(
            DescribeScreen(self._fetcher, self._cluster, self._service, instance_name)
        )

    def action_logs(self) -> None:
        table = self.query_one(FilterableTable)
        dt = table.query_one(DataTable)
        if dt.row_count == 0:
            return
        row_key = dt.coordinate_to_cell_key(dt.cursor_coordinate).row_key
        instance_name = str(row_key.value)
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.push(f"{instance_name} [logs]")
        app.push_screen(
            LogsScreen(self._fetcher, self._cluster, self._service, instance_name)
        )

    def action_help(self) -> None:
        self.notify(
            "d: describe | l: logs | /: filter | Esc: back | r: refresh | q: quit",
            timeout=5,
        )
