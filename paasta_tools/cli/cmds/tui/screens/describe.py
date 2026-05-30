from __future__ import annotations

from typing import TYPE_CHECKING

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer
from textual.widgets import LoadingIndicator
from textual.widgets import RichLog
from textual.worker import get_current_worker

from paasta_tools.cli.cmds.status import INSTANCE_TYPE_WRITERS
from paasta_tools.cli.cmds.status import find_instance_types
from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher

if TYPE_CHECKING:
    from paasta_tools.cli.cmds.tui.app import PaastaApp


class DescribeScreen(Screen):
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("j", "scroll_down", "Down", show=False),
        Binding("k", "scroll_up", "Up", show=False),
        Binding("g", "scroll_top", "Top", show=False),
        Binding("G", "scroll_bottom", "Bottom", show=False, key_display="shift+g"),
        Binding("d", "scroll_half_down", show=False),
        Binding("u", "scroll_half_up", show=False),
    ]

    def __init__(
        self, fetcher: PaastaDataFetcher, cluster: str, service: str, instance: str
    ) -> None:
        super().__init__()
        self._fetcher = fetcher
        self._cluster = cluster
        self._service = service
        self._instance = instance

    def compose(self) -> ComposeResult:
        yield LoadingIndicator()
        yield RichLog(highlight=False, markup=False, wrap=True, id="describe-log")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(RichLog).display = False
        self.load_describe()

    @work(exclusive=True, thread=True)
    def load_describe(self) -> None:
        worker = get_current_worker()
        try:
            output = self._fetch_status_output()
        except Exception as e:
            if not worker.is_cancelled:
                self.app.call_from_thread(self._show_error, str(e))
            return
        if not worker.is_cancelled:
            self.app.call_from_thread(self._show_output, output)

    def _fetch_status_output(self) -> list[str]:
        client = self._fetcher._get_client_for_cluster(self._cluster)
        status = client.service.status_instance(
            service=self._service,
            instance=self._instance,
            verbose=3,
            new=True,
        )
        output: list[str] = []
        output.append(f"{self._service}.{self._instance} in {self._cluster}")
        if status.get("version"):
            output.append(f"  Version:    {status.version} (desired)")
        elif status.get("git_sha"):
            output.append(f"  Git sha:    {status.git_sha} (desired)")

        instance_types = find_instance_types(status)
        for instance_type in instance_types:
            service_status_value = getattr(status, instance_type)
            writer_callable = INSTANCE_TYPE_WRITERS.get(instance_type)
            if writer_callable:
                writer_callable(
                    self._cluster,
                    self._service,
                    self._instance,
                    output,
                    service_status_value,
                    3,
                )
        return output

    def _show_output(self, output: list[str]) -> None:
        self.query_one(LoadingIndicator).display = False
        log = self.query_one(RichLog)
        log.display = True
        for line in output:
            log.write(Text.from_ansi(line))
        log.focus()

    def _show_error(self, error: str) -> None:
        self.query_one(LoadingIndicator).display = False
        log = self.query_one(RichLog)
        log.display = True
        log.write(Text.from_ansi(f"\033[31mError: {error}\033[0m"))

    def action_scroll_down(self) -> None:
        self.query_one(RichLog).scroll_relative(y=1)

    def action_scroll_up(self) -> None:
        self.query_one(RichLog).scroll_relative(y=-1)

    def action_scroll_top(self) -> None:
        self.query_one(RichLog).scroll_home()

    def action_scroll_bottom(self) -> None:
        self.query_one(RichLog).scroll_end()

    def action_scroll_half_down(self) -> None:
        self.query_one(RichLog).scroll_relative(
            y=self.query_one(RichLog).size.height // 2
        )

    def action_scroll_half_up(self) -> None:
        self.query_one(RichLog).scroll_relative(
            y=-(self.query_one(RichLog).size.height // 2)
        )

    def action_go_back(self) -> None:
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.pop()
        app.pop_screen()

    def action_refresh(self) -> None:
        self.query_one(LoadingIndicator).display = True
        log = self.query_one(RichLog)
        log.clear()
        log.display = False
        self.load_describe()
