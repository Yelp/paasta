from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer
from textual.worker import get_current_worker

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher
from paasta_tools.cli.cmds.tui.widgets.searchable_log import SearchableLog

if TYPE_CHECKING:
    from paasta_tools.cli.cmds.tui.app import PaastaApp


class LogsScreen(Screen):
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(
        self,
        fetcher: PaastaDataFetcher,
        cluster: str,
        service: str,
        instance: str,
    ) -> None:
        super().__init__()
        self._fetcher = fetcher
        self._cluster = cluster
        self._service = service
        self._instance = instance
        self._process: subprocess.Popen | None = None

    def compose(self) -> ComposeResult:
        yield SearchableLog(id="logs-view")
        yield Footer()

    def on_mount(self) -> None:
        self.tail_logs()

    @work(exclusive=True, thread=True)
    def tail_logs(self) -> None:
        worker = get_current_worker()
        self._process = subprocess.Popen(
            [
                "paasta",
                "logs",
                "-s",
                self._service,
                "-c",
                self._cluster,
                "-i",
                self._instance,
                "-f",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            for line in self._process.stdout:
                if worker.is_cancelled:
                    break
                self.app.call_from_thread(self._append_line, line.rstrip("\n"))
        finally:
            self._process.terminate()
            self._process = None

    def _append_line(self, line: str) -> None:
        self.query_one(SearchableLog).write_line(Text.from_ansi(line))

    def action_go_back(self) -> None:
        if self._process:
            self._process.terminate()
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.pop()
        app.pop_screen()
