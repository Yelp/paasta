from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer
from textual.widgets import RichLog
from textual.worker import get_current_worker

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher

if TYPE_CHECKING:
    from paasta_tools.cli.cmds.tui.app import PaastaApp


class LogsScreen(Screen):
    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("q", "quit", "Quit"),
        Binding("j", "scroll_down", "Down", show=False),
        Binding("k", "scroll_up", "Up", show=False),
        Binding("g", "scroll_top", "Top", show=False),
        Binding("G", "scroll_bottom", "Bottom", show=False, key_display="shift+g"),
        Binding("d", "scroll_half_down", show=False),
        Binding("u", "scroll_half_up", show=False),
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
        yield RichLog(highlight=False, markup=False, wrap=True, id="logs-output")
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
        log = self.query_one(RichLog)
        log.write(Text.from_ansi(line))

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
        if self._process:
            self._process.terminate()
        app: PaastaApp = self.app  # type: ignore[assignment]
        app.breadcrumb.pop()
        app.pop_screen()
