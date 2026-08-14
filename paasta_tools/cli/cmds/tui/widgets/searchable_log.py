import re

from rich.text import Text
from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Input
from textual.widgets import RichLog
from textual.widgets import Static


class SearchableLog(Vertical):
    BINDINGS = [
        Binding("slash", "open_search", "Search", show=True),
        Binding("escape", "close_search", "Clear", show=True),
        Binding("n", "next_match", "Next", show=True),
        Binding("N", "prev_match", "Prev", show=True, key_display="shift+n"),
        Binding("j", "scroll_down", show=False),
        Binding("k", "scroll_up", show=False),
        Binding("g", "scroll_top", show=False),
        Binding("G", "scroll_bottom", show=False, key_display="shift+g"),
        Binding("d", "scroll_half_down", show=False),
        Binding("u", "scroll_half_up", show=False),
    ]

    DEFAULT_CSS = """
    SearchableLog {
        height: 1fr;
    }
    #search-input {
        dock: bottom;
        height: 1;
        border: none;
        padding: 0 1;
    }
    #search-status {
        dock: bottom;
        height: 1;
        color: $text-muted;
        padding: 0 1;
    }
    #log-view {
        height: 1fr;
    }
    """

    MAX_LINES = 5000

    def __init__(self, id: str | None = None, max_lines: int | None = None) -> None:
        super().__init__(id=id)
        self._max_lines = max_lines or self.MAX_LINES
        self._lines: list[str] = []
        self._match_indices: list[int] = []
        self._current_match: int = -1
        self._searching = False
        self._active_query: str = ""

    def compose(self) -> ComposeResult:
        yield RichLog(highlight=False, markup=False, wrap=True, id="log-view")
        yield Static("", id="search-status")
        yield Input(placeholder="/search...", id="search-input")

    def on_mount(self) -> None:
        self.query_one("#search-input", Input).display = False
        self.query_one("#search-status", Static).display = False

    def write_line(self, text) -> None:
        line_str = str(text)
        self._lines.append(line_str)
        log = self.query_one(RichLog)
        if self._active_query:
            is_match = self._active_query.lower() in line_str.lower()
            if is_match:
                self._match_indices.append(len(self._lines) - 1)
                self._update_status()
            log.write(self._highlight_line(line_str, self._active_query))
        else:
            log.write(text)
        if len(self._lines) > self._max_lines + 500:
            self._lines = self._lines[-self._max_lines :]
            if self._active_query:
                self._find_matches(self._active_query)
            log.clear()
            for line in self._lines:
                if self._active_query:
                    log.write(self._highlight_line(line, self._active_query))
                else:
                    log.write(line)

    def _update_status(self) -> None:
        if not self._active_query:
            return
        status = self.query_one("#search-status", Static)
        if self._current_match >= 0:
            status.update(
                f"  Match {self._current_match + 1}/{len(self._match_indices)}"
                f" '{self._active_query}'"
            )
        else:
            status.update(
                f"  {len(self._match_indices)} matches for '{self._active_query}'"
            )
        status.display = True

    def clear(self) -> None:
        self._lines.clear()
        self._match_indices.clear()
        self._current_match = -1
        self.query_one(RichLog).clear()

    def scroll_to_end(self) -> None:
        self.query_one(RichLog).scroll_end(animate=False)

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool:
        if self._searching and action in (
            "scroll_down",
            "scroll_up",
            "scroll_top",
            "scroll_bottom",
            "scroll_half_down",
            "scroll_half_up",
            "next_match",
            "prev_match",
            "open_search",
        ):
            return False
        if not self._searching and not self._active_query and action == "close_search":
            return False
        return True

    def action_open_search(self) -> None:
        self._searching = True
        search_input = self.query_one("#search-input", Input)
        search_input.display = True
        search_input.focus()

    def action_close_search(self) -> None:
        if self._searching:
            self._searching = False
            self.query_one("#search-input", Input).display = False
        if self._active_query:
            self._active_query = ""
            self._match_indices.clear()
            self._current_match = -1
            self._render_lines(None)
        log = self.query_one(RichLog)
        log.auto_scroll = True
        self.query_one("#search-status", Static).display = False
        log.focus()

    @on(Input.Submitted, "#search-input")
    def on_search_submitted(self, event: Input.Submitted) -> None:
        self._searching = False
        query = event.value
        self.query_one("#search-input", Input).display = False
        log = self.query_one(RichLog)
        status = self.query_one("#search-status", Static)
        if query:
            self._active_query = query
            log.auto_scroll = False
            self._find_matches(query)
            self._render_lines(query)
            if self._match_indices:
                self._current_match = 0
                self._jump_to_match()
            else:
                status.update(f"  No matches for '{query}'")
                status.display = True
        else:
            status.display = False
        log.focus()

    @on(Input.Changed, "#search-input")
    def on_search_changed(self, event: Input.Changed) -> None:
        self._update_match_count(event.value)

    def _update_match_count(self, query: str) -> None:
        status = self.query_one("#search-status", Static)
        if not query:
            status.display = False
            return
        count = sum(1 for line in self._lines if query.lower() in line.lower())
        status.display = True
        status.update(
            f"  {count} matches for '{query}'"
            if count
            else f"  No matches for '{query}'"
        )

    def _find_matches(self, query: str) -> None:
        self._match_indices = [
            i for i, line in enumerate(self._lines) if query.lower() in line.lower()
        ]

    def _render_lines(self, query: str | None, current_line: int = -1) -> None:
        log = self.query_one(RichLog)
        log.clear()
        for i, line in enumerate(self._lines):
            if query:
                is_current = i == current_line
                log.write(self._highlight_line(line, query, is_current))
            else:
                log.write(line)

    def _highlight_line(self, line: str, query: str, is_current: bool = False) -> Text:
        text = Text(line)
        for match in re.finditer(re.escape(query), line, re.IGNORECASE):
            if is_current:
                text.stylize("bold black on yellow", match.start(), match.end())
            else:
                text.stylize("reverse", match.start(), match.end())
        return text

    def action_next_match(self) -> None:
        if not self._match_indices:
            return
        self._current_match = (self._current_match + 1) % len(self._match_indices)
        self._jump_to_match()

    def action_prev_match(self) -> None:
        if not self._match_indices:
            return
        self._current_match = (self._current_match - 1) % len(self._match_indices)
        self._jump_to_match()

    def _jump_to_match(self) -> None:
        line_idx = self._match_indices[self._current_match]
        self._render_lines(self._active_query, current_line=line_idx)
        log = self.query_one(RichLog)
        log.scroll_to(y=line_idx, animate=False)
        status = self.query_one("#search-status", Static)
        status.update(
            f"  Match {self._current_match + 1}/{len(self._match_indices)}"
            f" '{self._active_query}'"
        )
        status.display = True

    def action_scroll_down(self) -> None:
        self.query_one(RichLog).scroll_relative(y=1)

    def action_scroll_up(self) -> None:
        self.query_one(RichLog).scroll_relative(y=-1)

    def action_scroll_top(self) -> None:
        self.query_one(RichLog).scroll_home()

    def action_scroll_bottom(self) -> None:
        self.query_one(RichLog).scroll_end()

    def action_scroll_half_down(self) -> None:
        log = self.query_one(RichLog)
        log.scroll_relative(y=log.size.height // 2)

    def action_scroll_half_up(self) -> None:
        log = self.query_one(RichLog)
        log.scroll_relative(y=-(log.size.height // 2))
