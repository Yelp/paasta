from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import DataTable
from textual.widgets import Input
from textual.widgets import Static


class FilterableTable(Vertical):
    BINDINGS = [
        Binding("slash", "open_filter", "Filter", show=True),
        Binding("escape", "close_filter", "Back", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("g", "cursor_top", "Top", show=False),
        Binding("G", "cursor_bottom", "Bottom", show=False, key_display="shift+g"),
    ]

    class RowSelected(Message):
        def __init__(self, row_key: str, row_data: tuple[str, ...]) -> None:
            self.row_key = row_key
            self.row_data = row_data
            super().__init__()

    def __init__(
        self,
        *columns: str,
        id: str | None = None,
    ) -> None:
        super().__init__(id=id)
        self._columns = columns
        self._all_rows: list[tuple[str, ...]] = []
        self._filtering = False

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool:
        if self._filtering and action in (
            "cursor_down",
            "cursor_up",
            "cursor_top",
            "cursor_bottom",
            "open_filter",
        ):
            return False
        return True

    def compose(self) -> ComposeResult:
        yield Static("", id="filter-status")
        yield Input(placeholder="Type to filter...", id="filter-input")
        yield DataTable(cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns(*self._columns)
        filter_input = self.query_one("#filter-input", Input)
        filter_input.display = False
        self.query_one("#filter-status", Static).display = False

    def set_rows(self, rows: list[tuple[str, ...]]) -> None:
        self._all_rows = rows
        self._apply_filter()

    def _apply_filter(self) -> None:
        table = self.query_one(DataTable)
        filter_input = self.query_one("#filter-input", Input)
        filter_text = filter_input.value.lower()
        table.clear()
        for row in self._all_rows:
            if not filter_text or any(filter_text in cell.lower() for cell in row):
                table.add_row(*row, key=row[0])
        status = self.query_one("#filter-status", Static)
        if filter_text:
            status.display = True
            status.update(f"Filter: {filter_input.value} ({table.row_count} results)")
        else:
            status.display = False

    def action_cursor_down(self) -> None:
        table = self.query_one(DataTable)
        table.action_cursor_down()

    def action_cursor_up(self) -> None:
        table = self.query_one(DataTable)
        table.action_cursor_up()

    def action_cursor_top(self) -> None:
        table = self.query_one(DataTable)
        table.move_cursor(row=0)

    def action_cursor_bottom(self) -> None:
        table = self.query_one(DataTable)
        table.move_cursor(row=table.row_count - 1)

    def action_open_filter(self) -> None:
        self._filtering = True
        filter_input = self.query_one("#filter-input", Input)
        filter_input.display = True
        filter_input.focus()

    def action_close_filter(self) -> None:
        filter_input = self.query_one("#filter-input", Input)
        if self._filtering:
            self._filtering = False
            filter_input.value = ""
            filter_input.display = False
            self.query_one("#filter-status", Static).display = False
            self._apply_filter()
            self.query_one(DataTable).focus()
        else:
            self.screen.dismiss()

    @on(Input.Submitted, "#filter-input")
    def on_filter_submitted(self, event: Input.Submitted) -> None:
        self._filtering = False
        self.query_one(DataTable).focus()

    @on(Input.Changed, "#filter-input")
    def on_filter_changed(self, event: Input.Changed) -> None:
        self._apply_filter()

    @on(DataTable.RowSelected)
    def on_row_selected(self, event: DataTable.RowSelected) -> None:
        event.stop()
        row_key = str(event.row_key.value)
        row_data = tuple(str(cell) for cell in self._all_rows if cell[0] == row_key)
        if row_data:
            self.post_message(self.RowSelected(row_key, row_data))
