from textual.widgets import Static


class Breadcrumb(Static):
    DEFAULT_CSS = """
    Breadcrumb {
        dock: top;
        height: 1;
        background: $primary;
        color: $text;
        padding: 0 1;
    }
    """

    def __init__(self) -> None:
        super().__init__("")
        self._parts: list[str] = []

    def push(self, label: str) -> None:
        self._parts.append(label)
        self._render()

    def pop(self) -> None:
        if self._parts:
            self._parts.pop()
        self._render()

    def reset(self) -> None:
        self._parts.clear()
        self._render()

    def _render(self) -> None:
        if self._parts:
            self.update(" > ".join(self._parts))
        else:
            self.update("PaaSTA TUI")
