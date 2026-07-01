"""Compact Qt spotlight search window."""

from __future__ import annotations

from PySide6.QtCore import QEvent, QSize, Qt, Signal
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from spotlight_search import SpotlightResult


class SpotlightWindow(QDialog):
    search_requested = Signal(str)
    open_requested = Signal(object)
    pdf_requested = Signal(object)
    full_search_requested = Signal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("GetCases Spotlight")
        self.setWindowFlag(Qt.WindowType.Window, True)
        self.resize(720, 520)
        self._results: list[SpotlightResult] = []
        self._last_query = ""

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 12)
        root.setSpacing(10)

        search_frame = QFrame(self)
        search_frame.setObjectName("Panel")
        search_layout = QHBoxLayout(search_frame)
        search_layout.setContentsMargins(10, 8, 10, 8)
        search_layout.setSpacing(8)

        self.query_edit = QLineEdit()
        self.query_edit.setPlaceholderText("Case, citation, statute, rule, or regulation")
        self.query_edit.installEventFilter(self)
        self.query_edit.returnPressed.connect(self._submit_search)
        search_layout.addWidget(self.query_edit, 1)

        self.search_btn = QPushButton("Search")
        self.search_btn.clicked.connect(self._submit_search)
        search_layout.addWidget(self.search_btn)
        root.addWidget(search_frame)

        self.results_list = QListWidget()
        self.results_list.setAlternatingRowColors(False)
        self.results_list.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        self.results_list.installEventFilter(self)
        self.results_list.itemDoubleClicked.connect(lambda _item: self._open_current())
        root.addWidget(self.results_list, 1)

        footer = QHBoxLayout()
        self.status_label = QLabel("Ready")
        self.status_label.setObjectName("MutedLabel")
        footer.addWidget(self.status_label, 1)
        self.full_search_btn = QPushButton("Full Search")
        self.full_search_btn.clicked.connect(self._open_full_search)
        footer.addWidget(self.full_search_btn)
        root.addLayout(footer)

    def focus_query(self, text: str = "") -> None:
        if text:
            self.query_edit.setText(text)
        self.query_edit.setFocus(Qt.FocusReason.ActiveWindowFocusReason)
        self.query_edit.selectAll()

    def set_busy(self, busy: bool, message: str = "Searching...") -> None:
        self.search_btn.setEnabled(not busy)
        self.status_label.setText(message if busy else "Ready")
        if busy:
            self._results = []
            self.results_list.clear()

    def set_results(self, results: list[SpotlightResult]) -> None:
        self._results = results
        self.results_list.clear()
        for result in results:
            item = QListWidgetItem()
            item.setSizeHint(QSize(660, 72))
            self.results_list.addItem(item)
            row = SpotlightResultRow(result)
            row.pdf_requested.connect(self._request_pdf)
            self.results_list.setItemWidget(item, row)
        if results:
            self.results_list.setCurrentRow(0)
            self.status_label.setText(f"{len(results)} result(s)")
        else:
            self.status_label.setText("No results")
        self.search_btn.setEnabled(True)

    def set_error(self, message: str) -> None:
        self.search_btn.setEnabled(True)
        self.status_label.setText(message)

    def _submit_search(self) -> None:
        query = self.query_edit.text().strip()
        if query:
            if self._results and query == self._last_query and self.results_list.currentRow() >= 0:
                self._open_current()
                return
            self._last_query = query
            self.search_requested.emit(query)

    def _open_current(self) -> None:
        row = self.results_list.currentRow()
        if 0 <= row < len(self._results):
            result = self._results[row]
            self.hide()
            self.open_requested.emit(result)

    def _request_pdf(self, result: SpotlightResult) -> None:
        self.hide()
        self.pdf_requested.emit(result)

    def _open_full_search(self) -> None:
        query = self.query_edit.text().strip()
        self.hide()
        self.full_search_requested.emit(query)

    def keyPressEvent(self, event) -> None:  # noqa: N802 - Qt override.
        if event.key() == Qt.Key.Key_Escape:
            self.hide()
            return
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            if self.results_list.hasFocus() and self.results_list.currentRow() >= 0:
                self._open_current()
                return
        super().keyPressEvent(event)

    def eventFilter(self, watched, event) -> bool:  # noqa: N802 - Qt override.
        if event.type() != QEvent.Type.KeyPress:
            return super().eventFilter(watched, event)
        key = event.key()
        if watched is self.query_edit and key in (Qt.Key.Key_Down, Qt.Key.Key_Up):
            if self.results_list.count():
                current = self.results_list.currentRow()
                if current < 0:
                    current = 0
                elif key == Qt.Key.Key_Down:
                    current = min(current + 1, self.results_list.count() - 1)
                else:
                    current = max(current - 1, 0)
                self.results_list.setCurrentRow(current)
                self.results_list.setFocus(Qt.FocusReason.ShortcutFocusReason)
                return True
        if watched is self.results_list and key == Qt.Key.Key_Escape:
            self.query_edit.setFocus(Qt.FocusReason.ShortcutFocusReason)
            return True
        return super().eventFilter(watched, event)


class SpotlightResultRow(QWidget):
    pdf_requested = Signal(object)

    def __init__(self, result: SpotlightResult, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("SpotlightRow")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 7, 8, 7)
        layout.setSpacing(10)

        badge_text = _source_badge(result)
        badge = QLabel(badge_text[:12])
        badge.setObjectName("SourceBadge")
        badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        badge.setToolTip(result.source_label)
        badge.setMinimumWidth(86)
        badge.setMaximumWidth(96)
        layout.addWidget(badge)

        text_col = QVBoxLayout()
        text_col.setContentsMargins(0, 0, 0, 0)
        text_col.setSpacing(2)

        title = QLabel(result.title or "(untitled)")
        title.setObjectName("SpotlightTitle")
        title.setToolTip(result.title or "")
        title.setWordWrap(False)
        text_col.addWidget(title)

        detail = result.detail or result.source_label
        sub = QLabel(detail)
        sub.setObjectName("SpotlightDetail")
        sub.setWordWrap(False)
        text_col.addWidget(sub)
        layout.addLayout(text_col, 1)

        if result.source == "courtlistener" and isinstance(result.payload, dict):
            pdf_btn = QToolButton()
            pdf_btn.setObjectName("SpotlightActionButton")
            pdf_btn.setText("PDF")
            pdf_btn.setToolTip("Open PDF")
            pdf_btn.clicked.connect(lambda: self.pdf_requested.emit(result))
            layout.addWidget(pdf_btn)


def _source_badge(result: SpotlightResult) -> str:
    return {
        "courtlistener": "CL",
        "cache": "Cache",
        "scholar": "Scholar",
        "engrep": "Eng. Rep.",
    }.get(result.source, result.source_label)
