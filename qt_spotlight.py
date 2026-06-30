"""Compact Qt spotlight search window."""

from __future__ import annotations

from PySide6.QtCore import QSize, Qt, Signal
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from spotlight_search import SpotlightResult, court_label


class SpotlightWindow(QDialog):
    search_requested = Signal(str)
    open_requested = Signal(object)
    full_search_requested = Signal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("GetCases Spotlight")
        self.setWindowFlag(Qt.WindowType.Window, True)
        self.resize(720, 520)
        self._results: list[SpotlightResult] = []

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
        self.query_edit.returnPressed.connect(self._submit_search)
        search_layout.addWidget(self.query_edit, 1)

        self.search_btn = QPushButton("Search")
        self.search_btn.clicked.connect(self._submit_search)
        search_layout.addWidget(self.search_btn)
        root.addWidget(search_frame)

        self.results_list = QListWidget()
        self.results_list.setAlternatingRowColors(False)
        self.results_list.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
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
            self.results_list.clear()

    def set_results(self, results: list[SpotlightResult]) -> None:
        self._results = results
        self.results_list.clear()
        for result in results:
            item = QListWidgetItem()
            item.setSizeHint(QSize(660, 68))
            self.results_list.addItem(item)
            self.results_list.setItemWidget(item, SpotlightResultRow(result))
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
            self.search_requested.emit(query)

    def _open_current(self) -> None:
        row = self.results_list.currentRow()
        if 0 <= row < len(self._results):
            result = self._results[row]
            self.hide()
            self.open_requested.emit(result)

    def _open_full_search(self) -> None:
        query = self.query_edit.text().strip()
        self.hide()
        self.full_search_requested.emit(query)

    def keyPressEvent(self, event) -> None:  # noqa: N802 - Qt override.
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            if self.results_list.hasFocus() and self.results_list.currentRow() >= 0:
                self._open_current()
                return
        super().keyPressEvent(event)


class SpotlightResultRow(QWidget):
    def __init__(self, result: SpotlightResult, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("SpotlightRow")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 7, 8, 7)
        layout.setSpacing(10)

        badge_text = result.source_label
        if result.court_id:
            badge_text = court_label(result.court_id) or badge_text
        badge = QLabel(badge_text[:12])
        badge.setObjectName("SourceBadge")
        badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
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
