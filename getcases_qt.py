"""PySide6/QtWebEngine front end for GetCases.

This is the migration entry point for the Qt version of the app.  The legacy
Tkinter UI remains in ``courtlistener_gui.py`` while feature parity is brought
over incrementally.
"""

from __future__ import annotations

import html
import json
import re
import sys
import traceback
import urllib.parse
from pathlib import Path
from typing import Optional

try:
    from PySide6.QtCore import QObject, Qt, QThreadPool, QTimer, QUrl, Signal
    from PySide6.QtGui import QAction, QDesktopServices
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWidgets import (
        QApplication,
        QDialog,
        QFileDialog,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QInputDialog,
        QMainWindow,
        QMessageBox,
        QPushButton,
        QSpinBox,
        QSplitter,
        QStatusBar,
        QTableWidget,
        QTableWidgetItem,
        QTextEdit,
        QToolBar,
        QVBoxLayout,
        QWidget,
    )
except ImportError as exc:  # pragma: no cover - user-facing startup check.
    raise SystemExit(
        "The Qt version of GetCases needs PySide6 with QtWebEngine.\n"
        "Install it with: pip install PySide6"
    ) from exc

import brief_reader
import eng_rep
import oyez
from case_utils import (
    build_default_filename,
    case_name,
    citation_list,
    federal_appendix_cite,
    format_case_row,
    is_federal_appendix_cite,
    is_scotus_order,
    normalize_result_citations,
    parse_citation_line,
    pick_citation,
    preview_from_item,
    static_case_law_url,
    strip_html,
)
from citations import detect_links
from courtlistener import CourtListenerClient, CourtListenerError
from courtlistener_text import CourtListenerOpinion, assemble_case_parts
from getcases_config import load_token, save_token
from opinion_db import OpinionDB
from pdf_resolver import fetch_pdf_bytes, resolve_pdf_url
from qt_courts import CourtPickerDialog, courts_summary
from qt_opinions import (
    render_opinion_parts_body,
    render_oyez_case_details,
    render_scholar_opinion_body,
)
from qt_pdf import ChromiumPdfWindow, LinkHandlingPage, html_document
from qt_spotlight import SpotlightWindow
from qt_sources import (
    english_reports_url,
    load_source,
    parse_lookup,
    source_body,
    source_title,
)
from qt_workers import Worker
from spotlight_search import (
    SpotlightResult,
    courtlistener_spotlight_results,
    name_match_score,
    spotlight_search,
)

try:
    from google_scholar import GoogleScholarFetcher
except Exception:
    GoogleScholarFetcher = None  # type: ignore[assignment]


APP_STYLE = """
QMainWindow, QWidget {
  background: #f4f6f8;
  color: #1f2933;
  font-family: "Segoe UI";
  font-size: 10pt;
}
QFrame#Panel {
  background: #ffffff;
  border: 1px solid #dde3ea;
  border-radius: 8px;
}
QLabel#SectionTitle {
  color: #334155;
  font-size: 11pt;
  font-weight: 600;
}
QLabel#MutedLabel {
  color: #66788a;
}
QLineEdit, QSpinBox, QTextEdit, QTableWidget {
  background: #ffffff;
  border: 1px solid #cfd8e3;
  border-radius: 6px;
  padding: 5px;
  selection-background-color: #cde9ff;
}
QPushButton {
  background: #243b53;
  color: white;
  border: 0;
  border-radius: 6px;
  padding: 7px 12px;
}
QPushButton:hover { background: #334e68; }
QPushButton:disabled { background: #a8b2bd; }
QPushButton#FilterButton {
  background: #ffffff;
  color: #243b53;
  border: 1px solid #cfd8e3;
  padding: 5px 10px;
  text-align: left;
}
QPushButton#FilterButton:hover {
  background: #eef6ff;
  border-color: #9fc6e8;
}
QHeaderView::section {
  background: #e8edf3;
  border: 0;
  border-right: 1px solid #d6dee8;
  padding: 6px;
  font-weight: 600;
}
QTableWidget {
  gridline-color: #edf1f5;
  alternate-background-color: #f8fafc;
}
QListWidget {
  background: #ffffff;
  border: 1px solid #dde3ea;
  border-radius: 8px;
  padding: 4px;
}
QListWidget::item {
  border-radius: 6px;
  margin: 2px;
}
QListWidget::item:selected {
  background: #d9ebff;
}
QLabel#SourceBadge {
  background: #2f5f8f;
  color: #ffffff;
  border-radius: 5px;
  padding: 4px 6px;
  font-weight: 600;
}
QLabel#SpotlightTitle {
  color: #1f2933;
  font-size: 10.5pt;
  font-weight: 600;
}
QLabel#SpotlightDetail {
  color: #66788a;
  font-size: 9pt;
}
QStatusBar {
  background: #ffffff;
  border-top: 1px solid #dde3ea;
}
QToolButton#SpotlightActionButton {
  background: #ffffff;
  color: #243b53;
  border: 1px solid #cfd8e3;
  border-radius: 5px;
  padding: 5px 8px;
}
QToolButton#SpotlightActionButton:hover {
  background: #eef6ff;
  border-color: #9fc6e8;
}
"""


class ResultTable(QTableWidget):
    """Table that stores each row's backing object on the first cell."""

    def __init__(self, headers: list[str], parent=None) -> None:
        super().__init__(0, len(headers), parent)
        self.setHorizontalHeaderLabels(headers)
        self.setAlternatingRowColors(True)
        self.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)

    def set_rows(self, rows: list[tuple[list[str], object]]) -> None:
        self.setSortingEnabled(False)
        self.setRowCount(0)
        for values, payload in rows:
            row = self.rowCount()
            self.insertRow(row)
            for col, value in enumerate(values):
                cell = QTableWidgetItem(value)
                if col == 0:
                    cell.setData(Qt.ItemDataRole.UserRole, payload)
                self.setItem(row, col, cell)
        self.resizeRowsToContents()
        self.setSortingEnabled(True)

    def current_payload(self):
        row = self.currentRow()
        if row < 0:
            return None
        cell = self.item(row, 0)
        return cell.data(Qt.ItemDataRole.UserRole) if cell is not None else None


class HtmlWindow(QMainWindow):
    """Simple styled WebEngine document window with optional link callback."""

    def __init__(
        self,
        title: str,
        body: str,
        *,
        base_url: str = "",
        link_callback=None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        self.setWindowFlag(Qt.WindowType.Window, True)
        self.setWindowTitle(title)
        self.resize(1000, 820)
        self.view = QWebEngineView(self)
        page = LinkHandlingPage(self.view)
        if link_callback is not None:
            page.link_activated.connect(link_callback)
        else:
            page.link_activated.connect(lambda url: QDesktopServices.openUrl(QUrl(url)))
        self.view.setPage(page)
        self.view.setHtml(html_document(title, body, base_url), QUrl(base_url or "about:blank"))
        self.setCentralWidget(self.view)

    def run_javascript_soon(self, js: str, delay_ms: int = 150) -> None:
        def run() -> None:
            try:
                self.view.page().runJavaScript(js)
            except RuntimeError:
                pass

        self.view.loadFinished.connect(lambda _ok: QTimer.singleShot(delay_ms, run))
        QTimer.singleShot(delay_ms + 200, run)

    def insert_top_fragment(self, fragment: str) -> None:
        if not fragment:
            return
        js = f"""
(() => {{
  const template = document.createElement("template");
  template.innerHTML = {json.dumps(fragment)};
  const node = template.content.firstElementChild;
  if (!node) return false;
  const existing = document.getElementById(node.id);
  if (existing) {{
    existing.replaceWith(node);
    return true;
  }}
  const anchor = document.querySelector(".opinion-meta") || document.querySelector("h1");
  if (anchor) {{
    anchor.insertAdjacentElement("afterend", node);
    return true;
  }}
  const main = document.querySelector("main") || document.body;
  main.prepend(node);
  return true;
}})();
"""
        self.run_javascript_soon(js)

    def scroll_to_page(self, pin: str) -> None:
        page = "".join(ch for ch in str(pin or "") if ch.isalnum())
        if not page:
            return
        anchor = "page-" + page
        js = f"""
(() => {{
  const el = document.getElementById({anchor!r});
  if (!el) return false;
  el.scrollIntoView({{block: "center", behavior: "smooth"}});
  el.style.background = "rgba(73, 168, 255, .22)";
  el.style.borderRadius = "4px";
  return true;
}})();
"""
        self.run_javascript_soon(js)


class BriefWindow(HtmlWindow):
    def __init__(
        self,
        title: str,
        text: str,
        link_callback,
        *,
        source_path: str = "",
        open_original_callback=None,
        parent=None,
    ) -> None:
        super().__init__(
            title,
            _brief_body(title, text, source_path),
            link_callback=link_callback,
            parent=parent,
        )
        if source_path and open_original_callback is not None:
            toolbar = QToolBar("Brief", self)
            toolbar.setMovable(False)
            self.addToolBar(toolbar)
            action = QAction("Original PDF", self)
            action.triggered.connect(open_original_callback)
            toolbar.addAction(action)


def _brief_body(title: str, text: str, source_path: str = "") -> str:
    links = detect_links(text)
    source_name = Path(source_path).name if source_path else ""
    count_text = f"{len(links)} citation link" + ("" if len(links) == 1 else "s")
    parts: list[str] = [
        f"<h1>{html.escape(title or 'Brief')}</h1>",
        '<div class="opinion-meta">',
        f"<span>{html.escape(count_text)}</span>",
    ]
    if source_name:
        parts.append(f"<span>{html.escape(source_name)}</span>")
    parts.extend([
        "</div>",
        "<pre>",
    ])
    pos = 0
    for start, end, action in links:
        if start < pos:
            continue
        parts.append(html.escape(text[pos:start]))
        kind, value = action
        href = "getcases://open?" + urllib.parse.urlencode({"kind": kind, "value": value})
        label = html.escape(text[start:end])
        parts.append(f'<a class="cite cite-{html.escape(kind)}" href="{href}">{label}</a>')
        pos = end
    parts.append(html.escape(text[pos:]))
    parts.append("</pre>")
    return "".join(parts)


class HotkeyBridge(QObject):
    activated = Signal()


class CitationListDialog(QDialog):
    open_requested = Signal(list)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Open Citation List")
        self.resize(620, 480)

        layout = QVBoxLayout(self)
        intro = QLabel("One citation per line. Case name is optional.")
        intro.setObjectName("SectionTitle")
        layout.addWidget(intro)

        hint = QLabel("Example: Monroe v. Pape, 365 U.S. 167, 171 (1961)")
        hint.setObjectName("MutedLabel")
        layout.addWidget(hint)

        self.text_edit = QTextEdit()
        self.text_edit.setAcceptRichText(False)
        layout.addWidget(self.text_edit, 1)

        self.failure_box = QTextEdit()
        self.failure_box.setReadOnly(True)
        self.failure_box.setMaximumHeight(110)
        self.failure_box.hide()
        layout.addWidget(self.failure_box)

        row = QHBoxLayout()
        self.open_btn = QPushButton("Open All")
        self.open_btn.clicked.connect(self._request_open)
        row.addWidget(self.open_btn)
        self.status_label = QLabel("Ready")
        self.status_label.setObjectName("MutedLabel")
        row.addWidget(self.status_label, 1)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        row.addWidget(close_btn)
        layout.addLayout(row)

    def lines(self) -> list[str]:
        return [line.strip() for line in self.text_edit.toPlainText().splitlines() if line.strip()]

    def set_status(self, text: str) -> None:
        self.status_label.setText(text)

    def set_running(self, running: bool) -> None:
        self.open_btn.setEnabled(not running)
        if running:
            self.failure_box.clear()
            self.failure_box.hide()

    def set_finished(self, opened: int, total: int, failures: list[str]) -> None:
        self.set_running(False)
        if failures:
            self.status_label.setText(f"Opened {opened} of {total}; {len(failures)} not found.")
            self.failure_box.setPlainText("\n".join(failures))
            self.failure_box.show()
        else:
            self.status_label.setText(f"Opened all {opened} citation(s).")

    def _request_open(self) -> None:
        lines = self.lines()
        if not lines:
            self.set_status("Nothing to open.")
            return
        self.open_requested.emit(lines)


class GetCasesQt(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("GetCases")
        self.resize(1360, 820)
        self.pool = QThreadPool.globalInstance()
        self._client: Optional[CourtListenerClient] = None
        self._scholar = None
        self._pending_jobs = 0
        self._workers: list[Worker] = []
        self._court_results: list[dict] = []
        self._selected_courts: set[str] = set()
        self._opinion_db: Optional[OpinionDB] = None
        self._opinion_db_failed = False
        self._windows: list[QMainWindow] = []
        self._spotlight: Optional[SpotlightWindow] = None
        self._spotlight_generation = 0
        self._citation_list_dialog: Optional[CitationListDialog] = None
        self._hotkey_listener = None
        self._hotkey_bridge = HotkeyBridge(self)
        self._hotkey_bridge.activated.connect(self.show_spotlight)

        self._build_ui()
        self._wire_actions()
        self._install_global_hotkey()
        self.statusBar().showMessage("Ready.")

    def _build_ui(self) -> None:
        root = QWidget()
        layout = QVBoxLayout(root)
        layout.setContentsMargins(12, 10, 12, 8)
        layout.setSpacing(10)
        self.setCentralWidget(root)

        self._build_toolbar()

        search_panel = QFrame()
        search_panel.setObjectName("Panel")
        search_layout = QGridLayout(search_panel)
        search_layout.setContentsMargins(12, 10, 12, 10)
        search_layout.setHorizontalSpacing(8)
        search_layout.setVerticalSpacing(8)

        title = QLabel("Search")
        title.setObjectName("SectionTitle")
        search_layout.addWidget(title, 0, 0)

        self.query_edit = QLineEdit()
        self.query_edit.setPlaceholderText("Case name, citation, statute, or search terms")
        search_layout.addWidget(self.query_edit, 0, 1, 1, 6)

        self.search_btn = QPushButton("Search")
        search_layout.addWidget(self.search_btn, 0, 7)

        self.token_edit = QLineEdit(load_token())
        self.token_edit.setPlaceholderText("CourtListener API token")
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        search_layout.addWidget(QLabel("Token"), 1, 0)
        search_layout.addWidget(self.token_edit, 1, 1, 1, 2)

        self.courts_btn = QPushButton(courts_summary(self._selected_courts))
        self.courts_btn.setObjectName("FilterButton")
        self.courts_btn.setMinimumWidth(160)
        self.courts_btn.setToolTip("All courts")
        search_layout.addWidget(QLabel("Courts"), 1, 3)
        search_layout.addWidget(self.courts_btn, 1, 4)

        self.from_edit = QLineEdit()
        self.from_edit.setPlaceholderText("YYYY-MM-DD")
        self.to_edit = QLineEdit()
        self.to_edit.setPlaceholderText("YYYY-MM-DD")
        search_layout.addWidget(QLabel("Filed"), 1, 5)
        search_layout.addWidget(self.from_edit, 1, 6)
        search_layout.addWidget(self.to_edit, 1, 7)

        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(5, 20)
        self.limit_spin.setValue(20)
        search_layout.addWidget(QLabel("Max"), 1, 8)
        search_layout.addWidget(self.limit_spin, 1, 9)
        layout.addWidget(search_panel)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(splitter, 1)

        left_panel = self._panel("CourtListener")
        left_layout = left_panel.layout()
        self.court_table = ResultTable(
            ["Case", "Court", "Filed", "Citation", "Status"], self
        )
        self.court_table.setColumnWidth(0, 360)
        self.court_table.setColumnWidth(1, 90)
        self.court_table.setColumnWidth(2, 100)
        self.court_table.setColumnWidth(3, 170)
        left_layout.addWidget(self.court_table)

        action_row = QHBoxLayout()
        self.open_text_btn = QPushButton("Open Text")
        self.view_pdf_btn = QPushButton("View PDF")
        self.download_pdf_btn = QPushButton("Download PDF")
        action_row.addWidget(self.open_text_btn)
        action_row.addWidget(self.view_pdf_btn)
        action_row.addWidget(self.download_pdf_btn)
        action_row.addStretch(1)
        left_layout.addLayout(action_row)
        splitter.addWidget(left_panel)

        right_panel = self._panel("Google Scholar")
        right_layout = right_panel.layout()
        self.scholar_table = ResultTable(["Case", "Source"], self)
        self.scholar_table.setColumnWidth(0, 330)
        self.scholar_table.setColumnWidth(1, 210)
        right_layout.addWidget(self.scholar_table)
        scholar_row = QHBoxLayout()
        self.open_scholar_btn = QPushButton("Open Scholar Text")
        scholar_row.addWidget(self.open_scholar_btn)
        scholar_row.addStretch(1)
        right_layout.addLayout(scholar_row)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)

        preview_panel = self._panel("Preview")
        self.preview = QTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setMaximumHeight(120)
        preview_panel.layout().addWidget(self.preview)
        layout.addWidget(preview_panel)

        self.setStatusBar(QStatusBar(self))
        self._set_result_actions(False)

    def _build_toolbar(self) -> None:
        toolbar = QToolBar("Main", self)
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        self.spotlight_action = QAction("Spotlight", self)
        self.spotlight_action.setShortcut("Ctrl+Space")
        toolbar.addAction(self.spotlight_action)

        self.open_brief_action = QAction("Open Brief", self)
        toolbar.addAction(self.open_brief_action)

        self.open_citation_list_action = QAction("Open Citation List", self)
        toolbar.addAction(self.open_citation_list_action)

        self.quick_lookup_action = QAction("Quick Lookup", self)
        toolbar.addAction(self.quick_lookup_action)

        self.save_token_action = QAction("Save Token", self)
        toolbar.addAction(self.save_token_action)

        self.legacy_action = QAction("Legacy Tk App", self)
        toolbar.addAction(self.legacy_action)

    def _panel(self, title: str) -> QFrame:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(10, 9, 10, 10)
        label = QLabel(title)
        label.setObjectName("SectionTitle")
        layout.addWidget(label)
        return panel

    def _wire_actions(self) -> None:
        self.spotlight_action.triggered.connect(self.show_spotlight)
        self.search_btn.clicked.connect(self.search)
        self.query_edit.returnPressed.connect(self.search)
        self.courts_btn.clicked.connect(self.show_court_picker)
        self.save_token_action.triggered.connect(self._save_token)
        self.open_brief_action.triggered.connect(self.open_brief)
        self.open_citation_list_action.triggered.connect(self.open_citation_list)
        self.quick_lookup_action.triggered.connect(self.quick_lookup)
        self.legacy_action.triggered.connect(self._show_legacy_hint)
        self.court_table.itemSelectionChanged.connect(self._on_court_selection)
        self.scholar_table.itemSelectionChanged.connect(self._on_scholar_selection)
        self.court_table.cellDoubleClicked.connect(lambda _r, _c: self.open_selected_text())
        self.scholar_table.cellDoubleClicked.connect(lambda _r, _c: self.open_selected_scholar())
        self.open_text_btn.clicked.connect(self.open_selected_text)
        self.view_pdf_btn.clicked.connect(self.view_selected_pdf)
        self.download_pdf_btn.clicked.connect(self.download_selected_pdf)
        self.open_scholar_btn.clicked.connect(self.open_selected_scholar)

    def _save_token(self) -> None:
        save_token(self.token_edit.text())
        self.statusBar().showMessage("CourtListener token saved.", 3500)

    def show_spotlight(self, text: str = "") -> None:
        if self._spotlight is None:
            self._spotlight = SpotlightWindow(self)
            self._spotlight.search_requested.connect(self._run_spotlight_search)
            self._spotlight.open_requested.connect(self._open_spotlight_result)
            self._spotlight.pdf_requested.connect(self._open_spotlight_pdf_result)
            self._spotlight.full_search_requested.connect(self._open_full_search_from_spotlight)
            self._spotlight.destroyed.connect(lambda: setattr(self, "_spotlight", None))
        self._spotlight.show()
        self._spotlight.raise_()
        self._spotlight.activateWindow()
        self._spotlight.focus_query(text)

    def _run_spotlight_search(self, query: str) -> None:
        self._spotlight_generation += 1
        generation = self._spotlight_generation
        action = self._spotlight_direct_action(query)
        if action is not None:
            if self._spotlight is not None:
                self._spotlight.hide()
            self._handle_action(action)
            return
        if self._spotlight is not None:
            self._spotlight.set_busy(True)
        client = self._client_for_token(required=False)
        fetcher = self._scholar_fetcher()
        db = self._opinion_database()

        def task(status):
            status("Searching spotlight...")
            return generation, query, spotlight_search(query, client=client, fetcher=fetcher, db=db)

        def done(payload) -> None:
            completed_generation, completed_query, results = payload
            if completed_generation != self._spotlight_generation:
                return
            if self._spotlight is not None:
                self._spotlight.set_results(results)
            self.statusBar().showMessage(
                f"Spotlight found {len(results)} result(s) for {completed_query}.",
                5000,
            )

        self._queue_worker("Spotlight Search", task, done)

    def _spotlight_direct_action(self, query: str) -> Optional[tuple[str, str]]:
        action = parse_lookup(query)
        if action is not None:
            return action
        detected = detect_links(query)
        return detected[0][2] if detected else None

    def _open_spotlight_result(self, result: SpotlightResult) -> None:
        if result.source == "courtlistener" and isinstance(result.payload, dict):
            self._open_courtlistener_item_text(result.payload)
        elif result.source == "scholar":
            scholar = result.payload
            url = str(getattr(scholar, "url", "") or "")
            title = str(getattr(scholar, "title", "") or result.title)
            if url:
                self._fetch_scholar_url(url, title)
        elif result.source == "cache" and isinstance(result.payload, dict):
            self._open_cached_opinion(result.payload)
        elif result.source == "engrep":
            self._open_english_reports_case(result.payload)
        else:
            QMessageBox.information(self, "Spotlight", "That result type is not openable yet.")

    def _open_spotlight_pdf_result(self, result: SpotlightResult) -> None:
        if result.source == "courtlistener" and isinstance(result.payload, dict):
            self._open_courtlistener_item_pdf(result.payload)
            return
        QMessageBox.information(self, "Spotlight", "PDF opening is available for CourtListener results.")

    def _open_full_search_from_spotlight(self, query: str) -> None:
        self.show()
        self.raise_()
        self.activateWindow()
        if query:
            self.query_edit.setText(query)
            self.search()

    def show_court_picker(self) -> None:
        dialog = CourtPickerDialog(self._selected_courts, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self._selected_courts = dialog.selected_courts()
            self._update_courts_button()

    def _update_courts_button(self) -> None:
        self.courts_btn.setText(courts_summary(self._selected_courts))
        if self._selected_courts:
            self.courts_btn.setToolTip(" ".join(sorted(self._selected_courts)))
        else:
            self.courts_btn.setToolTip("All courts")

    def _show_legacy_hint(self) -> None:
        QMessageBox.information(
            self,
            "Legacy Tk App",
            "The legacy Tkinter app is still available with:\n\n"
            "python courtlistener_gui.py",
        )

    def _install_global_hotkey(self) -> None:
        try:
            from pynput import keyboard as pynput_keyboard
        except Exception:
            return
        hotkey = "<cmd>+<space>" if sys.platform == "darwin" else "<ctrl>+<space>"
        try:
            listener = pynput_keyboard.GlobalHotKeys(
                {hotkey: self._hotkey_bridge.activated.emit}
            )
            listener.daemon = True
            listener.start()
            self._hotkey_listener = listener
        except Exception:
            self._hotkey_listener = None

    def closeEvent(self, event) -> None:  # noqa: N802 - Qt override.
        if self._hotkey_listener is not None:
            try:
                self._hotkey_listener.stop()
            except Exception:
                pass
            self._hotkey_listener = None
        if self._opinion_db is not None:
            try:
                self._opinion_db.close()
            except Exception:
                pass
            self._opinion_db = None
        super().closeEvent(event)

    def _set_result_actions(self, enabled: bool) -> None:
        self.open_text_btn.setEnabled(enabled)
        self.view_pdf_btn.setEnabled(enabled)
        self.download_pdf_btn.setEnabled(enabled)

    def _set_busy(self, busy: bool) -> None:
        self.search_btn.setEnabled(not busy)
        if busy:
            self.statusBar().showMessage("Working...")

    def _remember(self, window: QMainWindow) -> None:
        self._windows.append(window)
        window.destroyed.connect(lambda _=None, w=window: self._forget_window(w))

    def _show_window(self, window: QMainWindow) -> None:
        self._remember(window)
        window.show()
        window.raise_()
        window.activateWindow()
        window.setWindowState(window.windowState() & ~Qt.WindowState.WindowMinimized)

    def _forget_window(self, window: QMainWindow) -> None:
        try:
            self._windows.remove(window)
        except ValueError:
            pass

    def _client_for_token(self, *, required: bool) -> Optional[CourtListenerClient]:
        token = self.token_edit.text().strip()
        if not token:
            if required:
                QMessageBox.warning(
                    self,
                    "CourtListener Token",
                    "Add a CourtListener API token to use this feature.",
                )
            return None
        expected = f"Token {token}"
        if self._client is None or self._client._session.headers.get("Authorization") != expected:
            self._client = CourtListenerClient(api_token=token)
            save_token(token)
        return self._client

    def _scholar_fetcher(self):
        if GoogleScholarFetcher is None:
            QMessageBox.warning(
                self,
                "Google Scholar",
                "Google Scholar support needs beautifulsoup4.",
            )
            return None
        if self._scholar is None:
            self._scholar = GoogleScholarFetcher(
                db=self._opinion_database(),
                name_scorer=name_match_score,
            )
        return self._scholar

    def _opinion_database(self) -> Optional[OpinionDB]:
        if self._opinion_db is None and not self._opinion_db_failed:
            try:
                self._opinion_db = OpinionDB()
            except Exception as exc:
                print(f"[qt] opinion database unavailable: {exc}")
                self._opinion_db_failed = True
        return self._opinion_db

    def _queue_worker(self, label: str, fn, finished) -> None:
        self._pending_jobs += 1
        self._set_busy(True)
        worker = Worker(fn)
        self._workers.append(worker)
        worker.signals.status.connect(self.statusBar().showMessage)
        worker.signals.finished.connect(
            lambda result, w=worker: self._worker_finished(result, finished, w)
        )
        worker.signals.error.connect(
            lambda message, w=worker: self._worker_error(label, message, w)
        )
        self.pool.start(worker)

    def _release_worker(self, worker: Worker) -> None:
        try:
            self._workers.remove(worker)
        except ValueError:
            pass

    def _worker_finished(self, result, finished, worker: Worker) -> None:
        try:
            finished(result)
        except Exception:
            self._show_error("UI update failed", traceback.format_exc())
        finally:
            self._release_worker(worker)
            self._job_done()

    def _worker_error(self, label: str, message: str, worker: Worker) -> None:
        if label == "Spotlight Search" and self._spotlight is not None:
            short = message.strip().splitlines()[-1] if message.strip() else message
            self._spotlight.set_error(short)
        self._show_error(label, message)
        self._release_worker(worker)
        self._job_done()

    def _job_done(self) -> None:
        self._pending_jobs = max(0, self._pending_jobs - 1)
        if self._pending_jobs == 0:
            self._set_busy(False)

    def _show_error(self, title: str, message: str) -> None:
        short = message.strip().splitlines()[-1] if message.strip() else message
        self.statusBar().showMessage(f"{title}: {short}", 8000)
        QMessageBox.critical(self, title, message[-2500:])

    def search(self) -> None:
        query = self.query_edit.text().strip()
        if not query:
            QMessageBox.information(self, "Search", "Enter a search query first.")
            return

        self._court_results.clear()
        self.court_table.set_rows([])
        self.scholar_table.set_rows([])
        self.preview.clear()
        self._set_result_actions(False)

        started = False
        court_ids = " ".join(sorted(self._selected_courts)) or None
        date_min = self.from_edit.text().strip() or None
        date_max = self.to_edit.text().strip() or None
        page_size = self.limit_spin.value()
        client = self._client_for_token(required=False)

        if client is not None:
            started = True

            def court_task(status):
                status("Searching CourtListener...")
                return client.search(
                    query,
                    type="o",
                    court=court_ids,
                    date_filed_min=date_min,
                    date_filed_max=date_max,
                    highlight=True,
                    page_size=page_size,
                )

            self._queue_worker("CourtListener Search", court_task, self._show_court_results)
        else:
            self.statusBar().showMessage(
                "No CourtListener token set; searching Google Scholar only."
            )

        fetcher = self._scholar_fetcher()
        if fetcher is not None:
            started = True

            def scholar_task(status):
                status("Searching Google Scholar...")
                return fetcher.search_cases(query, limit=15)

            self._queue_worker("Google Scholar Search", scholar_task, self._show_scholar_results)

        if not started:
            self.statusBar().showMessage("No search source is available.")

    def _show_court_results(self, data: dict) -> None:
        results = data.get("results", [])
        for item in results:
            normalize_result_citations(item)
        self._court_results = results

        main_rows: list[tuple[list[str], object]] = []
        order_rows: list[tuple[list[str], object]] = []
        for item in results:
            row = format_case_row(item)
            values = [row.case_name, row.court, row.date_filed, row.citation, row.status]
            if is_scotus_order(item):
                order_rows.append((values, item))
            else:
                main_rows.append((values, item))
        self.court_table.set_rows(main_rows + order_rows)
        count = data.get("count", len(results))
        self.statusBar().showMessage(f"Showing {len(results)} of {count:,} CourtListener results.")

    def _show_scholar_results(self, results: list) -> None:
        def field(row, name: str) -> str:
            if isinstance(row, dict):
                return str(row.get(name, ""))
            return str(getattr(row, name, ""))

        rows = [([field(r, "title"), field(r, "source")], r) for r in results]
        self.scholar_table.set_rows(rows)
        self.scholar_table.setVisible(True)
        self.scholar_table.viewport().update()
        self.statusBar().showMessage(f"Showing {len(results)} Google Scholar results.", 5000)

    def _on_court_selection(self) -> None:
        item = self.court_table.current_payload()
        self._set_result_actions(item is not None)
        if item is not None:
            self.preview.setPlainText(preview_from_item(item) or "No preview available.")

    def _on_scholar_selection(self) -> None:
        result = self.scholar_table.current_payload()
        self.open_scholar_btn.setEnabled(result is not None)
        if result is not None:
            self.preview.setPlainText(result.snippet or "No snippet available.")

    def open_selected_scholar(self) -> None:
        result = self.scholar_table.current_payload()
        if result is None:
            return
        self._fetch_scholar_url(result.url, result.title)

    def open_selected_text(self) -> None:
        item = self.court_table.current_payload()
        if item is None:
            return
        self._open_courtlistener_item_text(item)

    def _open_courtlistener_item_text(self, item: dict) -> None:
        appx_cite = federal_appendix_cite(item)
        if appx_cite and self._open_case_law_pdf(appx_cite):
            return
        fetcher = self._scholar_fetcher()
        client = self._client_for_token(required=False)
        cite = pick_citation(item.get("citation", []))
        name = case_name(item)
        year = str(item.get("dateFiled") or item.get("date_filed") or "")[:4] or None

        def task(status):
            if fetcher is not None:
                status("Fetching opinion text from Google Scholar...")
                try:
                    if cite:
                        result = fetcher.fetch_by_citation(cite)
                        if result:
                            return ("scholar", result)
                    result = fetcher.fetch_by_name(name, year)
                    if result:
                        return ("scholar", result)
                except Exception as exc:
                    print(f"[qt] Scholar text failed; trying CourtListener: {exc}")
            if client is not None:
                status("Fetching opinion text from CourtListener...")
                return ("courtlistener", assemble_case_parts(client, item))
            return None

        def done(result) -> None:
            window = self._show_text_result(result, name)
            if result and result[0] == "scholar":
                self._attach_oyez_details_for_item(window, item)

        self._queue_worker("Opinion Text", task, done)

    def _open_cached_opinion(self, summary: dict) -> None:
        scholar_id = str(summary.get("scholar_id") or "")
        title = str(summary.get("name") or "Cached opinion")
        if not scholar_id:
            QMessageBox.warning(self, "Cache", "That cached result is missing its opinion id.")
            return

        def task(status):
            status("Opening cached opinion...")
            db = OpinionDB()
            try:
                record = db.get_by_scholar_id(scholar_id)
                if not record or not record.get("html"):
                    return None
                return record
            finally:
                db.close()

        def done(record: Optional[dict]) -> None:
            if not record:
                QMessageBox.warning(self, "Cache", "That cached opinion could not be opened.")
                return
            self._show_opinion_result(
                (record.get("url") or "", record.get("html") or ""),
                record.get("name") or title,
            )

        self._queue_worker("Cached Opinion", task, done)

    def _fetch_scholar_url(self, url: str, title: str) -> None:
        fetcher = self._scholar_fetcher()
        if fetcher is None:
            return

        def task(status):
            status("Fetching opinion text from Google Scholar...")
            return fetcher.fetch_by_url(url)

        self._queue_worker(
            "Scholar Text",
            task,
            lambda result: self._show_text_result(("scholar", result) if result else None, title),
        )

    def _show_text_result(self, result, title: str) -> Optional[HtmlWindow]:
        if not result:
            QMessageBox.warning(
                self,
                "Opinion Text",
                "No matching opinion text was found from Scholar or CourtListener.",
            )
            return None
        source, payload = result
        if source == "scholar":
            return self._show_opinion_result(payload, title)
        if source == "courtlistener" and isinstance(payload, CourtListenerOpinion):
            body = render_opinion_parts_body(
                payload.title or title,
                payload.parts,
                source_label="CourtListener text",
                note="Loaded from CourtListener because Scholar text was unavailable.",
            )
            window = HtmlWindow(payload.title or title, body, link_callback=self._handle_link)
            self._show_window(window)
            self._attach_oyez_details_for_cluster(window, payload.cluster, payload.title or title)
            return window
        QMessageBox.warning(self, "Opinion Text", "The opinion text result was not understood.")
        return None

    def _show_opinion_result(self, result, title: str) -> Optional[HtmlWindow]:
        if not result:
            QMessageBox.warning(self, "Scholar Text", "No matching opinion text was found.")
            return None
        url, opinion_html = result
        body = render_scholar_opinion_body(title, url, opinion_html)
        window = HtmlWindow(title, body, base_url=url, link_callback=self._handle_link)
        self._show_window(window)
        self._attach_oyez_details_for_title(window, title)
        return window

    def _attach_oyez_details_for_item(self, window: Optional[HtmlWindow], item: dict) -> None:
        cites = citation_list(item.get("citation", []))
        name = case_name(item, "")
        year = str(item.get("dateFiled") or item.get("date_filed") or "")[:4]
        hint = " ".join(
            str(item.get(key) or "")
            for key in ("court_id", "court", "court_citation_string")
        )
        self._attach_oyez_details(window, cites, name, year, hint)

    def _attach_oyez_details_for_cluster(
        self,
        window: Optional[HtmlWindow],
        cluster: dict,
        fallback_title: str,
    ) -> None:
        cites = citation_list(cluster.get("citations") or [])
        name = strip_html(cluster.get("case_name") or fallback_title)
        year = str(cluster.get("date_filed") or "")[:4]
        hint = str(cluster.get("court_id") or cluster.get("court") or "")
        self._attach_oyez_details(window, cites, name, year, hint)

    def _attach_oyez_details_for_title(
        self,
        window: Optional[HtmlWindow],
        title: str,
    ) -> None:
        parsed = parse_citation_line(title)
        if parsed is None:
            cites: list[str] = []
            name = re.split(r"\s[-\u2013]\s", title or "", maxsplit=1)[0].strip()
        else:
            name, cite, _pin = parsed
            cites = [cite]
            if not name:
                name = re.split(r"\s[-\u2013]\s", title or "", maxsplit=1)[0].strip()
        years = re.findall(r"\b(?:17|18|19|20)\d{2}\b", title or "")
        year = years[-1] if years else ""
        hint = "Supreme Court" if "supreme court" in (title or "").lower() else ""
        self._attach_oyez_details(window, cites, name, year, hint)

    def _attach_oyez_details(
        self,
        window: Optional[HtmlWindow],
        cites: list[str],
        name: str,
        year: str,
        hint: str = "",
    ) -> None:
        if window is None:
            return
        clean_cites = list(dict.fromkeys(c for c in cites if c))
        if not self._should_lookup_oyez(clean_cites, hint):
            return

        def task(status):
            status("Looking up Supreme Court details...")
            try:
                return oyez.lookup(cites=clean_cites, name=name, year=year)
            except Exception as exc:
                print(f"[qt] Oyez lookup failed for {name!r}: {exc}")
                return None

        def done(case) -> None:
            fragment = render_oyez_case_details(case)
            if not fragment:
                return
            try:
                window.insert_top_fragment(fragment)
            except RuntimeError:
                pass

        self._queue_worker("Oyez Details", task, done)

    @staticmethod
    def _should_lookup_oyez(cites: list[str], hint: str = "") -> bool:
        haystack = " ".join(cites + [hint]).lower()
        if "scotus" in haystack or "supreme court" in haystack:
            return True
        return any(re.search(r"\b\d+\s+U\.?\s*S\.?\s+\d+\b", cite, re.IGNORECASE) for cite in cites)

    def view_selected_pdf(self) -> None:
        item = self.court_table.current_payload()
        if item is None:
            return
        self._open_courtlistener_item_pdf(item)

    def _open_courtlistener_item_pdf(self, item: dict) -> None:
        client = self._client_for_token(required=True)
        if client is None:
            return

        def task(status):
            status("Finding PDF...")
            return resolve_pdf_url(client, item, status=status)

        def done(url: Optional[str]) -> None:
            if not url:
                QMessageBox.warning(self, "PDF", "No PDF was found for this opinion.")
                return
            window = ChromiumPdfWindow(url, f"PDF - {case_name(item)}")
            self._show_window(window)

        self._queue_worker("PDF Lookup", task, done)

    def download_selected_pdf(self) -> None:
        item = self.court_table.current_payload()
        if item is None:
            return
        client = self._client_for_token(required=True)
        if client is None:
            return
        default = build_default_filename(item) + ".pdf"
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Download PDF",
            default,
            "PDF files (*.pdf);;All files (*.*)",
        )
        if not path:
            return

        def task(status):
            status("Resolving PDF...")
            url = resolve_pdf_url(client, item, status=status)
            if not url:
                raise RuntimeError("No PDF was found for this opinion.")
            status("Downloading PDF...")
            data = fetch_pdf_bytes(client, url)
            Path(path).write_bytes(data)
            return path

        self._queue_worker(
            "Download PDF",
            task,
            lambda saved: self.statusBar().showMessage(f"Saved PDF to {saved}", 7000),
        )

    def open_brief(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Brief",
            "",
            "Briefs (*.pdf *.docx *.doc *.rtf *.txt);;All files (*.*)",
        )
        if not path:
            return

        def task(status):
            status("Reading brief...")
            return brief_reader.extract_text(path)

        def done(text: str) -> None:
            title = f"Brief - {Path(path).name}"
            open_original = (
                (lambda checked=False, p=path: self._open_local_pdf(p))
                if Path(path).suffix.lower() == ".pdf"
                else None
            )
            window = BriefWindow(
                title,
                text,
                self._handle_link,
                source_path=path,
                open_original_callback=open_original,
            )
            self._show_window(window)
            self.statusBar().showMessage("Brief opened.", 5000)

        self._queue_worker("Open Brief", task, done)

    def _open_local_pdf(self, path: str) -> None:
        pdf_path = Path(path).resolve()
        url = QUrl.fromLocalFile(str(pdf_path)).toString()
        window = ChromiumPdfWindow(url, f"PDF - {pdf_path.name}")
        self._show_window(window)

    def open_citation_list(self) -> None:
        if self._citation_list_dialog is None:
            dialog = CitationListDialog(self)
            dialog.open_requested.connect(lambda lines, d=dialog: self._run_citation_list(lines, d))
            dialog.destroyed.connect(lambda: setattr(self, "_citation_list_dialog", None))
            self._citation_list_dialog = dialog
        self._citation_list_dialog.show()
        self._citation_list_dialog.raise_()
        self._citation_list_dialog.activateWindow()

    def _run_citation_list(self, lines: list[str], dialog: CitationListDialog) -> None:
        entries: list[tuple[str, str, str, str]] = []
        failures: list[str] = []
        for line in lines:
            parsed = parse_citation_line(line)
            if parsed is None:
                failures.append(f"{line}   (no citation recognized)")
                continue
            name, cite, pin = parsed
            entries.append((line, name, cite, pin))
        if not entries:
            dialog.set_finished(0, len(lines), failures)
            return

        fetcher = self._scholar_fetcher() if GoogleScholarFetcher is not None else None
        client = self._client_for_token(required=False)
        has_direct_pdf = any(is_federal_appendix_cite(cite) for _line, _name, cite, _pin in entries)
        if fetcher is None and client is None and not has_direct_pdf:
            dialog.set_status("Neither Google Scholar nor CourtListener is available.")
            return

        dialog.set_running(True)

        def task(status):
            opened = []
            missed = list(failures)
            total = len(entries)
            for index, (line, name, cite, pin) in enumerate(entries, 1):
                status(f"({index}/{total}) Opening {cite}...")
                resolved = self._resolve_citation(name, cite, pin, fetcher, client, status)
                if resolved is None:
                    missed.append(line)
                else:
                    opened.append(resolved)
            return opened, missed

        def done(result) -> None:
            opened, missed = result
            for resolved in opened:
                self._show_resolved_citation(resolved)
            dialog.set_finished(len(opened), len(lines), missed)
            self.statusBar().showMessage(
                f"Opened {len(opened)} of {len(lines)} citation(s).",
                7000,
            )

        self._queue_worker("Open Citation List", task, done)

    def quick_lookup(self) -> None:
        text, ok = QInputDialog.getText(
            self,
            "Quick Lookup",
            "Citation or source lookup",
            text=self.query_edit.text().strip(),
        )
        if not ok or not text.strip():
            return
        action = parse_lookup(text)
        if action is None:
            detected = detect_links(text)
            if detected:
                action = detected[0][2]
        if action is None:
            QMessageBox.information(
                self,
                "Quick Lookup",
                "I could not parse that citation yet. Try the main search box for case names.",
            )
            return
        self._handle_action(action)

    def _handle_action(self, action: tuple[str, str]) -> None:
        kind, value = action
        if kind == "cite":
            self._open_citation(value)
        elif kind in {"usc", "cfr", "rule", "const", "statestat"}:
            self._open_source_action(kind, value)
        elif kind == "statpdf":
            window = ChromiumPdfWindow(value, "Statutes at Large PDF")
            self._show_window(window)
        elif kind == "engrep":
            self._open_english_reports_spec(value)
        elif kind == "browse":
            QDesktopServices.openUrl(QUrl(value))
        else:
            QMessageBox.information(
                self,
                "Citation",
                f"This Qt view detected a {kind or 'citation'} link, but that "
                "source viewer has not been migrated yet.",
            )

    def _open_english_reports_spec(self, spec: str) -> None:
        cases = eng_rep.resolve(spec)
        if not cases:
            QDesktopServices.openUrl(QUrl(english_reports_url(spec)))
            return
        case = self._choose_english_reports_case(cases)
        if case is not None:
            self._open_english_reports_case(case)

    def _choose_english_reports_case(self, cases: list[eng_rep.ERCase]):
        if len(cases) == 1:
            return cases[0]
        labels = [case.label for case in cases]
        chosen, ok = QInputDialog.getItem(
            self,
            "English Reports",
            "Select case",
            labels,
            0,
            False,
        )
        if not ok:
            return None
        try:
            return cases[labels.index(chosen)]
        except ValueError:
            return None

    def _open_english_reports_case(self, case) -> None:
        if not hasattr(case, "pdf_url"):
            QMessageBox.warning(self, "English Reports", "That English Reports case is unavailable.")
            return
        title = f"English Reports - {getattr(case, 'label', 'case')}"
        window = ChromiumPdfWindow(case.pdf_url, title)
        self._show_window(window)

    def _handle_link(self, url: str) -> None:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != "getcases":
            QDesktopServices.openUrl(QUrl(url))
            return
        if parsed.netloc == "scholar":
            params = urllib.parse.parse_qs(parsed.query)
            target = (params.get("url") or [""])[0]
            title = (params.get("title") or ["Cited case"])[0] or "Cited case"
            if target:
                self._fetch_scholar_url(target, title)
            return
        params = urllib.parse.parse_qs(parsed.query)
        kind = (params.get("kind") or [""])[0]
        value = (params.get("value") or [""])[0]
        self._handle_action((kind, value))

    def _open_source_action(self, kind: str, spec: str) -> None:
        def task(status):
            return load_source(kind, spec, status=status)

        def done(loaded) -> None:
            window = HtmlWindow(
                source_title(loaded),
                source_body(loaded),
                base_url=str(getattr(loaded.doc, "url", "")),
                link_callback=self._handle_link,
            )
            self._show_window(window)

        self._queue_worker("Source Lookup", task, done)

    def _open_citation(self, cite: str) -> None:
        base = cite.split("@", 1)[0]
        pin = cite.split("@", 1)[1] if "@" in cite else ""
        if is_federal_appendix_cite(base) and self._open_case_law_pdf(base):
            return
        fetcher = self._scholar_fetcher()
        client = self._client_for_token(required=False)
        if fetcher is None and client is None:
            QMessageBox.warning(
                self,
                "Citation",
                "No opinion source is available. Add a CourtListener token or install Scholar support.",
            )
            return

        def task(status):
            status(f"Opening {base}...")
            return self._resolve_citation("", base, pin, fetcher, client, status)

        self._queue_worker(
            "Open Citation",
            task,
            lambda result: self._show_resolved_citation(result, fallback_title=base),
        )

    def _resolve_citation(self, name: str, cite: str, pin: str, fetcher, client, status):
        if is_federal_appendix_cite(cite):
            url = static_case_law_url(cite)
            if url:
                title = f"{name} - {cite}" if name else cite
                return "pdf", (url, title), title, ""
        if fetcher is not None:
            try:
                result = fetcher.fetch_by_citation(cite)
                if not result and name:
                    hits = fetcher.search_cases(f"{name} {cite}", limit=1)
                    if hits:
                        result = fetcher.fetch_by_url(hits[0].url)
                if result:
                    return "scholar", result, name or cite, pin
            except Exception as exc:
                print(f"[qt] Scholar citation lookup failed for {cite!r}: {exc}")
        if client is not None:
            try:
                for candidate in courtlistener_spotlight_results(client, cite):
                    if isinstance(candidate.payload, dict):
                        status("Fetching opinion text from CourtListener...")
                        opinion = assemble_case_parts(client, candidate.payload)
                        return "courtlistener", opinion, name or cite, pin
            except Exception as exc:
                print(f"[qt] CourtListener citation lookup failed for {cite!r}: {exc}")
        return None

    def _show_resolved_citation(self, resolved, fallback_title: str = "Citation") -> None:
        if not resolved:
            self._show_text_result(None, fallback_title)
            return
        if len(resolved) == 3:
            source, payload, title = resolved
            pin = ""
        else:
            source, payload, title, pin = resolved
        if source == "pdf":
            url, pdf_title = payload
            window = ChromiumPdfWindow(url, pdf_title)
            self._show_window(window)
            return
        window = self._show_text_result((source, payload), title or fallback_title)
        if pin and isinstance(window, HtmlWindow):
            window.scroll_to_page(pin)

    def _open_case_law_pdf(self, cite: str) -> bool:
        url = static_case_law_url(cite)
        if not url:
            return False
        window = ChromiumPdfWindow(url, f"case.law PDF - {cite}")
        self._show_window(window)
        self.statusBar().showMessage(f"Opening {cite} from case.law.", 5000)
        return True


def main() -> None:
    app = QApplication(sys.argv)
    app.setApplicationName("GetCases")
    app.setStyle("Fusion")
    app.setStyleSheet(APP_STYLE)
    window = GetCasesQt()
    window.show()
    QTimer.singleShot(250, window.show_spotlight)
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
