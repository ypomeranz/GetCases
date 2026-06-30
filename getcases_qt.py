"""PySide6/QtWebEngine front end for GetCases.

This is the migration entry point for the Qt version of the app.  The legacy
Tkinter UI remains in ``courtlistener_gui.py`` while feature parity is brought
over incrementally.
"""

from __future__ import annotations

import html
import sys
import traceback
import urllib.parse
from pathlib import Path
from typing import Optional

try:
    from PySide6.QtCore import Qt, QThreadPool, QUrl
    from PySide6.QtGui import QAction, QDesktopServices
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWidgets import (
        QApplication,
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
from case_utils import (
    build_default_filename,
    case_name,
    format_case_row,
    is_scotus_order,
    normalize_result_citations,
    pick_citation,
    preview_from_item,
    strip_html,
)
from citations import detect_links
from courtlistener import CourtListenerClient, CourtListenerError
from getcases_config import load_token, save_token
from pdf_resolver import fetch_pdf_bytes, resolve_pdf_url
from qt_opinions import render_scholar_opinion_body
from qt_pdf import ChromiumPdfWindow, LinkHandlingPage, html_document
from qt_sources import (
    english_reports_url,
    load_source,
    parse_lookup,
    source_body,
    source_title,
)
from qt_workers import Worker

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
QStatusBar {
  background: #ffffff;
  border-top: 1px solid #dde3ea;
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


class BriefWindow(HtmlWindow):
    def __init__(self, title: str, text: str, link_callback, parent=None) -> None:
        super().__init__(
            title,
            _brief_body(text),
            link_callback=link_callback,
            parent=parent,
        )


def _brief_body(text: str) -> str:
    links = detect_links(text)
    parts: list[str] = [
        "<h1>Brief</h1>",
        '<p class="muted">Detected citations are highlighted and clickable.</p>',
        "<pre>",
    ]
    pos = 0
    for start, end, action in links:
        if start < pos:
            continue
        parts.append(html.escape(text[pos:start]))
        kind, value = action
        href = "getcases://open?" + urllib.parse.urlencode({"kind": kind, "value": value})
        label = html.escape(text[start:end])
        parts.append(f'<a class="cite" href="{href}">{label}</a>')
        pos = end
    parts.append(html.escape(text[pos:]))
    parts.append("</pre>")
    return "".join(parts)


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
        self._windows: list[QMainWindow] = []

        self._build_ui()
        self._wire_actions()
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

        self.court_edit = QLineEdit()
        self.court_edit.setPlaceholderText("Court IDs, optional: scotus ca9 cadc")
        search_layout.addWidget(QLabel("Courts"), 1, 3)
        search_layout.addWidget(self.court_edit, 1, 4)

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

        self.open_brief_action = QAction("Open Brief", self)
        toolbar.addAction(self.open_brief_action)

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
        self.search_btn.clicked.connect(self.search)
        self.query_edit.returnPressed.connect(self.search)
        self.save_token_action.triggered.connect(self._save_token)
        self.open_brief_action.triggered.connect(self.open_brief)
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

    def _show_legacy_hint(self) -> None:
        QMessageBox.information(
            self,
            "Legacy Tk App",
            "The legacy Tkinter app is still available with:\n\n"
            "python courtlistener_gui.py",
        )

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
            self._scholar = GoogleScholarFetcher()
        return self._scholar

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
        court_ids = self.court_edit.text().strip() or None
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
        fetcher = self._scholar_fetcher()
        if fetcher is None:
            return
        cite = pick_citation(item.get("citation", []))
        name = case_name(item)
        year = str(item.get("dateFiled") or item.get("date_filed") or "")[:4] or None

        def task(status):
            status("Fetching opinion text from Google Scholar...")
            if cite:
                result = fetcher.fetch_by_citation(cite)
                if result:
                    return result
            return fetcher.fetch_by_name(name, year)

        self._queue_worker(
            "Scholar Text",
            task,
            lambda result: self._show_opinion_result(result, name),
        )

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
            lambda result: self._show_opinion_result(result, title),
        )

    def _show_opinion_result(self, result, title: str) -> None:
        if not result:
            QMessageBox.warning(self, "Scholar Text", "No matching opinion text was found.")
            return
        url, opinion_html = result
        body = render_scholar_opinion_body(title, url, opinion_html)
        window = HtmlWindow(title, body, base_url=url, link_callback=self._handle_link)
        self._show_window(window)

    def view_selected_pdf(self) -> None:
        item = self.court_table.current_payload()
        if item is None:
            return
        client = self._client_for_token(required=True)
        if client is None:
            return

        def task(status):
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
            window = BriefWindow(title, text, self._handle_link)
            self._show_window(window)
            self.statusBar().showMessage("Brief opened.", 5000)

        self._queue_worker("Open Brief", task, done)

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
            QDesktopServices.openUrl(QUrl(english_reports_url(value)))
        elif kind == "browse":
            QDesktopServices.openUrl(QUrl(value))
        else:
            QMessageBox.information(
                self,
                "Citation",
                f"This Qt view detected a {kind or 'citation'} link, but that "
                "source viewer has not been migrated yet.",
            )

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
        fetcher = self._scholar_fetcher()
        if fetcher is None:
            return
        base = cite.split("@", 1)[0]

        def task(status):
            status(f"Opening {base}...")
            return fetcher.fetch_by_citation(base)

        self._queue_worker(
            "Open Citation",
            task,
            lambda result: self._show_opinion_result(result, base),
        )


def main() -> None:
    app = QApplication(sys.argv)
    app.setApplicationName("GetCases")
    app.setStyle("Fusion")
    app.setStyleSheet(APP_STYLE)
    window = GetCasesQt()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
