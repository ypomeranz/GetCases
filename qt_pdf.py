"""Chromium-backed viewing helpers for the PySide6 GetCases UI."""

from __future__ import annotations

import html

from PySide6.QtCore import Qt, QTimer, QUrl, Signal
from PySide6.QtGui import QDesktopServices
from PySide6.QtWebEngineCore import QWebEnginePage, QWebEngineSettings
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QToolBar,
    QWidget,
)


LINK_BLUE = "#49a8ff"


class LinkHandlingPage(QWebEnginePage):
    """WebEngine page that reports clicked links to Python instead of navigating."""

    link_activated = Signal(str)

    def acceptNavigationRequest(self, url, nav_type, is_main_frame):  # noqa: N802
        if nav_type == QWebEnginePage.NavigationType.NavigationTypeLinkClicked:
            self.link_activated.emit(url.toString())
            return False
        return super().acceptNavigationRequest(url, nav_type, is_main_frame)


def html_document(title: str, body: str, base_url: str = "") -> str:
    """Wrap a content fragment in the app's readable WebEngine style."""
    base = f'<base href="{html.escape(base_url, quote=True)}">' if base_url else ""
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  {base}
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #1f2933;
      --muted: #697586;
      --paper: #fffdf8;
      --line: #dde3ea;
      --link: {LINK_BLUE};
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: #eef2f5;
      color: var(--ink);
      font-family: "Segoe UI", system-ui, sans-serif;
      line-height: 1.55;
    }}
    main {{
      max-width: 940px;
      margin: 0 auto;
      min-height: 100vh;
      padding: 34px 44px 60px;
      background: var(--paper);
      border-left: 1px solid var(--line);
      border-right: 1px solid var(--line);
    }}
    h1, h2, h3 {{ line-height: 1.2; }}
    a, a:visited {{
      color: var(--link);
      text-decoration-color: rgba(73, 168, 255, 0.45);
      text-underline-offset: 0.16em;
    }}
    p {{ margin: 0 0 0.86rem; }}
    .muted {{ color: var(--muted); }}
    .cite {{
      background: rgba(73, 168, 255, 0.12);
      border-radius: 3px;
      padding: 0 2px;
    }}
    .source-meta {{
      display: grid;
      gap: 4px;
      margin: 0 0 24px;
      padding: 12px 14px;
      background: #f6f8fa;
      border: 1px solid var(--line);
      border-radius: 8px;
      color: var(--muted);
    }}
    .source-meta strong {{
      color: var(--ink);
      font-size: 1.02rem;
    }}
    .para.sechead {{
      font-weight: 700;
      font-size: 1.12rem;
      margin-top: 1.2rem;
    }}
    .para.head, .para.note-head {{
      font-weight: 650;
      margin-top: 1rem;
    }}
    .para.credit, .para.note-body {{
      color: var(--muted);
      font-size: 0.94rem;
    }}
    .indent-1 {{ margin-left: 1.4rem; }}
    .indent-2 {{ margin-left: 2.8rem; }}
    .indent-3 {{ margin-left: 4.2rem; }}
    .indent-4 {{ margin-left: 5.6rem; }}
    .indent-5 {{ margin-left: 7rem; }}
    .indent-6, .indent-7, .indent-8 {{ margin-left: 8.4rem; }}
    pre {{
      white-space: pre-wrap;
      font: inherit;
      margin: 0;
    }}
  </style>
</head>
<body>
  <main>{body}</main>
</body>
</html>"""


class ChromiumPdfWindow(QMainWindow):
    """Show a PDF URL in QtWebEngine's Chromium PDF viewer."""

    def __init__(self, url: str, title: str, parent=None) -> None:
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        self.setWindowFlag(Qt.WindowType.Window, True)
        self._url = url
        self.setWindowTitle(title or "PDF")
        self.resize(1120, 860)

        self.view = QWebEngineView(self)
        settings = self.view.settings()
        settings.setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, True)
        if hasattr(QWebEngineSettings.WebAttribute, "PdfViewerEnabled"):
            settings.setAttribute(
                QWebEngineSettings.WebAttribute.PdfViewerEnabled, True
            )
        self.setCentralWidget(self.view)

        toolbar = QToolBar("PDF", self)
        toolbar.setMovable(False)
        self.addToolBar(toolbar)
        toolbar.addWidget(QLabel("PDF"))
        toolbar.addSeparator()

        reload_btn = QPushButton("Reload")
        reload_btn.clicked.connect(self.view.reload)
        toolbar.addWidget(reload_btn)

        external_btn = QPushButton("Open in Browser")
        external_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(self._url)))
        toolbar.addWidget(external_btn)

        copy_btn = QPushButton("Copy Link")
        copy_btn.clicked.connect(self._copy_link)
        toolbar.addWidget(copy_btn)

        spacer = QWidget()
        spacer.setLayout(QHBoxLayout())
        toolbar.addWidget(spacer)

        self.view.loadFinished.connect(self._after_load)
        self.view.load(QUrl(url))

    def _copy_link(self) -> None:
        from PySide6.QtWidgets import QApplication

        QApplication.clipboard().setText(self._url)

    def _after_load(self, ok: bool) -> None:
        if ok:
            self._inject_link_color()
            QTimer.singleShot(700, self._inject_link_color)
            QTimer.singleShot(1800, self._inject_link_color)

    def _inject_link_color(self) -> None:
        # The built-in Chromium PDF viewer does not expose every internal layer
        # consistently, but this catches PDF.js-style annotation layers and any
        # ordinary HTML links we control.
        js = f"""
(() => {{
  const id = "getcases-link-color";
  if (!document.getElementById(id)) {{
    const style = document.createElement("style");
    style.id = id;
    style.textContent = `
      a, a:visited, .linkAnnotation a, .textLayer a {{
        color: {LINK_BLUE} !important;
        outline-color: {LINK_BLUE} !important;
        text-decoration-color: rgba(73,168,255,.55) !important;
      }}
      .linkAnnotation > a {{
        background: rgba(73,168,255,.12) !important;
      }}
    `;
    document.documentElement.appendChild(style);
  }}
}})();
"""
        self.view.page().runJavaScript(js)
