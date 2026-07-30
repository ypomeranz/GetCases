"""Window ▸ "View PDF in Separate Window" and the minimal floating viewer.

The opinion window normally shows a PDF in place of its text.  With the new
Window-menu setting ticked, the "PDF" button instead hands the scan to
``_FloatingPdfWindow`` — a small Preview-style window that is nothing but the
page under a thin strip of zoom controls — and leaves the reader on the text.

The methods are lifted out of ``courtlistener_gui`` with ``ast`` (importing it
needs tkinter, absent on a headless run) and driven against stubs.
"""

import ast
import pathlib
import re
import sys
import typing
import unittest
from unittest import mock


SRC = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
TREE = ast.parse(SRC)


class _Tk:
    """Just the tkinter surface the extracted methods touch — including the
    names used only in their annotations, which Python evaluates at def time."""

    TclError = Exception
    Menu = Misc = Frame = Toplevel = object


def _load(cls: str, names, extra=None) -> dict:
    """Exec the named methods of *cls* into a namespace built from stubs."""
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(SRC, n) for n in body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found on {cls}: {missing}")
    ns = {"tk": _Tk, "sys": sys, "re": re, "Optional": typing.Optional,
          "_CaseTabPage": type("_CaseTabPage", (), {}), "_ACCEL": "Ctrl"}
    ns.update(extra or {})
    for name in names:
        # A decorator (@staticmethod) is not part of the FunctionDef segment,
        # so an extracted static method is exec'd as a plain function.
        exec(found[name], ns)
    return ns


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _Var:
    def __init__(self, value=None):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class _FakeMenu:
    def __init__(self):
        self.items = []

    def delete(self, *_a):
        self.items = []

    def add_checkbutton(self, **kw):
        self.items.append(("checkbutton", kw))

    def add_command(self, **kw):
        self.items.append(("command", kw))

    def add_separator(self):
        self.items.append(("separator", {}))

    def labels(self):
        return [kw.get("label") for _kind, kw in self.items]

    def entry(self, label):
        return next(kw for kind, kw in self.items if kw.get("label") == label)


class _FakePane:
    """The bits of _PdfPane the floating viewer calls."""

    _MARGIN = 18

    def __init__(self):
        self.zoomed = []
        self.links = None
        self.find_pages = None
        self.find_bind = None
        self.scrolled = []
        self.destroyed = False

    def zoom(self, delta):
        self.zoomed.append(delta)

    def zoom_percent(self):
        return 125 if self.zoomed else 100

    def set_citation_links(self, links, on_left=None, on_right=None,
                           quiet_pages=None):
        self.links = (links, on_left, on_right, quiet_pages)

    def enable_find(self, pages, bind_keys=True):
        self.find_pages, self.find_bind = pages, bind_keys

    def scroll_to_page(self, page, y_pt=None):
        self.scrolled.append((page, y_pt))

    def destroy(self):
        self.destroyed = True


class _FakeFloatingWindow:
    """Stands in for _FloatingPdfWindow while testing the reader's side."""

    opened = []

    def __init__(self, parent, data, url, title, **kw):
        self.parent, self.data, self.url, self.title = parent, data, url, title
        self.kw = kw
        self.replaced = []
        self.scrolled = []
        self.surfaced = 0
        self.analyses = []
        self._alive = True
        _FakeFloatingWindow.opened.append(self)

    def alive(self):
        return self._alive

    def showing(self, url):
        return self.url == url

    def set_pdf(self, data, url, title="", margin=None):
        self.data, self.url, self.title = data, url, title
        self.replaced.append(url)

    def scroll_to_page(self, page, y_pt=None):
        self.scrolled.append((page, y_pt))

    def surface(self):
        self.surfaced += 1

    def apply_analysis(self, result):
        self.analyses.append(result)

    def close(self):
        self._alive = False


class _InWindow(Exception):
    """Raised by a stub only the in-window PDF path reaches."""


# ---------------------------------------------------------------------------
# The app-level setting
# ---------------------------------------------------------------------------

APP_NS = _load(
    "CourtListenerGUI",
    ["populate_window_menu", "pdf_opens_in_separate_window",
     "set_pdf_separate_window"],
    {"_load_config": lambda: dict(APP_CONFIG),
     "_save_config": lambda data: APP_CONFIG.update(data)},
)
APP_CONFIG: dict = {}


class _App:
    def __init__(self, separate=False):
        self._case_tabs_var = _Var(False)
        self._case_tabs_enabled = False
        self._pdf_separate_window = separate
        self._pdf_separate_var = _Var(separate)
        self.tabs_calls = []
        for name in ("populate_window_menu", "pdf_opens_in_separate_window",
                     "set_pdf_separate_window"):
            setattr(self, name, APP_NS[name].__get__(self))

    def set_case_tabs_enabled(self, enabled):
        self.tabs_calls.append(enabled)


class WindowMenuTests(unittest.TestCase):
    def setUp(self):
        APP_CONFIG.clear()

    def test_the_setting_is_offered_in_the_window_menu(self):
        app, menu = _App(), _FakeMenu()
        app.populate_window_menu(menu, None)
        self.assertIn("View PDF in Separate Window", menu.labels())

    def test_it_sits_with_the_other_window_mode_choice(self):
        # Both entries decide *where* a view opens, so they belong together
        # above the separator.
        app, menu = _App(), _FakeMenu()
        app.populate_window_menu(menu, None)
        labels = menu.labels()
        self.assertEqual(
            labels[:2],
            ["Show All Windows in Tabbed View", "View PDF in Separate Window"],
        )
        self.assertLess(labels.index("View PDF in Separate Window"),
                        labels.index(None))  # the separator

    def test_the_checkbutton_tracks_the_saved_preference(self):
        app, menu = _App(separate=True), _FakeMenu()
        app.populate_window_menu(menu, None)
        entry = menu.entry("View PDF in Separate Window")
        self.assertIs(entry["variable"], app._pdf_separate_var)
        self.assertTrue(entry["variable"].get())

    def test_choosing_it_flips_the_mode_and_saves_it(self):
        app, menu = _App(), _FakeMenu()
        app.populate_window_menu(menu, None)
        app._pdf_separate_var.set(True)   # Tk sets the variable, then calls us
        menu.entry("View PDF in Separate Window")["command"]()
        self.assertTrue(app.pdf_opens_in_separate_window())
        self.assertIs(APP_CONFIG["pdf_separate_window"], True)

    def test_unticking_it_saves_the_default_back(self):
        app = _App(separate=True)
        app._pdf_separate_var.set(False)
        app.set_pdf_separate_window(False)
        self.assertFalse(app.pdf_opens_in_separate_window())
        self.assertIs(APP_CONFIG["pdf_separate_window"], False)

    def test_setting_the_mode_it_is_already_in_writes_nothing(self):
        app = _App(separate=True)
        app.set_pdf_separate_window(True)
        self.assertEqual(APP_CONFIG, {})

    def test_the_default_is_the_pdf_inside_the_opinion_window(self):
        self.assertFalse(_App().pdf_opens_in_separate_window())


# ---------------------------------------------------------------------------
# The reader hands the PDF to the floating window
# ---------------------------------------------------------------------------

READER_NS = _load(
    "_ScholarTextWindow",
    ["_pdf_opens_in_separate_window", "_show_pdf_floating", "_show_pdf",
     "_floating_pdf_closed", "_on_reader_destroyed"],
    {"_PdfPane": _FakePane,
     "_FloatingPdfWindow": _FakeFloatingWindow,
     "_is_us_reports_pdf": lambda url: "usrep" in (url or "").lower(),
     "_clamp_toplevel_to_work_area": lambda *a, **kw: (_ for _ in ()).throw(
         _InWindow()),
     },
)


class _FakeHost:
    """The reader's host: a Toplevel, or a page in the shared tab window."""

    def __init__(self, top=None):
        self._top = top or self

    def winfo_toplevel(self):
        return self._top

    def title(self, value=None):
        return "Untitled Opinion" if value is None else None


class _Reader:
    def __init__(self, separate=True, switch_target=None):
        self._app = _App(separate=separate)
        self._win = _FakeHost()
        self._pdf_url = None
        self._pdf_bytes = None
        self._pdf_located = None
        self._pdf_float_win = None
        self._active_pdf_analysis_key = None
        self._status_var = _Var("")
        self._switch_target = switch_target
        self.consumed = []
        self.refreshed = 0
        self.errors = []
        self.analysis_requests = []
        for name in ("_pdf_opens_in_separate_window", "_show_pdf_floating",
                     "_show_pdf", "_floating_pdf_closed",
                     "_on_reader_destroyed"):
            setattr(self, name, READER_NS[name].__get__(self))

    # --- collaborators the extracted methods call ---
    def _consume_pdf_switch_target(self, url):
        self.consumed.append(url)
        return self._switch_target

    def _title_citation(self):
        return "Roe v. Wade, 410 U.S. 113 (1973)"

    def _refresh_pdf_button(self):
        self.refreshed += 1

    def _on_pdf_error(self, msg):
        self.errors.append(msg)

    def _request_pdf_analysis(self, data, url, callback=None):
        self.analysis_requests.append((url, callback))
        return ("key", url)

    def _download_pdf(self):
        pass

    def _print_pdf(self, pane=None):
        pass

    def _open_pdf_cite(self, action, snippet):
        pass

    def _open_pdf_cite_browser(self, action, snippet):
        pass


class RoutingTests(unittest.TestCase):
    def setUp(self):
        _FakeFloatingWindow.opened = []

    def test_the_setting_sends_the_pdf_to_its_own_window(self):
        reader = _Reader(separate=True)
        reader._show_pdf(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(len(_FakeFloatingWindow.opened), 1)
        self.assertEqual(_FakeFloatingWindow.opened[0].url,
                         "https://example.test/a.pdf")

    def test_without_it_the_pdf_still_takes_over_the_reader(self):
        reader = _Reader(separate=False)
        # _clamp_toplevel_to_work_area is only reached by the in-window path.
        with self.assertRaises(_InWindow):
            reader._show_pdf(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(_FakeFloatingWindow.opened, [])

    def test_an_app_without_the_setting_keeps_the_old_behaviour(self):
        reader = _Reader(separate=False)
        reader._app = object()      # no pdf_opens_in_separate_window at all
        self.assertFalse(reader._pdf_opens_in_separate_window())


class FloatingHandoffTests(unittest.TestCase):
    def setUp(self):
        _FakeFloatingWindow.opened = []

    def test_the_viewer_is_told_who_to_ask_for_save_print_and_cites(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        kw = _FakeFloatingWindow.opened[0].kw
        self.assertEqual(kw["on_save"], reader._download_pdf)
        self.assertEqual(kw["on_print"], reader._print_pdf)
        self.assertEqual(kw["on_cite"], reader._open_pdf_cite)
        self.assertEqual(kw["on_close"], reader._floating_pdf_closed)

    def test_it_opens_off_the_reader_s_own_os_window(self):
        # In tabbed mode the reader is a notebook page, which cannot parent a
        # window; the shared Toplevel behind it can.
        reader = _Reader()
        top = object()
        reader._win = _FakeHost(top=top)
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertIs(_FakeFloatingWindow.opened[0].parent, top)

    def test_the_window_is_named_for_the_case(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(_FakeFloatingWindow.opened[0].title,
                         "Roe v. Wade, 410 U.S. 113 (1973)")

    def test_a_us_reports_scan_gets_the_roomier_margin(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://loc.test/usrep410113.pdf")
        self.assertEqual(_FakeFloatingWindow.opened[0].kw["margin"],
                         _FakePane._MARGIN * 3)

    def test_an_ordinary_scan_keeps_the_default_margin(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://court.test/op.pdf")
        self.assertIsNone(_FakeFloatingWindow.opened[0].kw["margin"])

    def test_the_text_view_gets_its_pdf_button_back(self):
        # The reader never left the text, so the control disabled while the
        # PDF was being fetched has to be re-enabled.
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(reader.refreshed, 1)
        self.assertTrue(reader._pdf_located)
        self.assertEqual(reader._pdf_bytes, b"%PDF-1")

    def test_the_page_opens_where_the_text_was_left(self):
        reader = _Reader(switch_target=(4, 288.0))
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(_FakeFloatingWindow.opened[0].scrolled, [(4, 288.0)])

    def test_no_alignment_means_no_jump(self):
        reader = _Reader(switch_target=None)
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(_FakeFloatingWindow.opened[0].scrolled, [])

    def test_clicking_pdf_again_resurfaces_the_one_window(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(len(_FakeFloatingWindow.opened), 1)
        win = _FakeFloatingWindow.opened[0]
        self.assertEqual(win.surfaced, 2)
        self.assertEqual(win.replaced, [])  # nothing re-rendered

    def test_another_reporter_s_scan_replaces_the_contents(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        reader._show_pdf_floating(b"%PDF-2", "https://example.test/b.pdf")
        self.assertEqual(len(_FakeFloatingWindow.opened), 1)
        self.assertEqual(_FakeFloatingWindow.opened[0].replaced,
                         ["https://example.test/b.pdf"])

    def test_a_closed_viewer_is_opened_again(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        first = _FakeFloatingWindow.opened[0]
        first.close()                      # the reader closed the window
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(len(_FakeFloatingWindow.opened), 2)
        self.assertIsNot(reader._pdf_float_win, first)

    def test_closing_the_viewer_lets_go_of_it(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        win = reader._pdf_float_win
        reader._floating_pdf_closed(win)
        self.assertIsNone(reader._pdf_float_win)

    def test_a_stale_close_does_not_drop_the_live_viewer(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        live = reader._pdf_float_win
        reader._floating_pdf_closed(object())
        self.assertIs(reader._pdf_float_win, live)

    def test_the_citation_and_search_analysis_is_routed_to_the_viewer(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        url, callback = reader.analysis_requests[0]
        self.assertEqual(url, "https://example.test/a.pdf")
        callback({"url": url, "pages": [["c"]]})
        self.assertEqual(len(_FakeFloatingWindow.opened[0].analyses), 1)
        self.assertEqual(reader._active_pdf_analysis_key,
                         ("key", "https://example.test/a.pdf"))

    def test_a_viewer_that_cannot_render_falls_back_to_the_error_path(self):
        reader = _Reader()
        with mock.patch.dict(
            READER_NS, {"_FloatingPdfWindow": mock.Mock(
                side_effect=RuntimeError("no pypdfium2"))}
        ):
            reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertEqual(reader.errors, ["no pypdfium2"])
        self.assertIsNone(reader._pdf_float_win)

    def test_closing_the_reader_closes_its_pdf_window(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        win = reader._pdf_float_win
        reader._on_reader_destroyed(mock.Mock(widget=reader._win))
        self.assertFalse(win.alive())
        self.assertIsNone(reader._pdf_float_win)

    def test_a_child_widget_going_away_leaves_the_pdf_window_alone(self):
        # <Destroy> on a window also fires for every widget inside it.
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        win = reader._pdf_float_win
        reader._on_reader_destroyed(mock.Mock(widget=object()))
        self.assertTrue(win.alive())
        self.assertIs(reader._pdf_float_win, win)


# ---------------------------------------------------------------------------
# The floating viewer itself
# ---------------------------------------------------------------------------

VIEWER_NS = _load(
    "_FloatingPdfWindow",
    ["_short_name", "showing", "apply_analysis", "zoom", "alive", "close",
     "_on_destroy", "_save", "_print"],
)


class _Viewer:
    def __init__(self, url="https://example.test/a.pdf", pane=None):
        self._url = url
        self._pane = pane if pane is not None else _FakePane()
        self._analysed_url = ""
        self._closing = False
        self._zoom_var = _Var("100%")
        self._win = mock.Mock()
        self._app = None
        self.cites = []
        self.printed = []
        self.saved = 0
        self.closed_with = []
        self._on_cite = lambda action, snippet: self.cites.append(action)
        self._on_cite_browser = lambda action, snippet: None
        self._on_save = lambda: setattr(self, "saved", self.saved + 1)
        self._on_print = self.printed.append
        self._on_close = self.closed_with.append
        for name in ("_short_name", "showing", "apply_analysis", "zoom",
                     "alive", "close", "_on_destroy", "_save", "_print"):
            setattr(self, name, VIEWER_NS[name].__get__(self))


class ShortNameTests(unittest.TestCase):
    def test_a_short_caption_is_shown_whole(self):
        self.assertEqual(VIEWER_NS["_short_name"]("Roe v. Wade"), "Roe v. Wade")

    def test_a_long_caption_is_elided_so_the_zoom_controls_keep_their_room(self):
        name = VIEWER_NS["_short_name"]("Roe v. Wade, " + "410 U.S. 113, " * 9)
        self.assertLessEqual(len(name), 58)
        self.assertTrue(name.endswith("…"))

    def test_whitespace_is_collapsed(self):
        self.assertEqual(VIEWER_NS["_short_name"]("Roe   v.\n Wade"),
                         "Roe v. Wade")

    def test_no_title_is_no_name(self):
        self.assertEqual(VIEWER_NS["_short_name"](""), "")


class ViewerTests(unittest.TestCase):
    def test_zooming_reports_the_new_level(self):
        viewer = _Viewer()
        viewer.zoom(+1)
        self.assertEqual(viewer._pane.zoomed, [1])
        self.assertEqual(viewer._zoom_var.get(), "125%")

    def test_zoom_before_a_page_is_loaded_is_harmless(self):
        viewer = _Viewer(pane=None)
        viewer._pane = None
        viewer.zoom(+1)  # must not raise
        self.assertEqual(viewer._zoom_var.get(), "100%")

    def test_it_knows_which_document_it_holds(self):
        viewer = _Viewer(url="https://example.test/a.pdf")
        self.assertTrue(viewer.showing("https://example.test/a.pdf"))
        self.assertFalse(viewer.showing("https://example.test/b.pdf"))
        self.assertFalse(viewer.showing(""))

    def test_the_analysis_makes_citations_clickable_and_the_text_searchable(self):
        viewer = _Viewer()
        viewer.apply_analysis({
            "url": viewer._url,
            "pages": [[("c", (0, 0, 1, 1))]],
            "links": {0: [((0, 0, 1, 1), ("case", "410 U.S. 113"), "snip")]},
            "quiet": {3},
        })
        links, on_left, _on_right, quiet = viewer._pane.links
        self.assertIn(0, links)
        self.assertIs(on_left, viewer._on_cite)
        self.assertEqual(quiet, {3})
        self.assertEqual(viewer._pane.find_pages, [[("c", (0, 0, 1, 1))]])
        # The viewer owns its window, so it takes the find accelerators itself.
        self.assertTrue(viewer._pane.find_bind)

    def test_a_scan_with_no_text_layer_gets_no_links_or_search(self):
        viewer = _Viewer()
        viewer.apply_analysis({"url": viewer._url, "pages": [[], []]})
        self.assertIsNone(viewer._pane.links)
        self.assertIsNone(viewer._pane.find_pages)

    def test_an_analysis_of_another_scan_is_ignored(self):
        viewer = _Viewer(url="https://example.test/b.pdf")
        viewer.apply_analysis({"url": "https://example.test/a.pdf",
                               "pages": [["c"]], "links": {0: ["x"]}})
        self.assertIsNone(viewer._pane.links)

    def test_the_analysis_is_not_applied_twice(self):
        # Re-linking re-renders every page on screen; a repeat click must not.
        viewer = _Viewer()
        result = {"url": viewer._url, "pages": [["c"]], "links": {0: ["x"]}}
        viewer.apply_analysis(result)
        viewer._pane.links = None
        viewer.apply_analysis(result)
        self.assertIsNone(viewer._pane.links)

    def test_an_analysis_arriving_after_the_window_closed_is_dropped(self):
        viewer = _Viewer()
        viewer._win.winfo_exists.return_value = False
        viewer.apply_analysis({"url": viewer._url, "pages": [["c"]],
                               "links": {0: ["x"]}})
        self.assertIsNone(viewer._pane.links)

    def test_closing_destroys_the_window_once(self):
        viewer = _Viewer()
        viewer.close()
        viewer.close()
        viewer._win.destroy.assert_called_once_with()

    def test_the_window_going_away_notifies_its_owner(self):
        viewer = _Viewer()
        viewer._on_destroy(mock.Mock(widget=viewer._win))
        self.assertEqual(viewer.closed_with, [viewer])
        self.assertIsNone(viewer._pane)

    def test_an_inner_widget_going_away_is_not_the_window_closing(self):
        viewer = _Viewer()
        pane = viewer._pane
        viewer._on_destroy(mock.Mock(widget=object()))
        self.assertEqual(viewer.closed_with, [])
        self.assertIs(viewer._pane, pane)

    def test_print_hands_over_the_rendering_on_screen(self):
        # The reader knows the filename and the redaction handling; only the
        # pane showing the pages lives here.
        viewer = _Viewer()
        viewer._print()
        self.assertEqual(viewer.printed, [viewer._pane])

    def test_save_is_delegated_to_the_reader(self):
        viewer = _Viewer()
        viewer._save()
        self.assertEqual(viewer.saved, 1)


if __name__ == "__main__":
    unittest.main()
