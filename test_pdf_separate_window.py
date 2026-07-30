"""Window ▸ "View PDF in Separate Window" and the minimal floating viewer.

The opinion window normally shows a PDF in place of its text.  With the new
Window-menu setting ticked, the "PDF" button instead hands the scan to
``_FloatingPdfWindow`` — a small Preview-style window that is nothing but the
page under a thin strip of zoom controls — and leaves the reader on the text.

Because that window has no parts panel, an opinion carrying separate writings
gets them mapped onto a slim rail beside the scrollbar instead
(``_PdfPane.set_section_marks``), so a reader dragging the thumb can see how
far down a concurrence or dissent begins.

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


def _base_ns(extra=None) -> dict:
    ns = {"tk": _Tk, "sys": sys, "re": re, "Optional": typing.Optional,
          "_CaseTabPage": type("_CaseTabPage", (), {}), "_ACCEL": "Ctrl"}
    ns.update(extra or {})
    return ns


def _load(cls: str, names, extra=None) -> dict:
    """Exec the named methods of *cls* into a namespace built from stubs."""
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(SRC, n) for n in body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found on {cls}: {missing}")
    ns = _base_ns(extra)
    for name in names:
        # A decorator (@staticmethod) is not part of the FunctionDef segment,
        # so an extracted static method is exec'd as a plain function.
        exec(found[name], ns)
    return ns


def _class_attr(cls: str, name: str):
    """The value of a simple class-level assignment, read from the source so a
    test asserts on the real thing rather than a restatement of it."""
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    for node in body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return eval(ast.get_source_segment(SRC, node.value),  # noqa: S307
                        {"frozenset": frozenset})
    raise AssertionError(f"{cls} has no {name}")


def _load_functions(names, extra=None) -> dict:
    """Exec the named module-level functions into a stub namespace."""
    found = {n.name: ast.get_source_segment(SRC, n) for n in TREE.body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"module-level functions not found: {missing}")
    ns = _base_ns(extra)
    for name in names:
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
        self.marks = None
        self.scrolled = []
        self.destroyed = False

    def set_section_marks(self, sections):
        self.marks = sections

    on_zoom = None

    def zoom(self, delta):
        self.zoomed.append(delta)
        if self.on_zoom is not None:
            self.on_zoom(self.zoom_percent())

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
     "set_pdf_separate_window", "surface_case_view"],
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
        self.root = _FakeHost()
        self._cited_pdf_windows: set = set()
        self.tabs_calls = []
        self._open_case_views: dict = {}
        for name in ("populate_window_menu", "pdf_opens_in_separate_window",
                     "set_pdf_separate_window", "surface_case_view"):
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
     "_floating_pdf_closed", "_surface_text_view", "_text_view_alive",
     "_float_pdf_master", "_float_pdf_anchor"],
    {"_PdfPane": _FakePane,
     "_FloatingPdfWindow": _FakeFloatingWindow,
     "_is_us_reports_pdf": lambda url: "usrep" in (url or "").lower(),
     "_clamp_toplevel_to_work_area": lambda *a, **kw: (_ for _ in ()).throw(
         _InWindow()),
     },
)


class _FakeHost:
    """The reader's host: a Toplevel, or a page in the shared tab window."""

    def __init__(self, top=None, alive=True):
        self._top = top or self
        self._alive = alive
        self.surfaced = 0

    def winfo_toplevel(self):
        return self._top

    def winfo_exists(self):
        return self._alive

    def title(self, value=None):
        return "Untitled Opinion" if value is None else None

    def deiconify(self):
        self.surfaced += 1

    def lift(self):
        pass

    def focus_force(self):
        pass


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
        self.reopened = 0
        self._history_reopen = self._count_reopen
        for name in ("_pdf_opens_in_separate_window", "_show_pdf_floating",
                     "_show_pdf", "_floating_pdf_closed",
                     "_surface_text_view", "_text_view_alive",
                     "_float_pdf_master", "_float_pdf_anchor"):
            setattr(self, name, READER_NS[name].__get__(self))

    # --- collaborators the extracted methods call ---
    def _consume_pdf_switch_target(self, url):
        self.consumed.append(url)
        return self._switch_target

    def _title_citation(self):
        return "Roe v. Wade, 410 U.S. 113 (1973)"

    def _history_key(self):
        return "case-key"

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

    def _count_reopen(self):
        self.reopened += 1




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
        # The case name on the strip goes back to the text this PDF came from.
        self.assertEqual(kw["on_open_text"], reader._surface_text_view)

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

    def test_the_name_on_the_strip_surfaces_the_reader_while_it_is_open(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        reader._surface_text_view()
        self.assertEqual(reader._win.surfaced, 1)
        self.assertEqual(reader.reopened, 0)

    def test_and_reopens_the_text_once_the_reader_has_been_closed(self):
        # The viewer outlives the reader, so the name has to bring the text
        # back rather than doing nothing.
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        reader._win._alive = False
        reader._surface_text_view()
        self.assertEqual(reader.reopened, 1)

    def test_it_prefers_a_window_already_showing_that_case(self):
        # Reopened from History, or a second click on the name.
        reader = _Reader()
        reader._win._alive = False
        live = _Reader()
        live._app = reader._app
        reader._app._open_case_views[id(live)] = {
            "owner": live, "view": live._win, "key": "case-key",
        }
        reader._surface_text_view()
        self.assertEqual(live._win.surfaced, 1)
        self.assertEqual(reader.reopened, 0)

    def test_a_reader_with_nothing_recorded_reopens_nothing(self):
        reader = _Reader()
        reader._win._alive = False
        reader._history_reopen = None
        reader._surface_text_view()      # must not raise

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

    def test_closing_the_reader_leaves_the_pdf_window_open(self):
        # The scan is a window in its own right; the reader going away must
        # not take it with it.
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        win = reader._pdf_float_win
        reader._win._alive = False           # the reader was closed
        self.assertTrue(win.alive())

    def test_the_viewer_is_owned_by_the_app_not_the_reader(self):
        # Tk destroys a toplevel with its master, so the master has to be
        # something that outlives this reader.
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertIs(_FakeFloatingWindow.opened[0].parent, reader._app.root)

    def test_it_still_opens_beside_the_reader(self):
        reader = _Reader()
        top = _FakeHost()
        reader._win = _FakeHost(top=top)
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        self.assertIs(_FakeFloatingWindow.opened[0].kw["anchor"], top)

    def test_the_app_holds_the_viewer_once_the_reader_cannot(self):
        reader = _Reader()
        reader._show_pdf_floating(b"%PDF-1", "https://example.test/a.pdf")
        win = reader._pdf_float_win
        self.assertIn(win, reader._app._cited_pdf_windows)
        reader._floating_pdf_closed(win)
        self.assertNotIn(win, reader._app._cited_pdf_windows)


# ---------------------------------------------------------------------------
# The floating viewer itself
# ---------------------------------------------------------------------------

VIEWER_NS = _load(
    "_FloatingPdfWindow",
    ["_short_name", "showing", "apply_analysis", "zoom", "alive", "close",
     "_on_destroy", "_save", "_print", "_show_zoom"],
)


class _Viewer:
    _SEPARATE_KINDS = _class_attr("_FloatingPdfWindow", "_SEPARATE_KINDS")

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
                     "alive", "close", "_on_destroy", "_save", "_print",
                     "_show_zoom"):
            setattr(self, name, VIEWER_NS[name].__get__(self))
        # set_pdf wires the pane's zoom reports to the strip's percentage.
        self._pane.on_zoom = self._show_zoom


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

    def test_the_level_follows_a_zoom_the_strip_did_not_ask_for(self):
        # Ctrl-wheel over the page zooms the pane directly; the percentage on
        # the strip has to follow it, not just its own buttons.
        viewer = _Viewer()
        viewer._pane.zoom(+1)
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


class SurfaceCaseViewTests(unittest.TestCase):
    """Finding a window already showing a case, before reopening one."""

    def setUp(self):
        self.app = _App()

    def _register(self, reader, key="case-key"):
        self.app._open_case_views[id(reader)] = {
            "owner": reader, "view": reader._win, "key": key,
        }
        return reader

    def test_it_surfaces_the_view_registered_under_that_key(self):
        reader = self._register(_Reader())
        self.assertTrue(self.app.surface_case_view("case-key"))
        self.assertEqual(reader._win.surfaced, 1)

    def test_a_key_nothing_is_showing_finds_nothing(self):
        self._register(_Reader())
        self.assertFalse(self.app.surface_case_view("another-case"))

    def test_no_key_finds_nothing(self):
        self.assertFalse(self.app.surface_case_view(""))

    def test_a_view_that_has_been_closed_is_passed_over(self):
        # Asking a closed reader to surface itself would send it straight back
        # here looking for one — so it is never asked.
        reader = self._register(_Reader())
        reader._win._alive = False
        self.assertFalse(self.app.surface_case_view("case-key"))
        self.assertEqual(reader._win.surfaced, 0)

    def test_something_that_is_not_a_reader_is_passed_over(self):
        self.app._open_case_views[1] = {"owner": object(), "view": object(),
                                        "key": "case-key"}
        self.assertFalse(self.app.surface_case_view("case-key"))


class StripIconTests(unittest.TestCase):
    """Save and print sit on the strip as icons, ahead of the zoom controls."""

    def setUp(self):
        body = next(
            ast.get_source_segment(SRC, node)
            for cls in TREE.body
            if isinstance(cls, ast.ClassDef) and cls.name == "_FloatingPdfWindow"
            for node in cls.body
            if isinstance(node, ast.FunctionDef) and node.name == "_build_bar"
        )
        self.body = body

    def test_the_strip_carries_a_save_and_a_print_button(self):
        self.assertIn('_ui_mini_button(bar, "", self._save', self.body)
        self.assertIn('_ui_mini_button(bar, "", self._print', self.body)

    def test_they_are_icons_not_words(self):
        self.assertIn('image=self._strip_icons["save"]', self.body)
        self.assertIn('image=self._strip_icons["print"]', self.body)

    def test_they_come_before_the_zoom_controls(self):
        self.assertLess(self.body.index("self._save"),
                        self.body.index('"−"'))
        self.assertLess(self.body.index("self._print"),
                        self.body.index('"−"'))

    def test_the_artwork_is_held_so_tk_does_not_drop_it(self):
        # An image nothing refers to is garbage collected and the button goes
        # blank.
        self.assertIn("self._strip_icons = _pdf_strip_icons(bar)", self.body)

    def test_an_icon_alone_says_what_it_does_on_hover(self):
        self.assertIn("_HoverTip(save_btn", self.body)
        self.assertIn("_HoverTip(print_btn", self.body)

    def test_save_and_print_stay_on_the_context_menu_too(self):
        # Where the accelerators are written down.
        self.assertIn("Save PDF As…", self.body)
        self.assertIn("Print…", self.body)


class SectionRailGateTests(unittest.TestCase):
    """Which documents get a rail at all — the "only then" of the request."""

    def _apply(self, sections):
        viewer = _Viewer()
        viewer.apply_analysis({
            "url": viewer._url, "pages": [["c"]], "links": {},
            "sections": sections,
        })
        return viewer._pane.marks

    @staticmethod
    def _sec(kind, page=0, label=None, start_at=None):
        return mock.Mock(kind=kind, start_page=page, start_at=start_at,
                         label=label or kind.title())

    def test_an_opinion_with_a_dissent_gets_the_rail(self):
        sections = [self._sec("majority"), self._sec("dissent", 6)]
        self.assertEqual(self._apply(sections), sections)

    def test_a_concurrence_counts_too(self):
        sections = [self._sec("majority"), self._sec("concurrence", 4)]
        self.assertEqual(self._apply(sections), sections)

    def test_an_unlabelled_separate_writing_counts_too(self):
        sections = [self._sec("majority"), self._sec("separate", 3)]
        self.assertEqual(self._apply(sections), sections)

    def test_a_syllabus_and_one_majority_get_no_rail(self):
        # Nothing to hunt for: the request was explicit that the marks show up
        # only when there are separate opinions.
        self.assertIsNone(
            self._apply([self._sec("syllabus"), self._sec("majority", 2)])
        )

    def test_a_pdf_with_no_parts_detected_gets_no_rail(self):
        self.assertIsNone(self._apply([]))

    def test_a_scan_with_no_text_layer_gets_no_rail(self):
        viewer = _Viewer()
        viewer.apply_analysis({
            "url": viewer._url, "pages": [[], []],
            "sections": [self._sec("majority"), self._sec("dissent", 2)],
        })
        self.assertIsNone(viewer._pane.marks)


# ---------------------------------------------------------------------------
# The rail itself, on the pane
# ---------------------------------------------------------------------------

WASH_NS = _load_functions(["_wash_hex"])

_PART_COLORS = {
    "syllabus": "#555555", "majority": "#1a3e72", "concurrence": "#1a7a3c",
    "dissent": "#a31515", "separate": "#59636f",
}


class _FakeCanvas:
    """The canvas surface the rail draws on (and the pane's page canvas)."""

    def __init__(self, master=None, **kw):
        self.kw = kw
        self.items = []
        self.packed = None
        self.bindings = {}
        self.destroyed = False
        self.height = 800
        self.width = 620
        self.pointer = (6, 0)
        self.view = 0.0
        self.region_w = 620
        self.x = 0
        self.scrolled_y = 0

    # --- canvas drawing ---
    def delete(self, _what):
        self.items = []

    def create_line(self, *coords, **kw):
        self.items.append(("line", coords, kw))
        return len(self.items)

    def create_rectangle(self, *coords, **kw):
        self.items.append(("rect", coords, kw))
        return len(self.items)

    def find_all(self):
        return list(range(len(self.items)))

    def rects(self):
        return [(c, kw) for kind, c, kw in self.items if kind == "rect"]

    # --- widget surface ---
    def pack(self, **kw):
        self.packed = kw

    def bind(self, seq, fn, add=None):
        self.bindings[seq] = fn

    def destroy(self):
        self.destroyed = True

    def winfo_height(self):
        return self.height

    def winfo_width(self):
        return self.width

    def winfo_rootx(self):
        return 700

    def winfo_rooty(self):
        return 100

    def winfo_pointerx(self):
        return 700 + self.pointer[0]

    def winfo_pointery(self):
        return 100 + self.pointer[1]

    def yview(self):
        return (self.view, self.view + 0.1)

    def yview_moveto(self, frac):
        self.view = frac

    def yview_scroll(self, amount, _what):
        self.scrolled_y += amount

    # --- horizontal viewport over the scrollregion (xscrollincrement=1) ---
    def configure(self, **kw):
        region = kw.get("scrollregion")
        if region:
            self.region_w = region[2]
            self._clamp_x()

    def _max_x(self):
        return max(0, self.region_w - self.width)

    def _clamp_x(self):
        self.x = max(0, min(self._max_x(), self.x))

    def xview(self):
        total = max(1, self.region_w)
        return (self.x / total, min(1.0, (self.x + self.width) / total))

    def xview_moveto(self, frac):
        self.x = frac * max(1, self.region_w)
        self._clamp_x()

    def xview_scroll(self, amount, _what):
        self.x += amount
        self._clamp_x()

    def canvasx(self, screen_x):
        return self.x + screen_x


class _FakeScrollbar:
    def __init__(self):
        self.mapped = False
        self.packed = None
        self.set_to = None

    def pack(self, **kw):
        self.mapped, self.packed = True, kw

    def pack_forget(self):
        self.mapped = False

    def set(self, first, last):
        self.set_to = (first, last)


_Tk.Canvas = _FakeCanvas   # the rail builds its own canvas


PANE_NS = _load(
    "_PdfPane",
    ["set_section_marks", "has_section_marks", "_section_doc_y",
     "_draw_section_rail", "_section_at_rail_y", "_rail_tip_text",
     "_on_rail_click", "fit_to_view", "_refit_by", "_update_scrollregion",
     "_show_hsb", "_x_overflow", "_x_center", "_center_x_at", "_hwheel",
     "_shift_wheel", "_wheel", "_scroll_x_into_view", "_notify_zoom",
     "zoom_percent"],
    {"_PDF_PART_COLORS": _PART_COLORS,
     "_wash_hex": WASH_NS["_wash_hex"],
     "_HoverTip": lambda *a, **kw: None},
)


class _Pane:
    """A stand-in pane: the layout numbers the rail reads, and stubs for the
    scrolling and re-layout it drives."""

    _PAD = 12
    _RAIL_W = 13
    _RAIL_TICK_H = 3
    _RAIL_BG = "#f0f1f3"
    _RAIL_EDGE = "#cdd0d5"
    _RAIL_BAND_WASH = 0.80
    _ZOOM_MIN_W = 240
    _ZOOM_MAX_W = 3200
    _AUTOFIT_SLOP = 3
    _SCROLL_PX = 60

    def __init__(self, pages=10, page_h=100, cap=None):
        # A ten-page document: page i occupies [i*100, i*100 + 88].
        self._slots = [(i * page_h, page_h - _Pane._PAD, (0, 0, 1, 1), 1.0)
                       for i in range(pages)]
        self._content_h = pages * page_h
        self._rail = None
        self._sections = []
        self._rail_spans = []
        self._base_w = 600
        self._target_w = 600
        self._fit_cap = cap          # None = autofit (fill the room available)
        self.zooms_reported = []
        self._on_zoom = self.zooms_reported.append
        self._canvas = _FakeCanvas()
        self._vsb = object()
        self._body = object()
        self._hsb = _FakeScrollbar()
        self._hsb_on = False
        self.scrolled = []
        self.layouts = 0
        self.renders = 0
        self.idle = []
        for name in ("set_section_marks", "has_section_marks",
                     "_section_doc_y", "_draw_section_rail",
                     "_section_at_rail_y", "_rail_tip_text", "_on_rail_click",
                     "fit_to_view", "_refit_by", "_update_scrollregion",
                     "_show_hsb", "_x_overflow", "_x_center", "_center_x_at",
                     "_hwheel", "_shift_wheel", "_wheel",
                     "_scroll_x_into_view", "_notify_zoom", "zoom_percent"):
            setattr(self, name, PANE_NS[name].__get__(self))

    # --- what the extracted methods call back into ---
    def scroll_to_page(self, i, y_pt=None):
        self.scrolled.append((i, y_pt))

    def _page_point_y(self, i, y_pt):
        # A page is 88px of content for 792pt of paper.
        return self._slots[i][0] + (y_pt / 792.0) * 88

    def _layout(self):
        self.layouts += 1

    def _render_visible(self):
        self.renders += 1

    def after_idle(self, fn, *a):
        self.idle.append((fn, a))


def _sec(kind, page, label=None, start_at=None):
    return mock.Mock(kind=kind, start_page=page, start_at=start_at,
                     label=label or f"{kind} part")


SECTIONS = [
    _sec("majority", 0, "Opinion of the Court"),
    _sec("concurrence", 4, "Stewart, J., concurring"),
    _sec("dissent", 6, "Rehnquist, J., dissenting"),
]


class RailBuildTests(unittest.TestCase):
    def setUp(self):
        self.pane = _Pane()

    def test_two_or_more_parts_raise_the_rail(self):
        self.pane.set_section_marks(SECTIONS)
        self.assertTrue(self.pane.has_section_marks())
        self.assertEqual(self.pane._rail.kw["width"], _Pane._RAIL_W)

    def test_it_is_packed_alongside_the_scrollbar(self):
        self.pane.set_section_marks(SECTIONS)
        packed = self.pane._rail.packed
        self.assertEqual(packed["side"], "right")
        self.assertEqual(packed["fill"], "y")
        # Just inside the scrollbar, so it shares the thumb's travel.
        self.assertIs(packed["after"], self.pane._vsb)

    def test_it_invites_a_click(self):
        self.pane.set_section_marks(SECTIONS)
        self.assertEqual(self.pane._rail.kw["cursor"], "hand2")
        self.assertIn("<Button-1>", self.pane._rail.bindings)

    def test_one_part_is_nothing_to_navigate_between(self):
        self.pane.set_section_marks(SECTIONS[:1])
        self.assertFalse(self.pane.has_section_marks())

    def test_no_parts_leaves_no_rail(self):
        self.pane.set_section_marks([])
        self.assertFalse(self.pane.has_section_marks())

    def test_parts_without_a_page_are_ignored(self):
        self.pane.set_section_marks(
            [_sec("majority", 0), mock.Mock(kind="dissent", start_page=None)]
        )
        self.assertFalse(self.pane.has_section_marks())

    def test_dropping_to_one_part_takes_an_existing_rail_away(self):
        self.pane.set_section_marks(SECTIONS)
        rail = self.pane._rail
        self.pane.set_section_marks(SECTIONS[:1])
        self.assertTrue(rail.destroyed)
        self.assertIsNone(self.pane._rail)
        self.assertEqual(self.pane._rail_spans, [])

    def test_the_rail_is_built_once_and_then_redrawn(self):
        self.pane.set_section_marks(SECTIONS)
        rail = self.pane._rail
        self.pane.set_section_marks(SECTIONS)
        self.assertIs(self.pane._rail, rail)
        self.assertFalse(rail.destroyed)

    def _refit_scheduled(self):
        return [fn.__name__ for fn, _a in self.pane.idle]

    def test_the_pages_are_re_fitted_to_the_column_the_rail_leaves(self):
        # A page still sized to the whole canvas would be clipped by the strip.
        self.pane.set_section_marks(SECTIONS)
        self.assertIn("fit_to_view", self._refit_scheduled())
        self.pane._canvas.width -= _Pane._RAIL_W    # Tk narrows the canvas
        self.pane.fit_to_view()
        self.assertLessEqual(self.pane._target_w + 2 * _Pane._PAD,
                             self.pane._canvas.width)

    def test_the_pages_are_re_fitted_again_when_the_rail_goes(self):
        self.pane.set_section_marks(SECTIONS)
        self.pane.idle.clear()
        self.pane.set_section_marks([])
        self.assertIn("fit_to_view", self._refit_scheduled())

    def test_a_zoomed_pane_keeps_its_zoom_when_the_rail_appears(self):
        self.pane._target_w = 900          # zoomed to 150%
        self.pane.set_section_marks(SECTIONS)
        self.pane._canvas.width -= _Pane._RAIL_W
        self.pane.fit_to_view()
        self.assertEqual(round(self.pane._target_w / self.pane._base_w, 2), 1.5)


class RailBandTests(unittest.TestCase):
    def setUp(self):
        self.pane = _Pane()
        self.pane.set_section_marks(SECTIONS)
        self.rail = self.pane._rail

    def test_each_part_covers_the_stretch_of_the_document_it_occupies(self):
        # Pages 1-4 majority, 5-6 concurrence, 7-10 dissent, of 10 pages, on
        # an 800px rail.
        spans = [(round(y0), round(y1), s.kind)
                 for y0, y1, s in self.pane._rail_spans]
        # A mark sits where scrolling to that part lands — the page top less
        # the inter-page gap (_PAD), just as scroll_to_page computes it.
        self.assertEqual(spans, [(0, 310, "majority"),
                                 (310, 470, "concurrence"),
                                 (470, 800, "dissent")])

    def test_the_bands_tile_the_whole_rail(self):
        spans = self.pane._rail_spans
        self.assertEqual(spans[0][0], 0)
        self.assertEqual(spans[-1][1], self.rail.height)
        for a, b in zip(spans, spans[1:]):
            self.assertAlmostEqual(a[1], b[0])

    def test_a_part_is_drawn_as_a_washed_band_under_a_solid_marker(self):
        rects = self.rail.rects()
        self.assertEqual(len(rects), 2 * len(SECTIONS))
        band, tick = rects[0], rects[1]
        self.assertEqual(tick[1]["fill"], _PART_COLORS["majority"])
        self.assertNotEqual(band[1]["fill"], tick[1]["fill"])
        # The marker sits on the part's first line, a few pixels tall.
        self.assertEqual(tick[0][1], 0)
        self.assertEqual(tick[0][3], _Pane._RAIL_TICK_H)

    def test_each_kind_keeps_its_own_color(self):
        ticks = [kw["fill"] for _c, kw in self.rail.rects()[1::2]]
        self.assertEqual(ticks, [_PART_COLORS["majority"],
                                 _PART_COLORS["concurrence"],
                                 _PART_COLORS["dissent"]])

    def test_a_part_opening_partway_down_a_page_is_marked_there(self):
        mid = _sec("dissent", 6, start_at=(6, 396.0))  # halfway down page 7
        self.pane.set_section_marks([SECTIONS[0], mid])
        start = self.pane._rail_spans[1][0]
        top_of_page = 600 / self.pane._content_h * self.rail.height
        self.assertGreater(start, top_of_page)

    def test_a_relayout_redraws_the_bands(self):
        # A zoom rebuilds the layout and moves every part.
        self.rail.height = 400
        self.pane._draw_section_rail()
        self.assertEqual(self.pane._rail_spans[-1][1], 400)

    def test_a_rail_with_no_layout_yet_draws_nothing(self):
        self.pane._slots = []
        self.pane._draw_section_rail()
        self.assertEqual(self.pane._rail_spans, [])
        self.assertEqual(self.rail.items, [])


class RailNavigationTests(unittest.TestCase):
    def setUp(self):
        self.pane = _Pane()
        self.pane.set_section_marks(SECTIONS)
        self.rail = self.pane._rail

    def test_a_height_names_the_part_whose_band_covers_it(self):
        kinds = [self.pane._section_at_rail_y(y).kind
                 for y in (10, 309, 311, 469, 471, 799)]
        self.assertEqual(kinds, ["majority", "majority", "concurrence",
                                 "concurrence", "dissent", "dissent"])

    def test_a_height_off_either_end_clamps(self):
        self.assertEqual(self.pane._section_at_rail_y(-40).kind, "majority")
        self.assertEqual(self.pane._section_at_rail_y(9000).kind, "dissent")

    def test_an_empty_rail_names_nothing(self):
        self.pane._rail_spans = []
        self.assertIsNone(self.pane._section_at_rail_y(100))

    def test_clicking_a_band_scrolls_to_that_part(self):
        self.pane._on_rail_click(mock.Mock(y=500))
        self.assertEqual(self.pane.scrolled, [(6, None)])

    def test_clicking_the_top_band_goes_back_to_the_opinion(self):
        self.pane._on_rail_click(mock.Mock(y=2))
        self.assertEqual(self.pane.scrolled, [(0, None)])

    def test_a_part_opening_mid_page_is_jumped_to_by_its_first_line(self):
        mid = _sec("dissent", 6, start_at=(6, 396.0))
        self.pane.set_section_marks([SECTIONS[0], mid])
        self.pane._on_rail_click(mock.Mock(y=799))
        self.assertEqual(self.pane.scrolled, [(6, 396.0)])

    def test_the_hover_tip_names_the_part_under_the_pointer(self):
        self.rail.pointer = (6, 500)
        self.assertEqual(self.pane._rail_tip_text(),
                         "Rehnquist, J., dissenting — p. 7")

    def test_the_tip_counts_pages_from_one(self):
        self.rail.pointer = (6, 10)
        self.assertTrue(self.pane._rail_tip_text().endswith("p. 1"))

    def test_a_pointer_off_the_rail_gets_no_tip(self):
        self.rail.pointer = (6, -30)      # left without a <Leave>
        self.assertEqual(self.pane._rail_tip_text(), "")

    def test_no_rail_means_no_tip(self):
        self.pane._rail = None
        self.assertEqual(self.pane._rail_tip_text(), "")


class FitToViewTests(unittest.TestCase):
    """The pages stay fitted to the room they actually have."""

    def setUp(self):
        self.pane = _Pane()

    def test_it_measures_the_canvas_the_pages_live_in(self):
        self.pane._canvas.width = 500
        self.pane.fit_to_view()
        self.assertEqual(self.pane._base_w, 500 - 2 * _Pane._PAD)
        self.assertEqual(self.pane.layouts, 1)

    def test_a_fit_that_is_already_right_re_lays_out_nothing(self):
        self.pane._canvas.width = 600 + 2 * _Pane._PAD
        self.pane.fit_to_view()
        self.assertEqual(self.pane.layouts, 0)

    def test_a_hairline_change_is_not_worth_a_re_render(self):
        self.pane._canvas.width = 600 + 2 * _Pane._PAD + _Pane._AUTOFIT_SLOP
        self.pane.fit_to_view()
        self.assertEqual(self.pane.layouts, 0)

    def test_nothing_is_measured_before_the_pane_is_laid_out(self):
        self.pane._canvas.width = 1
        self.pane.fit_to_view()
        self.assertEqual(self.pane._base_w, 600)
        self.assertEqual(self.pane.layouts, 0)

    def test_the_zoom_ratio_survives_a_refit(self):
        self.pane._target_w = 750         # zoomed to 125%
        self.pane._canvas.width = 424     # → fit width 400
        self.pane.fit_to_view()
        self.assertEqual(self.pane._base_w, 400)
        self.assertEqual(self.pane._target_w, 500)

    def test_the_reading_position_survives_a_refit(self):
        self.pane._canvas.view = 0.42
        self.pane._canvas.width = 500
        self.pane.fit_to_view()
        self.assertAlmostEqual(self.pane._canvas.view, 0.42)

    def test_a_refit_never_goes_below_the_minimum_page_width(self):
        self.pane._canvas.width = 40
        self.pane.fit_to_view()
        self.assertEqual(self.pane._base_w, _Pane._ZOOM_MIN_W)

    def test_a_dead_pane_measures_nothing(self):
        self.pane._canvas.winfo_width = mock.Mock(side_effect=Exception("gone"))
        self.pane.fit_to_view()   # must not raise
        self.assertEqual(self.pane.layouts, 0)


class HorizontalScrollTests(unittest.TestCase):
    """A page zoomed past the width of its window has to be reachable."""

    def setUp(self):
        self.pane = _Pane()
        self.canvas = self.pane._canvas
        self.canvas.width = 620

    def _zoom_to(self, target_w):
        self.pane._target_w = target_w
        self.pane._update_scrollregion()

    def test_a_page_that_fits_shows_no_scrollbar(self):
        self._zoom_to(500)
        self.assertFalse(self.pane._hsb_on)
        self.assertFalse(self.pane._hsb.mapped)

    def test_a_page_wider_than_the_window_raises_one(self):
        self._zoom_to(900)
        self.assertTrue(self.pane._hsb_on)
        self.assertTrue(self.pane._hsb.mapped)

    def test_the_scrollbar_meets_the_vertical_one_in_the_corner(self):
        self._zoom_to(900)
        packed = self.pane._hsb.packed
        self.assertEqual(packed["side"], "bottom")
        self.assertEqual(packed["fill"], "x")
        # Before the canvas, so neither scrollbar runs under the other.
        self.assertIs(packed["before"], self.canvas)

    def test_the_whole_page_is_inside_the_scrollregion(self):
        self._zoom_to(900)
        self.assertEqual(self.canvas.region_w, 900 + 2 * _Pane._PAD)

    def test_a_narrower_page_still_spans_the_viewport(self):
        # Otherwise a centred page would jitter under spurious scrolling.
        self._zoom_to(400)
        self.assertEqual(self.canvas.region_w, 620)

    def test_zooming_back_in_takes_the_scrollbar_away_and_pans_home(self):
        self._zoom_to(900)
        self.pane._hwheel(1)
        self.assertGreater(self.canvas.x, 0)
        self._zoom_to(500)
        self.assertFalse(self.pane._hsb.mapped)
        self.assertEqual(self.canvas.x, 0)   # nothing left to pan to

    def test_the_far_edge_of_a_zoomed_page_can_be_reached(self):
        self._zoom_to(900)
        self.canvas.xview_moveto(1.0)
        page_right = _Pane._PAD + 900
        self.assertLessEqual(page_right, self.canvas.canvasx(0) + 620 + 1)

    def test_panning_moves_one_scroll_step(self):
        self._zoom_to(900)
        self.pane._hwheel(1)
        self.assertEqual(self.canvas.x, _Pane._SCROLL_PX)
        self.pane._hwheel(-1)
        self.assertEqual(self.canvas.x, 0)

    def test_panning_a_page_that_fits_does_nothing_at_all(self):
        self._zoom_to(500)
        self.assertIsNone(self.pane._hwheel(1))
        self.assertEqual(self.canvas.x, 0)

    def test_panning_stops_at_the_edges(self):
        self._zoom_to(900)
        for _ in range(40):
            self.pane._hwheel(1)
        self.assertEqual(self.canvas.x, 900 + 2 * _Pane._PAD - 620)
        for _ in range(40):
            self.pane._hwheel(-1)
        self.assertEqual(self.canvas.x, 0)

    def test_shift_wheel_pans_a_zoomed_page(self):
        self._zoom_to(900)
        self.assertEqual(self.pane._shift_wheel(1), "break")
        self.assertEqual(self.canvas.x, _Pane._SCROLL_PX)

    def test_shift_wheel_still_scrolls_when_there_is_nothing_to_pan(self):
        # It scrolled the document before there was anything to pan; it must
        # not become a dead key on a page that fits.
        self._zoom_to(500)
        self.pane._shift_wheel(1)
        self.assertEqual(self.canvas.x, 0)
        self.assertGreater(self.canvas.scrolled_y, 0)

    def test_a_zoom_keeps_the_same_column_in_the_middle(self):
        self._zoom_to(900)
        self.pane._center_x_at(0.5)
        self.assertAlmostEqual(self.pane._x_center(), 0.5, places=2)

    def test_centring_clamps_at_the_ends(self):
        self._zoom_to(900)
        self.pane._center_x_at(0.0)
        self.assertEqual(self.canvas.x, 0)
        self.pane._center_x_at(1.0)
        self.assertAlmostEqual(self.canvas.x, 900 + 2 * _Pane._PAD - 620,
                               delta=1)

    def test_a_page_that_fits_is_centred_on_its_middle(self):
        self._zoom_to(400)
        self.assertAlmostEqual(self.pane._x_center(), 0.5, places=2)


class ScrollIntoViewTests(unittest.TestCase):
    """A find match on a zoomed page is panned to, not just scrolled to."""

    def setUp(self):
        self.pane = _Pane()
        self.canvas = self.pane._canvas
        self.canvas.width = 620
        self.pane._target_w = 1200
        self.pane._update_scrollregion()

    def test_a_match_already_on_screen_is_left_alone(self):
        self.pane._scroll_x_into_view(100, 200)
        self.assertEqual(self.canvas.x, 0)

    def test_a_match_off_to_the_right_is_brought_in(self):
        self.pane._scroll_x_into_view(900, 1000)
        left = self.canvas.canvasx(0)
        self.assertLessEqual(1000, left + 620)
        self.assertLessEqual(left, 900)

    def test_a_match_off_to_the_left_is_brought_in(self):
        self.canvas.xview_moveto(1.0)
        self.pane._scroll_x_into_view(50, 120)
        self.assertLessEqual(self.canvas.canvasx(0), 50)

    def test_it_pans_the_least_it_can(self):
        # Coming from the left, a match just off the right edge should end up
        # at the right of the view, not centred or flush left.
        self.pane._scroll_x_into_view(700, 760)
        self.assertAlmostEqual(self.canvas.canvasx(0) + 620,
                               760 + _Pane._PAD, delta=1)

    def test_a_match_wider_than_the_view_shows_its_start(self):
        self.pane._scroll_x_into_view(300, 1100)
        self.assertAlmostEqual(self.canvas.canvasx(0), 300 - _Pane._PAD,
                               delta=1)

    def test_nothing_moves_on_a_page_that_fits(self):
        self.pane._target_w = 400
        self.pane._update_scrollregion()
        self.pane._scroll_x_into_view(300, 380)
        self.assertEqual(self.canvas.x, 0)


class WashTests(unittest.TestCase):
    def test_a_color_washes_toward_white(self):
        wash = WASH_NS["_wash_hex"]
        self.assertEqual(wash("#000000", 0.0), "#000000")
        self.assertEqual(wash("#000000", 1.0), "#ffffff")
        self.assertEqual(wash("#a31515", 0.80), "#edd0d0")

    def test_a_wash_is_lighter_than_what_it_came_from(self):
        wash = WASH_NS["_wash_hex"]
        for color in ("#1a3e72", "#1a7a3c", "#a31515", "#59636f"):
            washed = wash(color, 0.80)
            self.assertGreater(int(washed[1:3], 16), int(color[1:3], 16))

    def test_an_unreadable_color_falls_back_to_grey(self):
        wash = WASH_NS["_wash_hex"]
        self.assertEqual(wash("", 0.8), "#eeeeee")
        self.assertEqual(wash("transparent", 0.8), "#eeeeee")

    def test_the_wash_is_clamped(self):
        wash = WASH_NS["_wash_hex"]
        self.assertEqual(wash("#808080", -5), "#808080")
        self.assertEqual(wash("#808080", 9), "#ffffff")


if __name__ == "__main__":
    unittest.main()
