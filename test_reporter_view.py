"""Reporter View: the interface that answers with the report itself.

Interface ▸ *Reporter View* changes what opening a document means.  A case
opens as its scan, in the small floating viewer, with T turning it into the
opinion text; a case with no scan anywhere opens in that same window at that
same size, on the text side, rather than as a different kind of window.  A
source printed only as pages — Statutes at Large, the English Reports — opens
there too, without the switch, because there is nothing to switch to.  And a
citation followed out of a search result, a brief or the text side is looked
up as a scan first, exactly as one clicked on a page already was.

As elsewhere in this suite the methods are lifted out of
``courtlistener_gui`` with ``ast`` (importing it needs tkinter, absent on a
headless run) and driven against stubs.
"""

import ast
import pathlib
import re
import sys
import typing
import unittest


SRC = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
TREE = ast.parse(SRC)


class _Tk:
    TclError = Exception
    Menu = Misc = Frame = Toplevel = object


def _base_ns(extra=None) -> dict:
    ns = {"tk": _Tk, "sys": sys, "re": re, "Optional": typing.Optional,
          "threading": _Threading, "print": lambda *a, **k: None,
          "_INTERFACE_MODES": _module_value("_INTERFACE_MODES"),
          "_DEFAULT_INTERFACE_MODE": _module_value(
              "_DEFAULT_INTERFACE_MODE")}
    ns.update(extra or {})
    return ns


class _Threading:
    """Runs a worker inline, so a test sees the whole path in one call."""

    started: list = []

    class Thread:
        def __init__(self, target=None, daemon=False, **kw):
            self._target = target

        def start(self):
            _Threading.started.append(self._target)
            if self._target is not None:
                self._target()


def _load(cls: str, names, extra=None) -> dict:
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(SRC, n) for n in body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found on {cls}: {missing}")
    ns = _base_ns(extra)
    for name in names:
        exec(found[name], ns)
    return ns


def _load_functions(names, extra=None) -> dict:
    found = {n.name: ast.get_source_segment(SRC, n) for n in TREE.body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"module-level functions not found: {missing}")
    ns = _base_ns(extra)
    for name in names:
        exec(found[name], ns)
    return ns


def _source_of(cls: str, name: str) -> str:
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    for node in body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(SRC, node)
    raise AssertionError(f"{cls} has no {name}")


def _module_value(name: str):
    """A module-level constant, evaluated from the source."""
    for node in TREE.body:
        targets = (
            node.targets if isinstance(node, ast.Assign)
            else [node.target] if isinstance(node, ast.AnnAssign) else []
        )
        if any(isinstance(t, ast.Name) and t.id == name for t in targets):
            return eval(ast.get_source_segment(SRC, node.value),  # noqa: S307
                        {"frozenset": frozenset})
    raise AssertionError(f"module-level constant not found: {name}")


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _Widget:
    def __init__(self, top=None):
        self._top = top
        self.destroyed = False
        self.shown = False

    def winfo_exists(self):
        return not self.destroyed

    def winfo_toplevel(self):
        return self._top if self._top is not None else self

    def destroy(self):
        self.destroyed = True

    def deiconify(self):
        self.shown = True

    def after(self, _ms, fn=None, *args):
        if fn is not None:
            fn(*args)


class _Var:
    def __init__(self, value=""):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


# ---------------------------------------------------------------------------
# PDF-first: a citation is looked up as a scan before it is looked up as text
# ---------------------------------------------------------------------------

APP_NAMES = ["open_case_pdf_first", "reporter_open_case",
             "pdf_opens_in_separate_window", "interface_mode",
             "_safe_root_status"]

APP_NS = _load(
    "CourtListenerGUI", APP_NAMES,
    {"_pick_citation": lambda cites: (cites[0] if cites else "")},
)


class _App:
    def __init__(self, mode="reporter", opens=True):
        self._interface_mode = mode
        self.root = _Widget()
        self.asked: list = []
        self.opens = opens
        self._status_var = _Var()
        for name in APP_NAMES:
            setattr(self, name, APP_NS[name].__get__(self))

    def open_cited_case_pdf(self, parent, action, snippet="",
                            status=lambda _s: None, fallback=None):
        self.asked.append((parent, action, snippet, fallback))
        return self.opens


class PdfFirstTests(unittest.TestCase):
    def test_the_reporter_interface_looks_for_the_scan(self):
        app = _App()
        self.assertTrue(
            app.open_case_pdf_first(app.root, ("cite", "410 U.S. 113")))
        self.assertEqual(app.asked[0][1], ("cite", "410 U.S. 113"))

    def test_the_other_two_do_not(self):
        for mode in ("windows", "tabs"):
            app = _App(mode=mode)
            self.assertFalse(
                app.open_case_pdf_first(app.root, ("cite", "410 U.S. 113")))
            self.assertEqual(app.asked, [])

    def test_a_window_popped_out_of_a_scan_does_though(self):
        app = _App(mode="tabs")
        popped = _Widget()
        popped._reporter_window = True
        self.assertTrue(
            app.open_case_pdf_first(popped, ("cite", "410 U.S. 113")))

    def test_the_caller_s_own_way_of_opening_it_is_the_fallback(self):
        app = _App()
        marker = []
        app.open_case_pdf_first(app.root, ("cite", "1 U.S. 1"),
                                fallback=lambda: marker.append(1))
        app.asked[0][3]()
        self.assertEqual(marker, [1])

    def test_a_lookup_that_cannot_start_leaves_the_caller_to_it(self):
        app = _App(opens=False)
        self.assertFalse(
            app.open_case_pdf_first(app.root, ("cite", "1 U.S. 1")))


class ReporterOpenCaseTests(unittest.TestCase):
    """What a search result means by "open this case"."""

    def test_a_result_is_opened_by_its_citation(self):
        app = _App()
        item = {"citation": ["410 U.S. 113"], "caseName": "Roe v. Wade"}
        self.assertTrue(app.reporter_open_case(app.root, item))
        _parent, action, snippet, _fb = app.asked[0]
        self.assertEqual(action, ("cite", "410 U.S. 113"))
        self.assertEqual(snippet, "Roe v. Wade")

    def test_an_explicit_citation_wins_over_the_item_s(self):
        app = _App()
        app.reporter_open_case(app.root, {"citation": ["1 U.S. 1"]},
                               cite="410 U.S. 113", name="Roe")
        self.assertEqual(app.asked[0][1], ("cite", "410 U.S. 113"))

    def test_a_result_with_no_citation_is_left_to_the_text_path(self):
        app = _App()
        self.assertFalse(app.reporter_open_case(app.root, {"caseName": "X"}))
        self.assertEqual(app.asked, [])

    def test_no_parent_means_the_main_window(self):
        app = _App()
        app.reporter_open_case(None, cite="410 U.S. 113")
        self.assertIs(app.asked[0][0], app.root)

    def test_the_other_interfaces_open_a_result_as_they_always_did(self):
        app = _App(mode="windows")
        self.assertFalse(app.reporter_open_case(app.root, cite="410 U.S. 113"))


class WhereItIsAskedTests(unittest.TestCase):
    """Every way of opening a case asks the same question first."""

    def test_the_main_window_s_search_results_do(self):
        src = _source_of("CourtListenerGUI", "_fetch_scholar_text")
        self.assertIn("self.reporter_open_case(self.root, item", src)
        self.assertIn("fallback=ordinary", src)

    def test_and_so_does_a_brief_s_citation(self):
        src = next(ast.get_source_segment(SRC, n) for n in TREE.body
                   if isinstance(n, ast.FunctionDef)
                   and n.name == "_follow_brief_action")
        self.assertIn("open_case_pdf_first", src)
        self.assertIn("_following_as_text", src)

    def test_and_a_link_clicked_in_the_opinion_text(self):
        src = _source_of("_ScholarTextWindow", "_follow_link")
        self.assertIn("self._app.open_case_pdf_first(", src)
        self.assertIn("fallback=as_text", src)

    def test_but_federal_appendix_keeps_its_own_scan_route(self):
        src = _source_of("_ScholarTextWindow", "_follow_link")
        self.assertIn("not _FED_APPX_RE.search(cite)", src)

    def test_and_a_citation_re_followed_as_text_is_not_asked_twice(self):
        src = _source_of("_ScholarTextWindow", "_follow_link")
        self.assertIn("not self._following_as_text", src)
        self.assertIn("self._following_as_text = True", src)


# ---------------------------------------------------------------------------
# A case with no scan: the same window, on the text side
# ---------------------------------------------------------------------------

HOST_NS = _load(
    "CourtListenerGUI", ["new_case_view_host", "_reporter_text_host"],
    {"_FloatingPdfWindow": lambda *a, **kw: _StubViewer(*a, **kw)},
)


class _StubViewer:
    made: list = []

    def __init__(self, parent, data, url, title, **kw):  # noqa: D107
        self.parent, self.data, self.kw = parent, data, kw
        self.host = object()
        _StubViewer.made.append(self)

    def text_host(self, standalone=True):
        self.standalone = standalone
        return self.host


class _HostApp:
    def __init__(self, mode="reporter"):
        self._interface_mode = mode
        self.root = _Widget()
        self._cited_pdf_windows: set = set()
        self.secondary: list = []
        for name in ("new_case_view_host", "_reporter_text_host"):
            setattr(self, name, HOST_NS[name].__get__(self))
        self.pdf_opens_in_separate_window = APP_NS[
            "pdf_opens_in_separate_window"].__get__(self)
        self.interface_mode = APP_NS["interface_mode"].__get__(self)

    def new_secondary_view_host(self, parent):
        self.secondary.append(parent)
        return "ordinary host"

    def _cited_pdf_window_closed(self, window):
        self._cited_pdf_windows.discard(window)


class TextOnlyCaseTests(unittest.TestCase):
    def setUp(self):
        _StubViewer.made.clear()

    def test_it_opens_in_a_viewer_of_its_own(self):
        app = _HostApp()
        host = app.new_case_view_host(app.root)
        self.assertEqual(len(_StubViewer.made), 1)
        self.assertIs(host, _StubViewer.made[0].host)

    def test_with_no_scan_in_it(self):
        app = _HostApp()
        app.new_case_view_host(app.root)
        self.assertIsNone(_StubViewer.made[0].data)

    def test_and_the_window_belongs_to_the_app_not_the_caller(self):
        # A viewer opened from a reader has to outlive it.
        app = _HostApp()
        app.new_case_view_host(_Widget())
        self.assertIs(_StubViewer.made[0].parent, app.root)

    def test_it_sits_beside_the_window_it_was_opened_from(self):
        app = _HostApp()
        caller = _Widget()
        app.new_case_view_host(caller)
        self.assertIs(_StubViewer.made[0].kw["anchor"], caller)

    def test_but_not_beside_the_main_window(self):
        app = _HostApp()
        app.new_case_view_host(app.root)
        self.assertIsNone(_StubViewer.made[0].kw["anchor"])

    def test_something_has_to_hold_the_window(self):
        app = _HostApp()
        app.new_case_view_host(app.root)
        self.assertIn(_StubViewer.made[0], app._cited_pdf_windows)

    def test_it_is_a_window_in_its_own_right_not_an_inset(self):
        # standalone: it records itself in History and answers to Window ▸ …
        app = _HostApp()
        app.new_case_view_host(app.root)
        self.assertTrue(_StubViewer.made[0].standalone)

    def test_the_other_interfaces_open_the_text_as_they_always_did(self):
        for mode in ("windows", "tabs"):
            app = _HostApp(mode=mode)
            self.assertEqual(app.new_case_view_host(app.root), "ordinary host")
            self.assertEqual(_StubViewer.made, [])

    def test_a_viewer_that_will_not_open_is_not_fatal(self):
        ns = _load(
            "CourtListenerGUI", ["new_case_view_host", "_reporter_text_host"],
            {"_FloatingPdfWindow": _raise},
        )
        app = _HostApp()
        for name in ("new_case_view_host", "_reporter_text_host"):
            setattr(app, name, ns[name].__get__(app))
        self.assertEqual(app.new_case_view_host(app.root), "ordinary host")


def _raise(*_a, **_kw):
    raise RuntimeError("no window")


# ---------------------------------------------------------------------------
# A source printed only as pages: no switch on the strip
# ---------------------------------------------------------------------------

VIEWER_NAMES = ["has_scan", "has_text_side", "set_text_side", "_toggle_mode",
                "showing_text", "_sync_bar", "_refresh_scale",
                "attach_scan", "no_scan_to_find", "_mode_button_tip"]


class _StubPane:
    made: list = []

    def __init__(self, body, data, **kw):
        self.body, self.data, self.kw = body, data, kw
        self.packed = False
        _StubPane.made.append(self)

    def pack(self, **kw):
        self.packed = True


VIEWER_NS = _load("_FloatingPdfWindow", VIEWER_NAMES,
                  {"_PdfPane": _StubPane})


class _Button:
    def __init__(self, text=""):
        self.text = text
        self.packed = False
        self.before = None
        self.state = "normal"

    def configure(self, **kw):
        self.text = kw.get("text", self.text)
        self.state = kw.get("state", self.state)

    def pack(self, **kw):
        self.packed = True
        self.before = kw.get("before")

    def pack_forget(self):
        self.packed = False


class _Viewer:
    _W = 720
    _MIN_W = 380

    def __init__(self, scan=True, build_text=None, reader=None):
        self._pane = object() if scan else None
        self._bytes = b"%PDF" if scan else None
        self._scan_search = True if scan else None
        self._on_save = self._on_print = None
        self._on_cite = self._on_cite_browser = None
        self._url = ""
        self._analysed_url = "x"
        self._body = object()
        self._win = _Widget()
        self.titles: list = []
        self._mode = "pdf"
        self._reader = reader
        self._on_build_text = build_text
        self._text_host = None
        self._flash_after = None
        self._zoom_var = _Var()
        self._mode_btn = _Button("T")
        self._mode_btn.packed = True
        self._fit_btn = _Button("Fit")
        self._fit_btn.packed = True
        self._copy_btn = _Button("Copy ▾")
        self._zoom_label = object()
        self.switched = []
        for name in VIEWER_NAMES:
            setattr(self, name, VIEWER_NS[name].__get__(self))

    def set_title(self, title):
        self.titles.append(title)

    def _show_zoom(self, *a, **kw):
        pass

    # what _toggle_mode calls once it has decided to switch
    def _show_text(self):
        self.switched.append("text")

    def _show_scan(self):
        self.switched.append("scan")


class NoTextSideTests(unittest.TestCase):
    def test_a_scan_with_an_opinion_behind_it_offers_the_switch(self):
        viewer = _Viewer(build_text=lambda host: object())
        self.assertTrue(viewer.has_text_side())
        viewer._sync_bar()
        self.assertTrue(viewer._mode_btn.packed)

    def test_so_does_one_already_showing_the_opinion(self):
        self.assertTrue(_Viewer(reader=object()).has_text_side())

    def test_but_statutes_at_large_has_none(self):
        viewer = _Viewer()
        self.assertFalse(viewer.has_text_side())
        viewer._sync_bar()
        self.assertFalse(viewer._mode_btn.packed)

    def test_and_pressing_it_anyway_does_nothing(self):
        viewer = _Viewer()
        viewer._toggle_mode()
        self.assertEqual(viewer.switched, [])

    def test_fit_still_belongs_to_the_pages(self):
        viewer = _Viewer()
        viewer._sync_bar()
        self.assertTrue(viewer._fit_btn.packed)
        self.assertFalse(viewer._copy_btn.packed)

    def test_a_lookup_that_came_back_empty_takes_the_switch_off(self):
        viewer = _Viewer(build_text=lambda host: object())
        viewer._sync_bar()
        self.assertTrue(viewer._mode_btn.packed)
        viewer.set_text_side(False)
        self.assertFalse(viewer._mode_btn.packed)
        self.assertIsNone(viewer._on_build_text)

    def test_a_lookup_that_found_something_leaves_it_alone(self):
        viewer = _Viewer(build_text=lambda host: object())
        viewer.set_text_side(True)
        self.assertIsNotNone(viewer._on_build_text)

    def test_a_window_that_is_only_text_is_not_touched_by_that(self):
        # No scan at all: the switch is greyed, for the other reason.
        viewer = _Viewer(scan=False, reader=object())
        viewer.set_text_side(False)
        self.assertTrue(viewer.has_text_side())


class ScanStillComingTests(unittest.TestCase):
    """A case that opened on the text keeps P on the strip, greyed, while the
    scan is looked for behind it — and comes alive if one turns up."""

    def setUp(self):
        _StubPane.made.clear()

    def _text_only(self):
        # As adopt_reader leaves it: the opinion is the surface on screen.
        viewer = _Viewer(scan=False, reader=object())
        viewer._mode = "text"
        return viewer

    def test_p_is_on_the_strip_from_the_start(self):
        viewer = self._text_only()
        viewer._sync_bar()
        self.assertTrue(viewer._mode_btn.packed)
        self.assertEqual(viewer._mode_btn.text, "P")

    def test_but_greyed_out_while_there_is_nothing_to_show(self):
        viewer = self._text_only()
        viewer._sync_bar()
        self.assertEqual(viewer._mode_btn.state, "disabled")

    def test_and_it_says_which_it_is(self):
        viewer = self._text_only()
        self.assertIn("Looking", viewer._mode_button_tip())
        viewer.no_scan_to_find()
        self.assertIn("could be found", viewer._mode_button_tip())

    def test_a_scan_that_turns_up_is_taken(self):
        viewer = self._text_only()
        self.assertTrue(viewer.attach_scan(b"%PDF", "https://x/1.pdf"))
        self.assertTrue(viewer.has_scan())
        self.assertEqual(_StubPane.made[0].data, b"%PDF")

    def test_and_wakes_the_button_up(self):
        viewer = self._text_only()
        viewer.attach_scan(b"%PDF", "https://x/1.pdf")
        self.assertEqual(viewer._mode_btn.state, "normal")
        self.assertEqual(viewer._mode_btn.text, "P")

    def test_the_opinion_on_screen_is_left_where_it_is(self):
        viewer = self._text_only()
        viewer.attach_scan(b"%PDF", "https://x/1.pdf")
        self.assertTrue(viewer.showing_text())
        self.assertFalse(_StubPane.made[0].packed)   # built, not shown

    def test_and_the_window_takes_the_name_the_pages_carry(self):
        viewer = self._text_only()
        viewer.attach_scan(b"%PDF", "https://x/1.pdf", "Roe v. Wade, 410 U.S. 113")
        self.assertEqual(viewer.titles, ["Roe v. Wade, 410 U.S. 113"])

    def test_a_window_already_showing_pages_keeps_them(self):
        viewer = _Viewer(scan=True, build_text=lambda h: object())
        self.assertFalse(viewer.attach_scan(b"%PDF2", "https://x/2.pdf"))
        self.assertEqual(_StubPane.made, [])

    def test_an_empty_search_leaves_the_button_greyed(self):
        viewer = self._text_only()
        viewer.no_scan_to_find()
        viewer._sync_bar()
        self.assertTrue(viewer._mode_btn.packed)
        self.assertEqual(viewer._mode_btn.state, "disabled")

    def test_the_reader_behind_it_is_the_one_that_looks(self):
        src = _source_of("_ScholarTextWindow", "_offer_scan_to_host")
        self.assertIn("if not self._standalone_embed:", src)
        self.assertIn("window.no_scan_to_find()", src)
        self.assertIn("data, url, self._scan_window_title(url), "
                      "margin=margin,", src)

    def test_and_the_pages_own_links_follow_when_they_are_read(self):
        src = _source_of("_ScholarTextWindow", "_offer_scan_to_host")
        self.assertIn("self._request_pdf_analysis(", src)
        self.assertIn("self._apply_host_analysis(", src)

    def test_the_handlers_come_across_with_the_pages(self):
        # Without them a citation on the page clicks into nothing: the window
        # was built around a case with no scan, so it has no scan handlers.
        viewer = self._text_only()
        self.assertIsNone(viewer._on_cite)
        marks: list = []
        viewer.attach_scan(
            b"%PDF", "https://x/1.pdf",
            on_cite=lambda a, s: marks.append(("cite", a)),
            on_cite_browser=lambda a, s: marks.append(("browser", a)),
            on_save=lambda pane: marks.append("save"),
            on_print=lambda pane: marks.append("print"))
        viewer._on_cite(("cite", "1 U.S. 1"), "")
        viewer._on_cite_browser(("cite", "1 U.S. 1"), "")
        viewer._on_save(None)
        viewer._on_print(None)
        self.assertEqual(marks, [("cite", ("cite", "1 U.S. 1")),
                                 ("browser", ("cite", "1 U.S. 1")),
                                 "save", "print"])

    def test_a_window_that_already_had_them_keeps_its_own(self):
        viewer = self._text_only()
        viewer._on_cite = "original"
        viewer.attach_scan(b"%PDF", "https://x/1.pdf")
        self.assertEqual(viewer._on_cite, "original")

    def test_the_reader_hands_over_the_ones_it_uses_for_its_own_scans(self):
        src = _source_of("_ScholarTextWindow", "_offer_scan_to_host")
        for handler in ("on_save=self._download_pdf",
                        "on_print=self._print_pdf",
                        "on_cite=self._open_pdf_cite",
                        "on_cite_browser=self._open_pdf_cite_browser"):
            self.assertIn(handler, src)

    def test_and_keeps_the_scan_those_handlers_act_on(self):
        # _download_pdf and _print_pdf read the bytes off the reader.
        src = _source_of("_ScholarTextWindow", "_offer_scan_to_host")
        self.assertIn("self._pdf_bytes = data", src)


class ScanTitleTests(unittest.TestCase):
    """The window takes the case's Bluebook citation once the text says what
    it is — cited to the reporter the pages on screen actually print."""

    def test_a_cited_scan_is_renamed_from_the_text_found_for_it(self):
        src = _source_of("CourtListenerGUI", "_cited_filename_item")
        self.assertIn("_scan_citation_item(item, named.get(\"url\") or \"\")",
                      src)

    def test_and_so_is_one_the_courier_went_looking_for(self):
        src = _source_of("_PdfWindow", "_retitle_from_text")
        self.assertIn("_scan_citation_item(item, self._url)", src)
        self.assertIn("self._float.set_title(title)", src)
        discovered = _source_of("_PdfWindow", "_on_text_discovered")
        self.assertIn("self._retitle_from_text(source)", discovered)

    def test_named_from_the_opinion_s_caption_not_its_docket_one(self):
        for cls, name in (("_PdfWindow", "_retitle_from_text"),
                          ("_ScholarTextWindow", "_filename_item")):
            src = _source_of(cls, name)
            self.assertIn('item.pop("case_name", None)', src)
        self.assertIn("_scholar_caption_name(list(source.blocks or ()))",
                      _source_of("_PdfWindow", "_retitle_from_text"))
        self.assertIn('name = bb.get("name") or ""',
                      _source_of("_ScholarTextWindow", "_filename_item"))

    def test_the_reporter_the_scan_names_beats_the_usual_order(self):
        src = _source_of("CourtListenerGUI", "_show_cited_case_pdf")
        self.assertIn("self._jump_to_pin(named)", src)
        name = next(ast.get_source_segment(SRC, n) for n in TREE.body
                    if isinstance(n, ast.FunctionDef)
                    and n.name == "_bluebook_display_name")
        self.assertIn('item.get("_scan_cite")', name)


# ---------------------------------------------------------------------------
# The scan window hands its pages to the viewer and steps aside
# ---------------------------------------------------------------------------

PDFWIN_NAMES = ["_hand_to_viewer", "_viewer_closed", "_dialog_parent",
                "_reveal", "_say", "_show", "_reporter_analysis"]


class _HandoffViewer:
    made: list = []

    def __init__(self, parent, data, url, title, **kw):
        self.parent, self.data, self.url, self.title, self.kw = (
            parent, data, url, title, kw)
        self._win = _Widget()
        self._pane = object()
        self.analyses: list = []
        self.surfaced = False
        _HandoffViewer.made.append(self)

    def surface(self):
        self.surfaced = True

    def apply_analysis(self, result):
        self.analyses.append(result)


def _pdfwin_ns(pages=(("some text",),)):
    return _load(
        "_PdfWindow", PDFWIN_NAMES,
        {"_FloatingPdfWindow": _HandoffViewer,
         "_PdfPane": _raise,
         "_extract_pdf_text_and_style": lambda data: (list(pages), []),
         "_citation_links_from_visible_pdf_text":
             lambda data, pages, italics: ({1: ["link"]}, set()),
         "slip_opinion": type("slip", (), {
             "detect_sections": staticmethod(lambda pages: ["section"])}),
         },
    )


PDFWIN_NS = _pdfwin_ns()


class _ScanWindow:
    def __init__(self, is_case=False, can_discover=False, reporter=True,
                 ns=None):
        ns = ns or PDFWIN_NS
        self._app = _HostApp()
        self._win = _Widget()
        self._body = _Widget()
        self._url = "https://example.test/scan.pdf"
        self._title = "5 Stat. 797"
        self._is_case = is_case
        self._can_discover_text = can_discover
        self._text_lookup_empty = False
        self._reporter = reporter
        self._reporter_anchor = None
        self._float = None
        self._pane = None
        self._bytes = None
        self._status_var = _Var()
        self._pdf_text_pages = None
        self._pdf_text_italics: list = []
        self._pdf_text_links: dict = {}
        self._pdf_quiet_pages: set = set()
        self._pdf_sections: list = []
        self.mapped = 0
        for name in PDFWIN_NAMES:
            setattr(self, name, ns[name].__get__(self))

    def _post(self, fn, *args):
        fn(*args)

    def _maybe_start_location_map(self):
        self.mapped += 1

    def _download(self, pane=None):
        pass

    def _print(self, pane=None):
        pass

    def _open_cite(self, action, snippet):
        pass

    def _open_cite_browser(self, action, snippet):
        pass

    def _embed_discovered_text(self, host):
        return None


class ScanHandoffTests(unittest.TestCase):
    def setUp(self):
        _HandoffViewer.made.clear()

    def test_the_pages_go_to_the_floating_viewer(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        self.assertEqual(len(_HandoffViewer.made), 1)
        self.assertEqual(_HandoffViewer.made[0].data, b"%PDF-1.4")

    def test_and_the_window_that_fetched_them_stays_out_of_sight(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        self.assertFalse(win._win.shown)

    def test_statutes_at_large_gets_no_switch_to_offer(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        self.assertIsNone(_HandoffViewer.made[0].kw["on_build_text"])

    def test_a_case_scan_whose_text_is_being_looked_for_does(self):
        win = _ScanWindow(is_case=True, can_discover=True)
        win._show(b"%PDF-1.4")
        self.assertIsNotNone(_HandoffViewer.made[0].kw["on_build_text"])

    def test_but_not_once_that_lookup_has_come_back_empty(self):
        win = _ScanWindow(is_case=True, can_discover=True)
        win._text_lookup_empty = True
        win._show(b"%PDF-1.4")
        self.assertIsNone(_HandoffViewer.made[0].kw["on_build_text"])

    def test_the_viewer_is_raised_when_it_opens(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        self.assertTrue(_HandoffViewer.made[0].surfaced)

    def test_something_holds_the_viewer_open(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        self.assertIn(_HandoffViewer.made[0], win._app._cited_pdf_windows)

    def test_closing_the_viewer_closes_the_courier_behind_it(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        _HandoffViewer.made[0].kw["on_close"](_HandoffViewer.made[0])
        self.assertTrue(win._win.destroyed)

    def test_a_viewer_that_will_not_open_leaves_the_window_to_show_it(self):
        ns = _pdfwin_ns()
        ns["_FloatingPdfWindow"] = _raise
        win = _ScanWindow(ns=ns)
        # _show falls through to its own pane, which this stub refuses to build
        with self.assertRaises(Exception):
            win._show(b"%PDF-1.4")

    def test_the_other_interfaces_show_the_scan_in_the_window(self):
        win = _ScanWindow(reporter=False)
        with self.assertRaises(Exception):
            win._show(b"%PDF-1.4")
        self.assertEqual(_HandoffViewer.made, [])


class ScanTextLayerTests(unittest.TestCase):
    def setUp(self):
        _HandoffViewer.made.clear()

    def test_the_viewer_gets_the_text_layer(self):
        win = _ScanWindow(is_case=True)
        win._show(b"%PDF-1.4")
        result = _HandoffViewer.made[0].analyses[0]
        self.assertEqual(result["pages"], [("some text",)])
        self.assertEqual(result["links"], {1: ["link"]})
        self.assertEqual(result["sections"], ["section"])

    def test_and_the_window_keeps_it_for_the_opinion_to_align_against(self):
        win = _ScanWindow(is_case=True)
        win._show(b"%PDF-1.4")
        self.assertEqual(win._pdf_text_pages, [("some text",)])
        self.assertEqual(win.mapped, 1)

    def test_a_statute_scan_is_not_scanned_for_citations(self):
        win = _ScanWindow(is_case=False)
        win._show(b"%PDF-1.4")
        self.assertEqual(_HandoffViewer.made[0].analyses[0]["links"], {})

    def test_it_is_read_once_for_both(self):
        win = _ScanWindow(is_case=True)
        win._show(b"%PDF-1.4")
        self.assertEqual(len(_HandoffViewer.made[0].analyses), 1)


class ScanWindowChromeTests(unittest.TestCase):
    def setUp(self):
        _HandoffViewer.made.clear()

    def test_a_dialog_belongs_to_the_viewer_once_there_is_one(self):
        win = _ScanWindow()
        self.assertIs(win._dialog_parent(), win._win)
        win._show(b"%PDF-1.4")
        self.assertIs(win._dialog_parent(), _HandoffViewer.made[0]._win)

    def test_a_hand_off_panel_brings_the_hidden_window_out(self):
        win = _ScanWindow()
        win._reveal()
        self.assertTrue(win._win.shown)

    def test_but_not_once_the_pages_are_showing_elsewhere(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        win._reveal()
        self.assertFalse(win._win.shown)

    def test_nothing_to_reveal_in_the_other_interfaces(self):
        win = _ScanWindow(reporter=False)
        win._reveal()
        self.assertFalse(win._win.shown)


class ReporterChainTests(unittest.TestCase):
    """A scan popped out of the tabbed window carries its interface with it —
    and so does everything opened from it, text side included."""

    def setUp(self):
        _HandoffViewer.made.clear()
        self.mark = _load_functions(["_mark_reporter_window"])[
            "_mark_reporter_window"]

    def test_a_viewer_opened_from_a_reporter_window_is_one(self):
        app, win = _App(mode="tabs"), _Widget()
        origin = _Widget()
        origin._reporter_window = True
        self.mark(win, app, origin)
        self.assertTrue(getattr(win, "_reporter_window", False))

    def test_and_so_is_every_viewer_in_reporter_view(self):
        app, win = _App(mode="reporter"), _Widget()
        self.mark(win, app, _Widget())
        self.assertTrue(getattr(win, "_reporter_window", False))

    def test_but_not_one_opened_from_an_ordinary_window(self):
        app, win = _App(mode="windows"), _Widget()
        self.mark(win, app, _Widget())
        self.assertFalse(getattr(win, "_reporter_window", False))

    def test_an_app_that_cannot_answer_is_not_fatal(self):
        win = _Widget()
        self.mark(win, None, _Widget())
        self.assertFalse(getattr(win, "_reporter_window", False))

    def test_the_viewer_marks_itself_where_it_was_opened_from(self):
        src = _source_of("_FloatingPdfWindow", "__init__")
        self.assertIn(
            "_mark_reporter_window(self._win, app,\n"
            "                              anchor if anchor is not None "
            "else parent)", src)

    def test_a_scan_handed_over_by_its_courier_is_one_too(self):
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        self.assertTrue(
            getattr(_HandoffViewer.made[0]._win, "_reporter_window", False))


class WindowIndependenceTests(unittest.TestCase):
    """No window in Reporter View closes another.  Tk destroys a top-level
    with its master, so every one of them hangs on the application root."""

    def setUp(self):
        self.ns = _load(
            "CourtListenerGUI", ["window_master", "new_secondary_view_host"],
            {"_ui_toplevel": lambda master: ("window on", master)},
        )

    def _app(self, mode="reporter"):
        app = _App(mode=mode)
        app._case_tabs_enabled = mode == "tabs"
        for name in ("window_master", "new_secondary_view_host"):
            setattr(app, name, self.ns[name].__get__(app))
        return app

    def test_a_window_opened_in_reporter_view_hangs_on_the_app(self):
        app = self._app()
        self.assertIs(app.window_master(_Widget()), app.root)

    def test_and_so_does_one_opened_from_a_popped_out_scan(self):
        app = self._app(mode="tabs")
        origin = _Widget()
        origin._reporter_window = True
        self.assertIs(app.window_master(origin), app.root)

    def test_individual_windows_keeps_its_own_arrangement(self):
        app = self._app(mode="windows")
        origin = _Widget()
        self.assertIs(app.window_master(origin), origin)

    def test_the_root_going_away_is_not_fatal(self):
        app = self._app()
        app.root.destroy()
        origin = _Widget()
        self.assertIs(app.window_master(origin), origin)

    def test_a_new_document_window_is_opened_on_that_master(self):
        app = self._app()
        self.assertEqual(app.new_secondary_view_host(_Widget()),
                         ("window on", app.root))

    def test_a_cited_scan_is_owned_by_the_app_not_by_the_case_it_came_from(self):
        src = _source_of("CourtListenerGUI", "_show_cited_case_pdf")
        self.assertIn("self.root, data, url, title, margin=margin, app=self",
                      src)
        self.assertIn("anchor = host if host is not self.root else None", src)
        self.assertIn("anchor=anchor", src)

    def test_and_a_citation_followed_out_of_it_starts_from_it(self):
        src = _source_of("CourtListenerGUI", "_show_cited_case_pdf")
        self.assertIn("on_cite=lambda act, snip: self.open_cited_case_pdf(\n"
                      "                    onward()", src)
        self.assertIn("on_open_text=lambda: _follow_brief_action(\n"
                      "                    self, onward()", src)

    def test_the_hidden_courier_is_owned_by_the_app_too(self):
        src = _source_of("_PdfWindow", "__init__")
        self.assertIn('_ui_toplevel(getattr(app, "root", parent)) '
                      'if self._reporter', src)

class _StripMenu:
    def __init__(self):
        self.items: list = []

    def add_command(self, label="", command=None, **kw):
        self.items.append(("command", label, command))

    def add_cascade(self, label="", menu=None, **kw):
        self.items.append(("cascade", label, menu))

    def add_separator(self, **kw):
        self.items.append(("separator", "", None))

    def index(self, _what):
        return len(self.items) - 1 if self.items else None

    def delete(self, first, last):
        del self.items[first:last + 1]

    def entryconfigure(self, index, **kw):
        kind, label, cmd = self.items[index]
        self.items[index] = (kind, kw.get("label", label), cmd)

    def labels(self):
        return [label for kind, label, _c in self.items
                if kind in ("command", "cascade")]


class _BookmarkOwner:
    def __init__(self, desc=None):
        self.desc = desc
        self.toggled = 0

    def _bookmark_descriptor(self):
        return self.desc

    def _toggle_bookmark(self):
        self.toggled += 1


class _BookmarkApp:
    def __init__(self, marked=()):
        self.marked = set(marked)

    def is_bookmarked(self, key):
        return key in self.marked


STRIP_NAMES = ["_sync_bar_menu", "_add_bar_menu_bookmark", "_bookmark_owner",
               "showing_text"]
STRIP_NS = _load("_FloatingPdfWindow", STRIP_NAMES, {"_ACCEL": "Ctrl"})


class _StripViewer:
    """Just the strip's menu and what decides what goes on it."""

    def __init__(self, owner=None, app=None, reader=None, mode="pdf",
                 recent=None, interface=None):
        self._bookmarks = owner
        self._app = app
        self._reader = reader
        self._mode = mode
        self._recent_menu = recent
        self._interface_menu = interface
        self.closed = 0
        self._bar_menu = _StripMenu()
        for label in ("Save As…", "Print…"):
            self._bar_menu.add_command(label=label)
        self._bar_menu_fixed = self._bar_menu.index("end")
        for name in STRIP_NAMES:
            setattr(self, name, STRIP_NS[name].__get__(self))

    def close(self):
        self.closed += 1


class StripMenuTests(unittest.TestCase):
    """A window with no menu bar keeps History, Bookmarks, the Interface and
    Close on the strip's own menu."""

    def test_save_and_print_lead_and_close_ends_it(self):
        viewer = _StripViewer()
        viewer._sync_bar_menu()
        self.assertEqual(viewer._bar_menu.labels(),
                         ["Save As…", "Print…", "Close\tCtrl+W"])

    def test_close_closes_the_window_properly(self):
        viewer = _StripViewer()
        viewer._sync_bar_menu()
        viewer._bar_menu.items[-1][2]()
        self.assertEqual(viewer.closed, 1)

    def test_history_and_the_interface_are_on_it(self):
        viewer = _StripViewer(recent=object(), interface=object())
        viewer._sync_bar_menu()
        self.assertEqual(viewer._bar_menu.labels()[2:4],
                         ["Recent", "Interface"])

    def test_an_app_that_cannot_fill_them_gets_neither(self):
        viewer = _StripViewer()
        viewer._sync_bar_menu()
        self.assertNotIn("Recent", viewer._bar_menu.labels())
        self.assertNotIn("Interface", viewer._bar_menu.labels())

    def test_posting_it_twice_does_not_stack_the_menu_up(self):
        viewer = _StripViewer(recent=object(), interface=object())
        viewer._sync_bar_menu()
        first = viewer._bar_menu.labels()
        viewer._sync_bar_menu()
        self.assertEqual(viewer._bar_menu.labels(), first)

    def test_the_menu_is_rebuilt_whenever_it_is_posted(self):
        src = _source_of("_FloatingPdfWindow", "_post_bar_menu")
        self.assertIn("self._sync_bar_menu()", src)

    def test_the_submenus_are_made_once_and_re_attached(self):
        src = _source_of("_FloatingPdfWindow", "_build_bar")
        self.assertIn('self._recent_menu = self._bar_submenu('
                      '"populate_history_menu")', src)
        self.assertIn('self._interface_menu = self._bar_submenu(', src)
        sub = _source_of("_FloatingPdfWindow", "_bar_submenu")
        self.assertIn("postcommand=lambda m=sub: fill(m, *args)", sub)


class StripBookmarkTests(unittest.TestCase):
    def test_a_statute_scan_can_be_bookmarked_from_the_strip(self):
        owner = _BookmarkOwner({"key": "pdf:x", "noun": "statute"})
        viewer = _StripViewer(owner, _BookmarkApp())
        viewer._sync_bar_menu()
        self.assertIn("Bookmark This Statute", viewer._bar_menu.labels())

    def test_and_taken_off_again(self):
        owner = _BookmarkOwner({"key": "pdf:x", "noun": "case"})
        viewer = _StripViewer(owner, _BookmarkApp(marked={"pdf:x"}))
        viewer._sync_bar_menu()
        self.assertIn("Remove Bookmark for This Case",
                      viewer._bar_menu.labels())

    def test_the_entry_does_the_bookmarking(self):
        owner = _BookmarkOwner({"key": "pdf:x", "noun": "case"})
        viewer = _StripViewer(owner, _BookmarkApp())
        viewer._sync_bar_menu()
        viewer._bar_menu.items[3][2]()
        self.assertEqual(owner.toggled, 1)

    def test_the_state_is_re_read_each_time(self):
        owner = _BookmarkOwner({"key": "pdf:x", "noun": "case"})
        app = _BookmarkApp()
        viewer = _StripViewer(owner, app)
        viewer._sync_bar_menu()
        app.marked.add("pdf:x")
        viewer._sync_bar_menu()
        self.assertIn("Remove Bookmark for This Case",
                      viewer._bar_menu.labels())

    def test_a_document_that_cannot_be_bookmarked_gets_no_entry(self):
        viewer = _StripViewer(_BookmarkOwner(None), _BookmarkApp())
        viewer._sync_bar_menu()
        self.assertEqual(viewer._bar_menu.labels(),
                         ["Save As…", "Print…", "Close\tCtrl+W"])

    def test_nor_does_a_viewer_with_no_owner_to_ask(self):
        viewer = _StripViewer(None, _BookmarkApp())
        viewer._sync_bar_menu()
        self.assertEqual(viewer._bar_menu.labels(),
                         ["Save As…", "Print…", "Close\tCtrl+W"])

    def test_the_text_side_bookmarks_the_opinion_it_is_showing(self):
        scan = _BookmarkOwner({"key": "pdf:x", "noun": "statute"})
        reader = _BookmarkOwner({"key": "case:y", "noun": "case"})
        viewer = _StripViewer(scan, _BookmarkApp(), reader=reader, mode="text")
        viewer._sync_bar_menu()
        self.assertIn("Bookmark This Case", viewer._bar_menu.labels())
        viewer._bar_menu.items[3][2]()
        self.assertEqual((reader.toggled, scan.toggled), (1, 0))

    def test_and_the_pages_bookmark_the_document_behind_them(self):
        scan = _BookmarkOwner({"key": "pdf:x", "noun": "statute"})
        reader = _BookmarkOwner({"key": "case:y", "noun": "case"})
        viewer = _StripViewer(scan, _BookmarkApp(), reader=reader, mode="pdf")
        viewer._sync_bar_menu()
        self.assertIn("Bookmark This Statute", viewer._bar_menu.labels())

    def test_the_courier_hands_the_viewer_its_bookmark(self):
        _HandoffViewer.made.clear()
        win = _ScanWindow()
        win._show(b"%PDF-1.4")
        self.assertIs(_HandoffViewer.made[0].kw["bookmarks"], win)


class ScanWindowSourceTests(unittest.TestCase):
    """The pieces that are easier to read off the source than to drive."""

    def test_the_hidden_courier_is_not_in_the_window_registry(self):
        src = _source_of("_PdfWindow", "__init__")
        self.assertIn("not self._reporter", src)
        self.assertIn("self._win.withdraw()", src)

    def test_saving_and_printing_use_the_rendering_on_screen(self):
        src = _source_of("_PdfWindow", "_hand_to_viewer")
        self.assertIn("on_save=lambda pane: self._download(pane)", src)
        self.assertIn("on_print=lambda pane: self._print(pane)", src)

    def test_the_place_the_reader_is_comes_from_the_viewer_s_pages(self):
        src = _source_of("_PdfWindow", "_discovered_text_target")
        self.assertIn('getattr(self._float, "_pane", None)', src)

    def test_an_empty_lookup_takes_the_switch_off_the_strip(self):
        src = _source_of("_PdfWindow", "_on_text_discovered")
        self.assertIn("self._float.set_text_side(False)", src)

    def test_the_switch_builds_the_discovered_text_in_place(self):
        src = _source_of("_PdfWindow", "_embed_discovered_text")
        self.assertIn("chromeless=True", src)
        self.assertIn("initial_pdf_analysis=self._initial_pdf_analysis()", src)


if __name__ == "__main__":
    unittest.main()
