"""Following citations from a PDF, and the U.S. Reports cite already on file.

1. The case name on the floating viewer's strip opens that case's **text**.
   For a PDF opened from the opinion reader it goes back to that reader; for a
   case reached by following a citation it opens the text the ordinary way,
   warmed in the background while the reader looks at the scan.

2. A citation clicked **inside a PDF** opens the cited case's own PDF in a
   viewer of its own, resolved by the routes the PDF button already uses, and
   falls back to the text when no scan exists anywhere.

3. A U.S. Reports cite that came with the opinion — from the opinion database
   or the opinion's own header — is what the PDF resolver tries first, before
   it goes looking for one on CourtListener or Google Scholar.

Lifted out of ``courtlistener_gui`` with ``ast`` (importing it needs tkinter,
absent on a headless run) and driven against stubs.
"""

import ast
import pathlib
import re
import typing
import unittest
from unittest import mock


SRC = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
TREE = ast.parse(SRC)


class _Tk:
    TclError = Exception
    Misc = Menu = object


class _Thread:
    """Runs the worker inline, so a test sees the whole chain at once."""

    started: list = []

    def __init__(self, target=None, daemon=False, **_kw):
        self._target = target
        _Thread.started.append(self)

    def start(self):
        if self._target is not None:
            self._target()


def _load(cls, names, extra=None) -> dict:
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(SRC, n) for n in body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found on {cls}: {missing}")
    ns = {"tk": _Tk, "re": re, "Optional": typing.Optional,
          "threading": mock.Mock(Thread=_Thread)}
    ns.update(extra or {})
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


# ---------------------------------------------------------------------------
# 2. A citation clicked inside a PDF opens the cited case's PDF
# ---------------------------------------------------------------------------

RESOLVED: dict = {}          # cite -> url the resolver "finds"
FETCHED: dict = {}           # url  -> (bytes, final url)
CL_ITEMS: dict = {}          # cite -> the CourtListener cluster record


def _fetch_pdf_bytes(url, client=None, timeout=30):
    return FETCHED.get(url)


def _cl_item_for_citation(client, cite, name=""):
    return CL_ITEMS.get(cite)


APP_NS = _load(
    "CourtListenerGUI",
    ["open_cited_case_pdf", "_cited_case_pdf_item", "_show_cited_case_pdf",
     "_cited_pdf_window_closed", "_warm_case_text",
     "_request_cited_pdf_analysis"],
    {"_citation_link_name": lambda snippet, cite="": snippet.strip(),
     "_cl_item_for_citation": _cl_item_for_citation,
     "_fetch_pdf_bytes": _fetch_pdf_bytes,
     "_us_reports_cite": lambda cite: (
         "5 U.S. 137" if "Cranch" in cite else ""),
     "_pin_display": lambda pin: pin,
     "_is_us_reports_pdf": lambda url: "usrep" in (url or "").lower(),
     "_PdfPane": type("_PdfPane", (), {"_MARGIN": 18}),
     "_FloatingPdfWindow": lambda *a, **kw: _FakeViewer(*a, **kw),
     "_follow_brief_action": lambda *a, **kw: TEXT_OPENS.append((a, kw)),
     "_open_citation_in_browser": lambda *a: None,
     "_SCHOLAR_AVAILABLE": True,
     "_citation_search_variants": lambda cite: (cite,),
     "_extract_pdf_text_and_style": lambda data: ([[("c", (0, 0, 1, 1))]], []),
     "_citation_links_from_visible_pdf_text": lambda d, p, i: ({0: ["x"]}, set()),
     "slip_opinion": mock.Mock(detect_sections=lambda pages: []),
     },
)

TEXT_OPENS: list = []


class _FakeViewer:
    opened: list = []

    def __init__(self, parent, data, url, title, **kw):
        self.parent, self.data, self.url, self.title = parent, data, url, title
        self.kw = kw
        self.surfaced = 0
        self.analyses = []
        _FakeViewer.opened.append(self)

    def surface(self):
        self.surfaced += 1

    def apply_analysis(self, result):
        self.analyses.append(result)


class _FakeHost:
    def winfo_toplevel(self):
        return self


class _App:
    def __init__(self, token="tok", resolves=True):
        self.root = _FakeHost()
        self._token_var = mock.Mock()
        self._token_var.get.return_value = token
        self._cited_pdf_windows = set()
        self.resolved_items = []
        self._resolves = resolves
        self.scholar_calls = []
        for name in ("open_cited_case_pdf", "_cited_case_pdf_item",
                     "_show_cited_case_pdf", "_cited_pdf_window_closed",
                     "_warm_case_text", "_request_cited_pdf_analysis"):
            setattr(self, name, APP_NS[name].__get__(self))

    # --- collaborators ---
    def _get_client(self):
        return "client"

    def _resolve_pdf_url(self, client, item):
        self.resolved_items.append(item)
        if not self._resolves:
            return None
        for cite in item.get("citation") or []:
            if cite in RESOLVED:
                return RESOLVED[cite]
        return None

    def _post_root(self, fn):
        fn()

    def _get_scholar(self):
        return mock.Mock(
            fetch_by_citation=lambda c: self.scholar_calls.append(c) or True)


class CitedCasePdfTests(unittest.TestCase):
    def setUp(self):
        RESOLVED.clear(); FETCHED.clear(); CL_ITEMS.clear()
        TEXT_OPENS.clear(); _FakeViewer.opened.clear(); _Thread.started.clear()
        RESOLVED["410 U.S. 113"] = "https://loc.test/usrep410113.pdf"
        FETCHED["https://loc.test/usrep410113.pdf"] = (
            b"%PDF-1", "https://loc.test/usrep410113.pdf")
        self.app = _App()
        self.status = []

    def _click(self, action=("cite", "410 U.S. 113"), snippet="Roe v. Wade",
               fallback=None):
        return self.app.open_cited_case_pdf(
            _FakeHost(), action, snippet, self.status.append,
            fallback=fallback)

    def test_a_case_citation_opens_the_cited_case_s_pdf(self):
        self.assertTrue(self._click())
        self.assertEqual(len(_FakeViewer.opened), 1)
        self.assertEqual(_FakeViewer.opened[0].data, b"%PDF-1")

    def test_the_viewer_is_named_for_the_case_and_cite(self):
        self._click()
        self.assertEqual(_FakeViewer.opened[0].title,
                         "Roe v. Wade — 410 U.S. 113")

    def test_it_comes_to_the_front(self):
        self._click()
        self.assertEqual(_FakeViewer.opened[0].surfaced, 1)

    def test_a_us_reports_scan_gets_the_roomier_margin(self):
        self._click()
        self.assertEqual(_FakeViewer.opened[0].kw["margin"], 18 * 3)

    def test_the_app_holds_the_window_until_it_closes(self):
        self._click()
        window = _FakeViewer.opened[0]
        self.assertIn(window, self.app._cited_pdf_windows)
        self.app._cited_pdf_window_closed(window)
        self.assertNotIn(window, self.app._cited_pdf_windows)

    def test_the_pin_is_reported_but_the_scan_is_what_opens(self):
        self._click(action=("cite", "410 U.S. 113@153"))
        self.assertEqual(len(_FakeViewer.opened), 1)
        self.assertTrue(any("153" in s for s in self.status))

    def test_no_scan_anywhere_falls_back_to_the_text(self):
        self.app._resolves = False
        fallback = mock.Mock()
        self._click(fallback=fallback)
        self.assertEqual(_FakeViewer.opened, [])
        fallback.assert_called_once_with()

    def test_a_scan_that_will_not_download_falls_back_too(self):
        FETCHED.clear()
        fallback = mock.Mock()
        self._click(fallback=fallback)
        self.assertEqual(_FakeViewer.opened, [])
        fallback.assert_called_once_with()

    def test_a_statute_citation_is_not_ours_to_open(self):
        self.assertFalse(self._click(action=("statute", "42 U.S.C. 1983")))
        self.assertEqual(_FakeViewer.opened, [])

    def test_an_empty_citation_is_not_ours_either(self):
        self.assertFalse(self._click(action=("cite", "   ")))

    def test_the_text_is_fetched_while_the_reader_looks_at_the_scan(self):
        # So the case name on the strip opens a page already in hand.
        self._click()
        self.assertEqual(self.app.scholar_calls, ["410 U.S. 113"])

    def test_the_case_name_on_the_strip_opens_that_text(self):
        self._click()
        _FakeViewer.opened[0].kw["on_open_text"]()
        self.assertEqual(len(TEXT_OPENS), 1)

    def test_a_citation_inside_that_scan_opens_its_case_too(self):
        # Following citations from scan to scan, not just the first hop.
        RESOLVED["381 U.S. 479"] = "https://loc.test/usrep381479.pdf"
        FETCHED["https://loc.test/usrep381479.pdf"] = (
            b"%PDF-2", "https://loc.test/usrep381479.pdf")
        self._click()
        _FakeViewer.opened[0].kw["on_cite"](
            ("cite", "381 U.S. 479"), "Griswold v. Connecticut")
        self.assertEqual(len(_FakeViewer.opened), 2)
        self.assertEqual(_FakeViewer.opened[1].data, b"%PDF-2")

    def test_the_scan_gets_clickable_citations_and_search(self):
        self._click()
        analysis = _FakeViewer.opened[0].analyses[0]
        self.assertEqual(analysis["url"],
                         "https://loc.test/usrep410113.pdf")
        self.assertIn(0, analysis["links"])


class CitedCaseItemTests(unittest.TestCase):
    """What the resolver is handed for a cited case."""

    def setUp(self):
        CL_ITEMS.clear()
        self.app = _App()

    def test_the_clicked_citation_is_always_among_them(self):
        item = self.app._cited_case_pdf_item("client", "410 U.S. 113", "Roe")
        self.assertIn("410 U.S. 113", item["citation"])

    def test_the_case_name_comes_along_for_the_reporter_scans(self):
        item = self.app._cited_case_pdf_item("client", "410 U.S. 113", "Roe")
        self.assertEqual(item["caseName"], "Roe")

    def test_courtlistener_s_record_brings_the_court_date_and_docket(self):
        # What the official-report and slip-opinion paths key on.
        CL_ITEMS["410 U.S. 113"] = {
            "citation": ["410 U.S. 113", "93 S. Ct. 705"],
            "court_id": "scotus", "dateFiled": "1973-01-22",
            "docketNumber": "70-18", "caseName": "Roe v. Wade",
        }
        item = self.app._cited_case_pdf_item("client", "410 U.S. 113", "Roe")
        self.assertEqual(item["court_id"], "scotus")
        self.assertEqual(item["docketNumber"], "70-18")
        self.assertIn("93 S. Ct. 705", item["citation"])

    def test_an_old_nominative_cite_also_offers_its_modern_form(self):
        # "1 Cranch 137" is filed as "5 U.S. 137" in the official reports.
        item = self.app._cited_case_pdf_item("client", "1 Cranch 137",
                                             "Marbury")
        self.assertEqual(item["citation"], ["1 Cranch 137", "5 U.S. 137"])

    def test_without_courtlistener_the_citation_alone_will_do(self):
        item = self.app._cited_case_pdf_item(None, "410 U.S. 113", "Roe")
        self.assertEqual(item["citation"], ["410 U.S. 113"])

    def test_a_courtlistener_name_is_not_overwritten(self):
        CL_ITEMS["410 U.S. 113"] = {"caseName": "Roe v. Wade",
                                    "citation": ["410 U.S. 113"]}
        item = self.app._cited_case_pdf_item("client", "410 U.S. 113", "Roe")
        self.assertEqual(item["caseName"], "Roe v. Wade")


class PdfClickRoutingTests(unittest.TestCase):
    """The reader sends a click on a page through the cited-PDF path."""

    def setUp(self):
        self.ns = _load(
            "_ScholarTextWindow", ["_open_pdf_cite"],
            {"_follow_brief_action": lambda *a, **kw: TEXT_OPENS.append(a),
             "_open_citation_in_browser": lambda *a: BROWSER.append(a)},
        )
        TEXT_OPENS.clear()
        BROWSER.clear()

    def _reader(self, app):
        reader = mock.Mock()
        reader._app = app
        reader._open_pdf_cite = self.ns["_open_pdf_cite"].__get__(reader)
        return reader

    def test_a_case_citation_goes_to_the_cited_pdf_path(self):
        app = mock.Mock()
        app.open_cited_case_pdf.return_value = True
        reader = self._reader(app)
        reader._open_pdf_cite(("cite", "410 U.S. 113"), "Roe")
        app.open_cited_case_pdf.assert_called_once()
        self.assertEqual(TEXT_OPENS, [])

    def test_the_fallback_it_is_given_opens_the_text(self):
        app = mock.Mock()
        app.open_cited_case_pdf.return_value = True
        reader = self._reader(app)
        reader._open_pdf_cite(("cite", "410 U.S. 113"), "Roe")
        app.open_cited_case_pdf.call_args.kwargs["fallback"]()
        self.assertEqual(len(TEXT_OPENS), 1)

    def test_anything_that_is_not_a_case_opens_the_old_way(self):
        app = mock.Mock()
        app.open_cited_case_pdf.return_value = False   # a statute, a rule
        reader = self._reader(app)
        reader._open_pdf_cite(("statute", "42 U.S.C. 1983"), "")
        self.assertEqual(len(TEXT_OPENS), 1)

    def test_without_an_app_the_citation_opens_in_the_browser(self):
        reader = self._reader(None)
        reader._open_pdf_cite(("cite", "410 U.S. 113"), "Roe")
        self.assertEqual(len(BROWSER), 1)


BROWSER: list = []


# ---------------------------------------------------------------------------
# 1. The case name on the strip
# ---------------------------------------------------------------------------

VIEWER_NS = _load(
    "_FloatingPdfWindow",
    ["_open_text", "_style_name_as_link"],
    {"_CTK_AVAILABLE": False, "_UI": {"accent": "#2f6bd8"},
     "time": __import__("time")},
)
DEBOUNCE = next(
    eval(ast.get_source_segment(SRC, node.value))       # noqa: S307
    for cls in TREE.body
    if isinstance(cls, ast.ClassDef) and cls.name == "_FloatingPdfWindow"
    for node in cls.body
    if isinstance(node, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id == "_OPEN_TEXT_DEBOUNCE"
        for t in node.targets)
)


class _Viewer:
    _OPEN_TEXT_DEBOUNCE = DEBOUNCE

    def __init__(self, on_open_text=None):
        self._on_open_text = on_open_text
        self._name_label = mock.Mock()
        for name in ("_open_text", "_style_name_as_link"):
            setattr(self, name, VIEWER_NS[name].__get__(self))


class NameOnTheStripTests(unittest.TestCase):
    def test_clicking_the_name_opens_the_text(self):
        opened = []
        _Viewer(on_open_text=lambda: opened.append(True))._open_text()
        self.assertEqual(opened, [True])

    def test_a_viewer_with_no_text_to_offer_does_nothing(self):
        _Viewer()._open_text()      # must not raise

    def test_a_failure_opening_the_text_does_not_take_the_viewer_down(self):
        viewer = _Viewer(on_open_text=mock.Mock(side_effect=RuntimeError("no")))
        viewer._open_text()         # must not raise

    def test_one_click_opens_the_opinion_once(self):
        # A CustomTkinter label forwards a binding to the canvas and label it
        # is drawn from, so the handler is reached more than once per click.
        opened = []
        viewer = _Viewer(on_open_text=lambda: opened.append(True))
        viewer._open_text()
        viewer._open_text()
        viewer._open_text()
        self.assertEqual(opened, [True])

    def test_a_later_click_opens_it_again(self):
        opened = []
        viewer = _Viewer(on_open_text=lambda: opened.append(True))
        viewer._open_text()
        viewer._opened_text_at -= DEBOUNCE + 0.1     # a click a moment later
        viewer._open_text()
        self.assertEqual(opened, [True, True])

    def test_the_name_is_coloured_like_a_link(self):
        viewer = _Viewer(on_open_text=lambda: None)
        viewer._style_name_as_link("#2f6bd8")
        viewer._name_label.configure.assert_called_with(fg="#2f6bd8")

    def test_the_viewer_offers_the_name_only_when_there_is_text_behind_it(self):
        body = _source_of("_FloatingPdfWindow", "_build_bar")
        self.assertIn("if self._on_open_text is not None:", body)

    def test_a_pdf_opened_from_the_reader_goes_back_to_that_reader(self):
        body = _source_of("_ScholarTextWindow", "_show_pdf_floating")
        self.assertIn("on_open_text=self._surface_text_view", body)


# ---------------------------------------------------------------------------
# 3. The U.S. Reports cite already on file leads
# ---------------------------------------------------------------------------

ITEM_NS = _load(
    "_ScholarTextWindow",
    ["_pdf_item", "_adopt_stored_us_reports_cite"],
    {"_normalized_us_cite": lambda c: (
        c.strip() if re.search(r"\d+\s+U\.\s?S\.\s+\d+", str(c)) else ""),
     "_scotus_docket_tokens": lambda text: set(),
     "_item_docket_text": lambda item: "",
     },
)


class _DbReader:
    """A reader opened from the opinion database: no search result behind it,
    just the stored record."""

    def __init__(self, stored=(), header=(), bb_cite="", item=None,
                 scholar_id="sid"):
        self._item = item
        self._app = mock.Mock()
        self._app._get_opinion_db.return_value = mock.Mock(
            stored_citations=lambda sid: list(stored))
        self._header_cites = list(header)
        self._bb = {"cite": bb_cite, "name": "Cedar Point Nursery"}
        self._is_scotus = False
        self._us_reports_cite = ""
        self._sid = scholar_id
        for name in ("_pdf_item", "_adopt_stored_us_reports_cite"):
            setattr(self, name, ITEM_NS[name].__get__(self))

    def _opinion_scholar_id(self):
        return self._sid


class StoredUsCiteTests(unittest.TestCase):
    def test_the_stored_us_cite_leads_even_when_it_is_not_first(self):
        # The database has it second, behind the S. Ct. cite Scholar saved.
        reader = _DbReader(stored=["141 S.Ct. 2063", "594 U. S. 139"])
        item = reader._pdf_item()
        self.assertEqual(item["citation"][0], "594 U. S. 139")

    def test_it_is_handed_to_the_resolver_by_name(self):
        reader = _DbReader(stored=["141 S.Ct. 2063", "594 U. S. 139"])
        self.assertEqual(reader._pdf_item()["_us_reports_cite"],
                         "594 U. S. 139")

    def test_the_reader_learns_it_from_the_database(self):
        reader = _DbReader(stored=["141 S.Ct. 2063", "594 U. S. 139"])
        reader._adopt_stored_us_reports_cite()
        self.assertEqual(reader._us_reports_cite, "594 U. S. 139")

    def test_the_other_cites_are_still_offered_after_it(self):
        reader = _DbReader(stored=["141 S.Ct. 2063", "594 U. S. 139"])
        self.assertEqual(reader._pdf_item()["citation"],
                         ["594 U. S. 139", "141 S.Ct. 2063"])

    def test_a_cite_from_the_opinion_s_own_header_counts_too(self):
        reader = _DbReader(header=["143 S.Ct. 1369", "598 U. S. 631"])
        self.assertEqual(reader._pdf_item()["_us_reports_cite"],
                         "598 U. S. 631")

    def test_one_already_known_to_the_reader_is_not_looked_up_again(self):
        reader = _DbReader(stored=["141 S.Ct. 2063"])
        reader._us_reports_cite = "594 U. S. 139"
        reader._adopt_stored_us_reports_cite()
        self.assertEqual(reader._us_reports_cite, "594 U. S. 139")

    def test_an_opinion_with_no_us_cite_names_none(self):
        reader = _DbReader(stored=["141 S.Ct. 2063"], bb_cite="141 S. Ct. 2063")
        item = reader._pdf_item()
        self.assertNotIn("_us_reports_cite", item)
        self.assertEqual(item["citation"][0], "141 S.Ct. 2063")

    def test_an_opinion_the_database_does_not_have_is_no_worse_off(self):
        reader = _DbReader(scholar_id="", bb_cite="141 S. Ct. 2063")
        self.assertEqual(reader._pdf_item()["citation"], ["141 S. Ct. 2063"])

    def test_the_resolver_tries_that_cite_before_any_network_lookup(self):
        body = _source_of("CourtListenerGUI", "_resolve_pdf_url")
        given = body.index("if given_us_cite:")
        gather = body.index("all_cites = _gather_all_citations")
        self.assertLess(given, gather)


if __name__ == "__main__":
    unittest.main()
