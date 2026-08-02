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
import sys
import types
import typing
import unittest
from unittest import mock

import citations
import opinion_location
import slip_opinion


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


def _load_function(name: str, extra=None):
    """Exec one module-level function into a stub namespace."""
    src = next((ast.get_source_segment(SRC, n) for n in TREE.body
                if isinstance(n, ast.FunctionDef) and n.name == name), None)
    if src is None:
        raise AssertionError(f"module-level function not found: {name}")
    ns = {"tk": _Tk, "re": re, "Optional": typing.Optional}
    ns.update(extra or {})
    exec(src, ns)
    return ns[name]


#: The real thing: what saving and printing both write, so a test that watches
#: the pane sees exactly the calls the app makes.
WRITE_OUTPUT_PDF = _load_function("_write_output_pdf")


def _fake_case_law_reporter_cite(url: str) -> str:
    """The reporter a static.case.law file names, as the real one reads it."""
    m = re.search(r"static\.case\.law/([a-z0-9-]+)/(\d+)/(\d+)", url or "")
    reporters = {"us": "U.S.", "sct": "S. Ct.", "f2d": "F.2d"}
    if not m or m.group(1) not in reporters:
        return ""
    return f"{int(m.group(2))} {reporters[m.group(1)]} {int(m.group(3))}"


_NORMALIZED_US_CITE = (
    lambda c: (str(c).strip()
               if re.search(r"\d+\s+U\.?\s?S\.?\s+\d+", str(c)) else "")
)

#: The real pin-cite arithmetic, so a test exercises the rule rather than a
#: restatement of it.
PIN_PAGE_NUMBER = _load_function(
    "_pin_page_number", {"_split_note_pin": citations.split_note_pin})
def _module_value(name: str):
    """A module-level constant, evaluated from the source, so a test reads the
    real thing rather than a restatement of it."""
    for node in TREE.body:
        targets = (
            node.targets if isinstance(node, ast.Assign)
            else [node.target] if isinstance(node, ast.AnnAssign) else []
        )
        if any(isinstance(t, ast.Name) and t.id == name for t in targets):
            return eval(ast.get_source_segment(SRC, node.value),  # noqa: S307
                        {"re": re, "frozenset": frozenset})
    raise AssertionError(f"module-level constant not found: {name}")


PRINTED_PAGE_NUMBERS = _load_function(
    "_printed_page_numbers",
    {"slip_opinion": slip_opinion,
     "_FOLIO_LINES": _module_value("_FOLIO_LINES"),
     "_FOLIO_RE": _module_value("_FOLIO_RE"),
     "_SPACED_DIGITS_RE": _module_value("_SPACED_DIGITS_RE")})
SCAN_PAGE_FOR_PIN = _load_function(
    "_scan_page_for_pin",
    {"opinion_location": opinion_location,
     "_pin_page_number": PIN_PAGE_NUMBER,
     "_printed_page_numbers": PRINTED_PAGE_NUMBERS})


#: The real narrowing: which of a case's parallel citations names the pages
#: actually on screen.
SCAN_CITATION_ITEM = _load_function(
    "_scan_citation_item",
    {"_normalized_us_cite": _NORMALIZED_US_CITE,
     "_is_us_reports_pdf": lambda url: "usrep" in (url or "").lower(),
     "_case_law_reporter_cite": _fake_case_law_reporter_cite},
)


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
     "_request_cited_pdf_analysis", "_cited_filename_item",
     "_jump_to_pin", "_scan_cite_for",
     "_describe_warmed_case", "_save_cited_pdf", "_print_cited_pdf"],
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
     "_normalized_us_cite": _NORMALIZED_US_CITE,
     "_scan_citation_item": SCAN_CITATION_ITEM,
     "_scan_page_for_pin": SCAN_PAGE_FOR_PIN,
     # The reader's caption reader, which names a case the way the reports do.
     "parse_opinion_blocks": lambda html: html,
     "_scholar_caption_name": lambda blocks: CAPTIONS.get(blocks, ""),
     "_case_law_reporter_cite": _fake_case_law_reporter_cite,
     "_cluster_citations_to_strings": lambda cites: [str(c) for c in cites],
     "_is_redacted_case_pdf": lambda url: "case.law" in (url or ""),
     "_build_default_filename": lambda item: FILENAMES.append(item) or "NAME",
     "_named_temp_pdf_path": lambda stem: f"/tmp/{stem}.pdf",
     "_write_output_pdf": WRITE_OUTPUT_PDF,
     "_print_pdf_file": lambda parent, path, status: PRINTED.append(path),
     "_case_law_print_citation": lambda *a, **kw: "HEADER",
     "filedialog": mock.Mock(),
     "messagebox": mock.Mock(),
     },
)

TEXT_OPENS: list = []
FILENAMES: list = []
PRINTED: list = []
CAPTIONS: dict = {}          # opinion html -> what its caption reads as


class _FakeViewer:
    opened: list = []

    def __init__(self, parent, data, url, title, **kw):
        self.parent, self.data, self.url, self.title = parent, data, url, title
        self.kw = kw
        self._win = f"toplevel of {title}"   # what a click here starts from
        self.surfaced = 0
        self.analyses = []
        self.scrolled: list = []
        self.at_page = None
        self.living = True
        _FakeViewer.opened.append(self)

    def surface(self):
        self.surfaced += 1

    def apply_analysis(self, result):
        self.analyses.append(result)

    def alive(self):
        return self.living

    def viewport_page(self):
        return self.at_page

    def scroll_to_page(self, page, y_pt=None):
        self.scrolled.append(page)
        self.at_page = page


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
        self._describe_warmed_case = APP_NS["_describe_warmed_case"]
        for name in ("open_cited_case_pdf", "_cited_case_pdf_item",
                     "_show_cited_case_pdf", "_cited_pdf_window_closed",
                     "_warm_case_text", "_request_cited_pdf_analysis",
                     "_cited_filename_item", "_save_cited_pdf",
                     "_print_cited_pdf", "_jump_to_pin", "_scan_cite_for"):
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

    def test_and_the_scan_opens_at_the_page_the_pin_names(self):
        self._click(action=("cite", "410 U.S. 113@153"))
        # 153 is the 41st page of a scan that starts at 113.
        self.assertEqual(_FakeViewer.opened[0].scrolled[0], 40)

    def test_a_citation_with_no_pin_opens_at_the_start(self):
        self._click(action=("cite", "410 U.S. 113"))
        self.assertEqual(_FakeViewer.opened[0].scrolled, [])

    def test_a_pin_in_another_reporter_is_not_counted_against_this_one(self):
        # The link cites the Supreme Court Reporter; the scan found for it is
        # the U.S. Reports, whose pages break somewhere else entirely.
        RESOLVED["93 S. Ct. 705"] = "https://loc.test/usrep410113.pdf"
        self._click(action=("cite", "93 S. Ct. 705@710"))
        self.assertEqual(_FakeViewer.opened[0].scrolled, [])

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

    def test_a_statute_inside_that_scan_opens_as_well(self):
        # The scan lookup has no answer for anything but a case, so a statute,
        # a rule or a regulation clicked on these pages has to go on to the
        # ordinary opener rather than fall on the floor.
        self._click()
        _FakeViewer.opened[0].kw["on_cite"](
            ("usc", "42:1983:"), "42 U.S.C. § 1983")
        self.assertEqual(len(_FakeViewer.opened), 1)   # no second scan
        self.assertEqual([args[2] for args, _kw in TEXT_OPENS],
                         [("usc", "42:1983:")])

    def test_and_so_do_the_other_sources_that_are_not_cases(self):
        self._click()
        for action in (("statpdf", "https://govinfo.test/statute/14/27"),
                       ("cfr", "29:1614.105:a,1"),
                       ("engrep", "156:145")):
            _FakeViewer.opened[0].kw["on_cite"](action, "")
        self.assertEqual([args[2] for args, _kw in TEXT_OPENS],
                         [("statpdf", "https://govinfo.test/statute/14/27"),
                          ("cfr", "29:1614.105:a,1"),
                          ("engrep", "156:145")])

    def test_a_statute_click_starts_from_the_scan_it_was_clicked_in(self):
        # Not from the window that scan was opened out of, which may be gone.
        self._click()
        window = _FakeViewer.opened[0]
        window.kw["on_cite"](("usc", "42:1983:"), "")
        self.assertIs(TEXT_OPENS[0][0][1], window._win)

    def test_the_viewer_can_save_and_print_the_scan(self):
        self._click()
        kw = _FakeViewer.opened[0].kw
        self.assertIsNotNone(kw["on_save"])
        self.assertIsNotNone(kw["on_print"])

    def test_the_scan_gets_clickable_citations_and_search(self):
        self._click()
        analysis = _FakeViewer.opened[0].analyses[0]
        self.assertEqual(analysis["url"],
                         "https://loc.test/usrep410113.pdf")
        self.assertIn(0, analysis["links"])


class CitedCaseFilenameTests(unittest.TestCase):
    """What a cited case's scan is saved and printed as."""

    def setUp(self):
        FILENAMES.clear(); PRINTED.clear()
        self.app = _App()
        self.named = {"data": b"%PDF", "url": "https://loc.test/usrep410113.pdf",
                      "cite": "410 U.S. 113", "name": "Roe v. Wade",
                      "record": None}

    def test_before_the_text_arrives_the_citation_names_the_file(self):
        item = self.app._cited_filename_item(self.named)
        self.assertEqual(item["caseName"], "Roe v. Wade")
        self.assertEqual(item["citation"], ["410 U.S. 113"])

    def test_the_opinion_text_supplies_the_bluebook_pieces(self):
        # A citation and a link caption cannot give a court or a date; the
        # fetched opinion can.
        self.named["record"] = {
            "name": "Roe v. Wade", "cites": ["410 U.S. 113", "93 S. Ct. 705"],
            "date_filed": "1973-01-22", "year": "1973", "court": "scotus",
        }
        item = self.app._cited_filename_item(self.named)
        self.assertEqual(item["caseName"], "Roe v. Wade")
        self.assertEqual(item["dateFiled"], "1973-01-22")
        self.assertEqual(item["court_id"], "scotus")
        self.assertIn("93 S. Ct. 705", item["citation"])

    def test_a_year_alone_still_dates_the_file(self):
        self.named["record"] = {"name": "Roe", "cites": [], "year": "1973"}
        self.assertEqual(
            self.app._cited_filename_item(self.named)["dateFiled"],
            "1973-01-01")

    def test_a_us_reports_scan_is_filed_under_the_pages_it_prints(self):
        self.named["record"] = {"name": "Roe",
                                "cites": ["93 S. Ct. 705", "410 U.S. 113"]}
        item = self.app._cited_filename_item(self.named)
        self.assertEqual(item["_us_reports_cite"], "410 U.S. 113")

    def test_another_reporter_s_scan_is_not(self):
        self.named.update(url="https://static.case.law/f2d/500/0123-01.pdf",
                          record={"name": "Smith", "cites": ["500 F.2d 123"]})
        self.assertNotIn("_us_reports_cite",
                         self.app._cited_filename_item(self.named))

    def test_it_is_filed_under_the_reporter_its_own_file_names(self):
        # A Supreme Court Reporter scan prints S. Ct. pages: naming it by the
        # U.S. Reports pages it does not print describes the wrong document.
        self.named.update(
            url="https://static.case.law/sct/93/0705-01.pdf",
            record={"name": "Roe", "cites": ["410 U.S. 113", "93 S. Ct. 705"]})
        item = self.app._cited_filename_item(self.named)
        self.assertEqual(item["_scan_cite"], "93 S. Ct. 705")
        self.assertNotIn("_us_reports_cite", item)

    def test_the_us_reports_pages_still_win_where_they_are_what_is_shown(self):
        self.named.update(
            url="https://static.case.law/us/410/0113-01.pdf",
            record={"name": "Roe", "cites": ["410 U.S. 113", "93 S. Ct. 705"]})
        item = self.app._cited_filename_item(self.named)
        self.assertEqual(item["_scan_cite"], "410 U.S. 113")

    def test_a_scan_whose_file_names_no_reporter_keeps_the_usual_order(self):
        self.named.update(url="https://www.supremecourt.gov/x/22-1234.pdf",
                          record={"name": "Roe", "cites": ["410 U.S. 113"]})
        item = self.app._cited_filename_item(self.named)
        self.assertNotIn("_scan_cite", item)
        self.assertNotIn("_us_reports_cite", item)

    def test_the_clicked_citation_is_kept_when_the_text_omits_it(self):
        self.named["record"] = {"name": "Roe", "cites": ["93 S. Ct. 705"]}
        self.assertIn("410 U.S. 113",
                      self.app._cited_filename_item(self.named)["citation"])

    def test_saving_names_the_file_from_that(self):
        self.app._save_cited_pdf(self.named)
        self.assertEqual(len(FILENAMES), 1)
        self.assertEqual(FILENAMES[0]["caseName"], "Roe v. Wade")

    def test_printing_names_it_the_same_way(self):
        self.app._print_cited_pdf(self.named, pane=None)
        self.assertEqual(PRINTED, ["/tmp/NAME.pdf"])

    def test_printing_uses_the_rendering_on_screen(self):
        pane = mock.Mock()
        self.app._print_cited_pdf(self.named, pane=pane)
        pane.export_cropped_pdf.assert_called_once()

    def test_a_redacted_scan_is_whitened_and_re_lettered_for_paper(self):
        self.named["url"] = "https://static.case.law/f2d/500/0123-01.pdf"
        pane = mock.Mock()
        self.app._print_cited_pdf(self.named, pane=pane)
        kwargs = pane.export_cropped_pdf.call_args.kwargs
        self.assertTrue(kwargs["whiten_redactions"])
        self.assertEqual(kwargs["header_cite"], "HEADER")

    def test_a_pane_that_will_not_export_still_prints_the_original(self):
        pane = mock.Mock()
        pane.export_cropped_pdf.side_effect = RuntimeError("no")
        pane.export_pdf.side_effect = RuntimeError("no")
        self.app._print_cited_pdf(self.named, pane=pane)
        self.assertEqual(PRINTED, ["/tmp/NAME.pdf"])

    def test_nothing_is_saved_before_there_is_a_scan(self):
        self.app._save_cited_pdf({"data": None})
        self.assertEqual(FILENAMES, [])


class WarmedCaseRecordTests(unittest.TestCase):
    """Reading the caption and citations off the opinion fetched behind it."""

    def setUp(self):
        self.got = []
        self.app = _App()

    def _describe(self, record, name="Roe"):
        with mock.patch.dict(
            "sys.modules",
            {"opinion_db": mock.Mock(extract_record=lambda u, h: record)},
        ):
            self.app._describe_warmed_case(("u", "<html>"), name,
                                           self.got.append)

    def test_the_record_reaches_the_caller(self):
        self._describe({"name": "Roe v. Wade", "cites": ["410 U.S. 113"]})
        self.assertEqual(self.got[0]["name"], "Roe v. Wade")

    def test_the_link_s_own_caption_fills_in_a_missing_one(self):
        self._describe({"name": "", "cites": []}, name="Roe v. Wade")
        self.assertEqual(self.got[0]["name"], "Roe v. Wade")

    def test_a_page_with_no_record_reports_nothing(self):
        self._describe(None)
        self.assertEqual(self.got, [])


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
    ["_open_text"],
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
        for name in ("_open_text",):
            setattr(self, name, VIEWER_NS[name].__get__(self))


class OpenTheTextTests(unittest.TestCase):
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


# ---------------------------------------------------------------------------
# 4. What names the case: its own caption, not the docket one
# ---------------------------------------------------------------------------

NAME_NS = _load("_ScholarTextWindow", ["_filename_item", "_scan_window_title",
                                       "_pdf_filename_item"],
                {"_scan_citation_item": SCAN_CITATION_ITEM,
                 "_bluebook_display_name": lambda item: (
                     f"{item.get('caseName')} | "
                     f"{(item.get('_us_reports_cite') or item.get('_scan_cite') or (item.get('citation') or [''])[0])}")})


class _NamedReader:
    def __init__(self, item, caption):
        self._item = item
        self._bb = {"name": caption, "cite": "137 S. Ct. 911"}
        for name in ("_filename_item", "_scan_window_title",
                     "_pdf_filename_item"):
            setattr(self, name, NAME_NS[name].__get__(self))

    def _shown_us_reports_cite(self):
        return ""

    def _title_citation(self):
        return ""


#: What CourtListener's search result calls it, and what the reports print.
DOCKET_ITEM = {"caseName": "Manuel v. City of Joliet, Illinois",
               "citation": ["137 S. Ct. 911"],
               "dateFiled": "2017-03-21", "court_id": "scotus"}
CAPTION = "Manuel v. City of Joliet"
SLIP_URL = "https://www.supremecourt.gov/opinions/16pdf/14-9496.pdf"


class WarmedCaseNameTests(unittest.TestCase):
    """What the background fetch of the opinion tells the scan's window the
    case is called."""

    HTML = "<opinion of Manuel>"

    def setUp(self):
        CAPTIONS.clear()
        CAPTIONS[self.HTML] = "Manuel v. City of Joliet"

    def _describe(self, stored_name="Manuel v. City of Joliet, Illinois",
                  fallback="Manuel v. City of Joliet, Ill.", html=None):
        html = self.HTML if html is None else html
        record = {"name": stored_name, "cites": ["137 S. Ct. 911"],
                  "year": "2017", "court": "scotus"}
        fake = types.ModuleType("opinion_db")
        fake.extract_record = lambda url, h: dict(record)
        got: list = []
        with mock.patch.dict(sys.modules, {"opinion_db": fake}):
            APP_NS["_describe_warmed_case"](
                ("https://scholar.google.com/scholar_case?case=1", html),
                fallback, got.append)
        return got[0] if got else None

    def test_the_caption_the_reports_print_names_the_case(self):
        # The stored record keeps the docket caption whole; the window is
        # named the way every other window in the app names a case.
        self.assertEqual(self._describe()["name"], "Manuel v. City of Joliet")

    def test_the_stored_record_itself_is_not_rewritten(self):
        # A copy goes to the window; the database keeps what it keeps.
        record = self._describe()
        self.assertEqual(record["cites"], ["137 S. Ct. 911"])
        self.assertEqual(record["court"], "scotus")

    def test_an_unreadable_caption_leaves_the_stored_name(self):
        self.assertEqual(self._describe(html="<no caption here>")["name"],
                         "Manuel v. City of Joliet, Illinois")

    def test_and_with_neither_the_clicked_name_stands_in(self):
        record = self._describe(stored_name="", html="<no caption here>")
        self.assertEqual(record["name"], "Manuel v. City of Joliet, Ill.")


class CaseNameTests(unittest.TestCase):
    """A search result carries the caption the court docketed the case under;
    the opinion carries the one the reports print."""

    def test_the_opinion_s_own_caption_names_the_case(self):
        reader = _NamedReader(DOCKET_ITEM, CAPTION)
        self.assertEqual(reader._filename_item()["caseName"], CAPTION)

    def test_and_names_the_window_the_scan_opens_in(self):
        reader = _NamedReader(DOCKET_ITEM, CAPTION)
        self.assertTrue(
            reader._scan_window_title(SLIP_URL).startswith(CAPTION + " |"))

    def test_and_the_file_a_scan_is_saved_as(self):
        reader = _NamedReader(DOCKET_ITEM, CAPTION)
        self.assertEqual(reader._pdf_filename_item()["caseName"], CAPTION)

    def test_everything_else_about_the_case_still_comes_from_the_result(self):
        item = _NamedReader(DOCKET_ITEM, CAPTION)._filename_item()
        self.assertEqual(item["dateFiled"], "2017-03-21")
        self.assertEqual(item["court_id"], "scotus")
        self.assertEqual(item["citation"], ["137 S. Ct. 911"])

    def test_the_result_itself_is_not_rewritten(self):
        _NamedReader(DOCKET_ITEM, CAPTION)._filename_item()
        self.assertEqual(DOCKET_ITEM["caseName"],
                         "Manuel v. City of Joliet, Illinois")

    def test_no_caption_to_read_leaves_the_result_s_name(self):
        # A window opened straight onto a scan has no opinion text to ask.
        reader = _NamedReader(DOCKET_ITEM, "")
        self.assertEqual(reader._filename_item()["caseName"],
                         "Manuel v. City of Joliet, Illinois")

    def test_a_stale_case_name_key_does_not_outvote_it(self):
        item = dict(DOCKET_ITEM, case_name="Manuel v. City of Joliet, Ill.")
        self.assertNotIn("case_name",
                         _NamedReader(item, CAPTION)._filename_item())

    def test_a_reader_with_no_result_behind_it_is_unchanged(self):
        reader = _NamedReader(None, CAPTION)
        reader._bb = {"name": CAPTION, "cite": "137 S. Ct. 911",
                      "year": "2017", "court": ""}
        self.assertEqual(reader._filename_item()["caseName"], CAPTION)


# ---------------------------------------------------------------------------
# 5. A pin cite, and the page of the scan that prints it
# ---------------------------------------------------------------------------


def _page_with(text: str) -> list:
    """One page of a scan's text layer, as a run of positioned glyphs."""
    return [(ch, (i * 6.0, 700.0, i * 6.0 + 5.0, 710.0))
            for i, ch in enumerate(text)]


class PinPageTests(unittest.TestCase):
    """Counting a pin cite out to a page of the file it is printed in."""

    def test_the_offset_from_the_case_s_first_page(self):
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "153", "410 U.S. 113"), 40)

    def test_the_first_page_is_the_first_page(self):
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "113", "410 U.S. 113"), 0)

    def test_a_pin_before_the_case_starts_is_not_in_this_file(self):
        self.assertIsNone(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "94", "410 U.S. 113"))

    def test_a_footnote_pin_still_names_its_page(self):
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "153n4", "410 U.S. 113"), 40)

    def test_a_range_opens_at_the_first_of_it(self):
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "153-55", "410 U.S. 113"), 40)

    def test_a_pin_naming_only_a_footnote_names_no_page(self):
        self.assertIsNone(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "n4", "410 U.S. 113"))

    def test_a_pin_in_another_reporter_is_refused(self):
        # "93 S. Ct. at 710" says nothing about where 410 U.S. breaks.
        self.assertIsNone(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "710", "93 S. Ct. 705"))

    def test_the_same_reporter_in_another_volume_is_still_refused(self):
        self.assertIsNone(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "710", "93 S.Ct. 705"))

    def test_a_link_that_names_no_reporter_is_taken_at_its_word(self):
        self.assertEqual(SCAN_PAGE_FOR_PIN("410 U.S. 113", "153"), 40)

    def test_a_scan_that_names_no_reporter_answers_nothing(self):
        self.assertIsNone(SCAN_PAGE_FOR_PIN("", "153", "410 U.S. 113"))

    def test_a_pin_past_the_end_of_the_file_is_refused(self):
        self.assertIsNone(SCAN_PAGE_FOR_PIN(
            "410 U.S. 113", "900", "410 U.S. 113",
            pdf_pages=[_page_with("113"), _page_with("114")]))

    def test_the_printed_numbers_confirm_the_arithmetic(self):
        pages = [_page_with("113  ROE v. WADE"),
                 _page_with("114  OCTOBER TERM, 1972")]
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "114", "410 U.S. 113", pages), 1)

    def test_and_correct_it_where_a_cover_leaf_shifts_the_pages(self):
        pages = [_page_with("Supreme Court of the United States"),
                 _page_with("113  ROE v. WADE"),
                 _page_with("114  OCTOBER TERM, 1972")]
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "114", "410 U.S. 113", pages), 2)

    def test_pages_that_print_nothing_leave_the_arithmetic_alone(self):
        pages = [_page_with(""), _page_with(""), _page_with("")]
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "115", "410 U.S. 113", pages), 2)

    def test_a_number_read_twice_is_not_trusted_over_the_count(self):
        # Two pages both claiming 114: the arithmetic is the better answer.
        pages = [_page_with("113"), _page_with("114"), _page_with("114")]
        self.assertEqual(
            SCAN_PAGE_FOR_PIN("410 U.S. 113", "114", "410 U.S. 113", pages), 1)


class PrintedFolioTests(unittest.TestCase):
    def test_a_number_at_the_left_of_the_running_head(self):
        self.assertEqual(
            PRINTED_PAGE_NUMBERS([_page_with("152  OCTOBER TERM, 1972")]),
            {0: 152})

    def test_and_one_at_the_right(self):
        self.assertEqual(
            PRINTED_PAGE_NUMBERS([_page_with("ROE v. WADE  153")]), {0: 153})

    def test_figures_parted_by_the_line_reader_are_one_number(self):
        # A "1" leaves more room after its ink than the type's own advance,
        # so a page number comes off the scan as "1 13".
        self.assertEqual(
            PRINTED_PAGE_NUMBERS([_page_with("ROE v. WADE  1 13")]), {0: 113})
        self.assertEqual(
            PRINTED_PAGE_NUMBERS([_page_with("1 14  OCTOBER TERM, 1972")]),
            {0: 114})

    def test_a_page_printing_no_number_is_absent(self):
        self.assertEqual(PRINTED_PAGE_NUMBERS([_page_with("Syllabus")]), {})

    def test_an_empty_page_is_absent_too(self):
        self.assertEqual(PRINTED_PAGE_NUMBERS([[]]), {})


class PinNumberTests(unittest.TestCase):
    def test_a_plain_page(self):
        self.assertEqual(PIN_PAGE_NUMBER("153"), 153)

    def test_a_page_and_a_note(self):
        self.assertEqual(PIN_PAGE_NUMBER("153n4"), 153)

    def test_a_note_alone(self):
        self.assertIsNone(PIN_PAGE_NUMBER("n4"))

    def test_nothing_at_all(self):
        self.assertIsNone(PIN_PAGE_NUMBER(""))


if __name__ == "__main__":
    unittest.main()
