"""U.S. Reports PDFs: the citation spelling, and the page-proof watermark.

Two things the app gets from a preliminary print off supremecourt.gov.  The
citation has to be recognised however it is spelled, since that is what names
the saved file; and the Reporter's "Page Proof Pending Publication" stamp has to
come off before the opinion is shown or printed.

Both are lifted out of ``courtlistener_gui`` with ``ast`` — the module imports
tkinter, which a headless run does not have.
"""

import __future__
import ast
import dataclasses
import difflib
import pathlib
import re
import threading
import typing
import unittest
from types import SimpleNamespace

from court_catalog import STATE_COURTS

try:
    import pypdfium2  # noqa: F401
    HAVE_PDFIUM = True
except ImportError:  # pragma: no cover - depends on the environment
    HAVE_PDFIUM = False


#: ``courtlistener_gui`` postpones its annotations; the pieces lifted out of
#: it are compiled the same way, or an annotation naming a class loaded later
#: would be evaluated here and fail.
_POSTPONED = __future__.annotations.compiler_flag


def _exec(source: str, ns: dict) -> None:
    exec(compile(source, "<courtlistener_gui>", "exec", _POSTPONED), ns)


def _segment(src: str, node) -> str:
    """A node's source, decorators and all — ``get_source_segment`` starts at
    the ``def``/``class`` line, which would drop a ``@dataclass``."""
    text = ast.get_source_segment(src, node)
    for dec in reversed(getattr(node, "decorator_list", None) or []):
        text = f"@{ast.get_source_segment(src, dec)}\n{text}"
    return text


def _load(*names, consts=(), extra=None):
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    ns = {"re": re, "_PDFIUM_LOCK": threading.RLock()}
    ns.update(extra or {})
    found = {n.name: _segment(src, n)
             for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.ClassDef))
             and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found at module level: {missing}")
    # Definitions first: a constant can be built out of them (the frequent
    # party names are a set comprehension over _name_tokens), while a function
    # only reaches for a constant when it is called.
    for name in names:
        _exec(found[name], ns)
    for node in tree.body:
        targets = (
            node.targets if isinstance(node, ast.Assign)
            else [node.target] if isinstance(node, ast.AnnAssign) else []
        )
        if any(getattr(t, "id", "") in consts for t in targets):
            _exec(ast.get_source_segment(src, node), ns)
    return ns


NS = _load(
    "_normalized_us_cite", "_text_obj_string", "_is_page_proof_object",
    "_strip_page_proof_watermark",
    consts=("_US_CITE_RE", "_PAGE_PROOF_RE"),
)


class CitationSpellingTests(unittest.TestCase):
    """"601 U. S. 124" and "601 U.S. 124" are the same citation.

    The Court sets the reporter with a space, and CourtListener and Google
    Scholar pass it through that way.  A pattern that missed the spaced form
    let a U.S. Reports scan be found — us_reports_pdf's own pattern is
    laxer — while the cite that names the saved file came back empty, so the
    file fell back to the S. Ct. citation.
    """

    def test_both_spellings_normalize_the_same(self):
        for text in ("601 U.S. 124", "601 U. S. 124"):
            with self.subTest(text=text):
                self.assertEqual(NS["_normalized_us_cite"](text),
                                 "601 U.S. 124")

    def test_a_full_citation_is_reduced_to_the_reporter_cite(self):
        self.assertEqual(
            NS["_normalized_us_cite"](
                "Pulsifer v. United States, 601 U. S. 124, 130 (2024)"),
            "601 U.S. 124")

    def test_a_supreme_court_reporter_cite_is_not_a_us_cite(self):
        self.assertEqual(NS["_normalized_us_cite"]("144 S. Ct. 718"), "")

    def test_no_citation_at_all(self):
        for text in ("", "no citation here", "39 F. 4th 1018"):
            with self.subTest(text=text):
                self.assertEqual(NS["_normalized_us_cite"](text), "")

    def test_the_pattern_matches_both_spellings(self):
        for text in ("601 U.S. 124", "601 U. S. 124"):
            with self.subTest(text=text):
                self.assertTrue(NS["_US_CITE_RE"].search(text))


class PageProofPhraseTests(unittest.TestCase):
    def test_the_phrase_is_recognised_however_it_is_spaced(self):
        for text in ("Page Proof Pending Publication",
                     "PAGE PROOF PENDING PUBLICATION",
                     "Page  Proof  Pending  Publication",
                     "PageProofPendingPublication"):
            with self.subTest(text=text):
                self.assertTrue(NS["_PAGE_PROOF_RE"].search(text))

    def test_ordinary_opinion_text_is_not_the_phrase(self):
        for text in ("The judgment is affirmed.",
                     "Pending publication of the opinion",
                     "page proof"):
            with self.subTest(text=text):
                self.assertIsNone(NS["_PAGE_PROOF_RE"].search(text))


def _minimal_pdf(*content_lines: str) -> bytes:
    """A one-page PDF drawing each of *content_lines* as a line of text."""
    stream = "\n".join(
        f"BT /F1 12 Tf 72 {700 - 40 * i} Td ({line}) Tj ET"
        for i, line in enumerate(content_lines)
    ).encode("latin-1")
    objects = [
        b"<</Type/Catalog/Pages 2 0 R>>",
        b"<</Type/Pages/Kids[3 0 R]/Count 1>>",
        b"<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]"
        b"/Resources<</Font<</F1 5 0 R>>>>/Contents 4 0 R>>",
        b"<</Length %d>>stream\n" % len(stream) + stream + b"\nendstream",
        b"<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj" % i + body + b"endobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n" % (len(objects) + 1)
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += b"%010d 00000 n \n" % off
    out += (b"trailer<</Size %d/Root 1 0 R>>\nstartxref\n%d\n%%%%EOF\n"
            % (len(objects) + 1, xref))
    return bytes(out)


def _govinfo_ocr_pdf() -> bytes:
    """Small image-plus-hidden-text PDF with GovInfo's letter-spaced shape."""
    def spaced(line: str) -> str:
        # One PDF space is the artificial inter-letter gap; a real word gap is
        # wide enough for the geometry repair to retain it.
        return " ".join(line.replace(" ", "                    "))

    lines = (
        "Cite as: 567 U. S. 519 (2012) 521",
        "See Roe v. Wade, 410 U. S. 113, 152 (1973).",
        "The judgment of the Court is affirmed.",
    )
    text = []
    for i, line in enumerate(lines):
        escaped = (
            spaced(line)
            .replace("\\", "\\\\")
            .replace("(", "\\(")
            .replace(")", "\\)")
        )
        # Near-zero vertical text scale reproduces the baseline-only glyph
        # boxes in the GovInfo volume; render mode 3 makes the layer invisible.
        # Negative word spacing makes each artificial single space narrow;
        # the enlarged run at a genuine word boundary remains visibly wider.
        text.append(
            f"BT /F1 1 Tf -0.55 Tw 3 Tr 3 0 0 .01 72 {700 - 18 * i} Tm "
            f"({escaped}) Tj ET"
        )
    stream = (
        "q 612 0 0 792 0 0 cm /Im1 Do Q\n" + "\n".join(text)
    ).encode("latin-1")
    objects = [
        b"<</Type/Catalog/Pages 2 0 R>>",
        b"<</Type/Pages/Kids[3 0 R]/Count 1>>",
        b"<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]"
        b"/Resources<</Font<</F1 5 0 R>>/XObject<</Im1 6 0 R>>>>"
        b"/Contents 4 0 R>>",
        b"<</Length %d>>stream\n" % len(stream) + stream + b"\nendstream",
        b"<</Type/Font/Subtype/Type1/BaseFont/Courier>>",
        b"<</Type/XObject/Subtype/Image/Width 1/Height 1"
        b"/ColorSpace/DeviceGray/BitsPerComponent 8/Length 1>>stream\n"
        b"\xff\nendstream",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj" % i + body + b"endobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n" % (len(objects) + 1)
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += b"%010d 00000 n \n" % off
    out += (
        b"trailer<</Size %d/Root 1 0 R>>\nstartxref\n%d\n%%%%EOF\n"
        % (len(objects) + 1, xref)
    )
    return bytes(out)


@unittest.skipUnless(HAVE_PDFIUM, "pypdfium2 not installed")
class WatermarkRemovalTests(unittest.TestCase):
    """The stamp comes off; everything else is left exactly as it was."""

    PHRASE = re.compile(r"page\s*proof\s*pending\s*publication", re.IGNORECASE)

    def _text(self, data: bytes) -> str:
        doc = pypdfium2.PdfDocument(data)
        try:
            return "\n".join(doc[i].get_textpage().get_text_range()
                             for i in range(len(doc)))
        finally:
            doc.close()

    def test_the_stamp_is_removed(self):
        pdf = _minimal_pdf("Opinion of the Court",
                           "Page Proof Pending Publication")
        out = NS["_strip_page_proof_watermark"](pdf)
        self.assertIsNone(self.PHRASE.search(self._text(out)))

    def test_the_opinion_text_survives(self):
        pdf = _minimal_pdf("Opinion of the Court",
                           "Page Proof Pending Publication",
                           "The judgment is affirmed.")
        out = NS["_strip_page_proof_watermark"](pdf)
        text = self._text(out)
        self.assertIn("Opinion of the Court", text)
        self.assertIn("The judgment is affirmed.", text)

    def test_a_pdf_without_the_stamp_is_returned_untouched(self):
        pdf = _minimal_pdf("Opinion of the Court",
                           "The judgment is affirmed.")
        self.assertIs(NS["_strip_page_proof_watermark"](pdf), pdf)

    def test_the_page_count_is_unchanged(self):
        pdf = _minimal_pdf("Opinion", "Page Proof Pending Publication")
        out = NS["_strip_page_proof_watermark"](pdf)
        doc_in, doc_out = pypdfium2.PdfDocument(pdf), pypdfium2.PdfDocument(out)
        try:
            self.assertEqual(len(doc_in), len(doc_out))
        finally:
            doc_in.close()
            doc_out.close()

    def test_the_result_is_still_a_pdf(self):
        pdf = _minimal_pdf("Opinion", "Page Proof Pending Publication")
        self.assertTrue(
            NS["_strip_page_proof_watermark"](pdf).startswith(b"%PDF"))

    def test_rubbish_is_handed_back_rather_than_raising(self):
        # A watermark is a blemish; losing the document is not acceptable.
        for data in (b"", b"not a pdf at all", b"%PDF-1.4 truncated"):
            with self.subTest(data=data):
                self.assertIs(NS["_strip_page_proof_watermark"](data), data)


@unittest.skipUnless(HAVE_PDFIUM, "pypdfium2 not installed")
class LinkPipelineTests(unittest.TestCase):
    """The extraction-to-rectangles chain, called the way the viewer calls it.

    Every link on a PDF goes through these three functions in order, and the
    viewer swallows any exception from them into an empty result — a mismatch
    anywhere along the chain shows up not as a crash but as a page with no blue
    on it.  Running the real chain end to end is what catches that.
    """

    @classmethod
    def setUpClass(cls):
        import citations
        cls.ns = _load(
            "_degenerate_ocr_metrics", "_page_has_repairable_ocr_text",
            "_repair_degenerate_ocr_page",
            "_union_line_runs", "_extract_pdf_text_and_style",
            "_extract_pdf_text_pages", "_citation_links_from_pages",
            "_page_has_scan_background", "_pdf_ocr_scan_pages",
            "_citation_links_from_visible_pdf_text",
            "_detect_pdf_citation_links",
            consts=("_FONT_FLAG_ITALIC",),
        )
        cls.ns["detect_brief_links"] = citations.detect_links
        cls.pdf = _minimal_pdf("See Roe v. Wade, 410 U.S. 113, 152 (1973).")

    def test_the_extractor_returns_text_and_styling_in_step(self):
        pages, italics = self.ns["_extract_pdf_text_and_style"](self.pdf)
        self.assertEqual(len(pages), len(italics))
        for chars, slants in zip(pages, italics):
            self.assertEqual(len(chars), len(slants))

    def test_the_one_value_extractor_still_returns_just_pages(self):
        pages = self.ns["_extract_pdf_text_pages"](self.pdf)
        self.assertTrue(all(len(c) == 2 for c in pages[0]),
                        "consumers unpack (char, box)")

    def test_the_viewer_chain_produces_rectangles(self):
        pages, italics = self.ns["_extract_pdf_text_and_style"](self.pdf)
        links, quiet = self.ns["_citation_links_from_visible_pdf_text"](
            self.pdf, pages, italics)
        self.assertTrue(sum(len(v) for v in links.values()),
                        "no citation rectangles — nothing would turn blue")
        self.assertEqual(quiet, set())

    def test_styling_is_optional_all_the_way_down(self):
        pages = self.ns["_extract_pdf_text_pages"](self.pdf)
        links, _quiet = self.ns["_citation_links_from_visible_pdf_text"](
            self.pdf, pages)
        self.assertTrue(sum(len(v) for v in links.values()))

    def test_the_convenience_wrapper_agrees(self):
        direct = self.ns["_detect_pdf_citation_links"](self.pdf)
        self.assertTrue(sum(len(v) for v in direct.values()))


@unittest.skipUnless(HAVE_PDFIUM, "pypdfium2 not installed")
class GovInfoHiddenTextTests(unittest.TestCase):
    """The image-backed 2012 GovInfo format remains searchable and colored."""

    @classmethod
    def setUpClass(cls):
        import citations
        cls.ns = _load(
            "_degenerate_ocr_metrics", "_page_has_repairable_ocr_text",
            "_repair_degenerate_ocr_page",
            "_union_line_runs", "_extract_pdf_text_and_style",
            "_citation_links_from_pages", "_page_has_scan_background",
            "_pdf_ocr_scan_pages", "_citation_links_from_visible_pdf_text",
            consts=("_FONT_FLAG_ITALIC",),
        )
        cls.ns["detect_brief_links"] = citations.detect_links
        cls.pdf = _govinfo_ocr_pdf()
        cls.pages, cls.italics = cls.ns["_extract_pdf_text_and_style"](cls.pdf)

    def test_letter_spacing_is_repaired(self):
        text = "".join(ch for ch, _box in self.pages[0])
        self.assertIn("Cite as: 567 U. S. 519", text)
        self.assertIn("Roe v. Wade, 410 U. S. 113", text)
        self.assertNotIn("C i t e", text)

    def test_baseline_boxes_are_expanded_to_the_printed_line(self):
        heights = [
            box[3] - box[1]
            for ch, box in self.pages[0]
            if box is not None and ch.strip()
        ]
        self.assertTrue(heights)
        self.assertGreater(min(heights), 5.0)

    def test_repair_keeps_text_and_style_parallel(self):
        self.assertEqual(len(self.pages[0]), len(self.italics[0]))

    def test_links_are_found_and_colored_not_quiet(self):
        links, quiet = self.ns["_citation_links_from_visible_pdf_text"](
            self.pdf, self.pages, self.italics
        )
        self.assertTrue(sum(len(value) for value in links.values()))
        self.assertEqual(quiet, set())

    def test_us_reports_citation_is_recovered_for_pagination(self):
        import opinion_location
        self.assertEqual(
            opinion_location.us_reports_cite_from_pdf(self.pages),
            "567 U.S. 519",
        )


# ---------------------------------------------------------------------------
# Two opinions beginning on one page
# ---------------------------------------------------------------------------
#
# 71 U.S. 2 is where both Brobst v. Brobst and Ex parte Milligan begin.  Each
# collection files the second alongside the first — the LOC appends a letter,
# GovInfo a number — and only the case name inside each PDF says which is
# which, so a citation on its own would always have opened Brobst.

PAGE_NS = _load(
    "_us_reports_loc_url", "_us_reports_govinfo_url", "_us_reports_sibling_url",
    "_pdf_title", "_us_reports_title_name", "_us_reports_pdf_case_name",
    "_us_reports_page_opinions", "_us_reports_page_choice",
    "_normalized_us_cite", "_UsReportsPageOpinion", "_CaseLawPdfChoice",
    # The real name matcher and everything under it, so two captions are told
    # apart here exactly as they are everywhere else in the app.
    "_match_page_opinion", "_match_tier", "_name_match_score", "_name_parties",
    "_name_tokens", "_party_overlap", "_is_acronym_of", "_is_common_party",
    "_token_close",
    consts=("_US_CITE_RE", "_LOC_CUTOFF", "_GOVINFO_MAX", "_PDF_TITLE_RE",
            "_PDF_STRING_ESCAPES", "_LOC_PAGE_SUFFIXES", "_MAX_PAGE_OPINIONS",
            "_LOC_SIBLING_URL_RE", "_LOC_USREP_URL_RE", "_GOVINFO_USREP_URL_RE",
            "_us_reports_page_lock",
            "_us_reports_page_cache", "_NAME_STOPWORDS", "_NAME_MATCH_MIN",
            "_NAME_PARTY_SPLIT_RE", "_US_PARTY_RE", "_MAX_CLEAN_PARTY",
            "_COMMON_PARTY_NAMES"),
    extra={"threading": threading, "dataclass": dataclasses.dataclass,
           "Optional": typing.Optional, "difflib": difflib,
           "_STATE_COURTS": STATE_COURTS,      # the frequent-party names
           "print": lambda *a, **k: None},
)

#: The real document information dictionaries, as the two collections write
#: them — the LOC gives the whole citation, GovInfo the caption alone.
LOC_INFO = (
    b"5 0 obj\n<</Title(U.S. Reports: Brobst et al. v. Brobst, 71 U.S. "
    b"\\(4 Wall.\\) 2 \\(1867\\).)/Author(Supreme Court of the United States)"
    b"/Subject(U.S. Reports Volume 71; Wallace Volume 4)>>"
)
GOVINFO_INFO = (
    b"<</Producer(ABBYY FineReader 15; modified using iText)"
    b"/Title(Brobst et al. v. Brobst)>>"
)

LOC_FIRST = ("https://cdn.loc.gov/service/ll/usrep/usrep071/"
             "usrep071002/usrep071002.pdf")
LOC_SECOND = ("https://cdn.loc.gov/service/ll/usrep/usrep071/"
              "usrep071002a/usrep071002a.pdf")
GOV_FIRST = ("https://www.govinfo.gov/content/pkg/USREPORTS-71/pdf/"
             "USREPORTS-71-2.pdf")
GOV_SECOND = ("https://www.govinfo.gov/content/pkg/USREPORTS-71/pdf/"
              "USREPORTS-71-2-2.pdf")


def _scan(title: str, pad: int = 0) -> bytes:
    """A stand-in scan carrying *title* in its information dictionary, with
    *pad* bytes of page data in front of it."""
    return (b"%PDF-1.4\n" + b"\x00" * pad
            + b"<</Title(" + title.encode() + b")>>\ntrailer\n")


class _FakeCollection:
    """The collections' HTTP surface: HEAD says what exists, GET hands back
    the scan (or the first bytes of it, for a range request)."""

    def __init__(self, bodies):
        self.bodies = dict(bodies)
        self.heads: list = []
        self.gets: list = []

    def head(self, url, **_kw):
        self.heads.append(url)
        return SimpleNamespace(
            status_code=200 if url in self.bodies else 404)

    def get(self, url, headers=None, **_kw):
        self.gets.append((url, dict(headers or {})))
        body = self.bodies.get(url)
        if body is None:
            return SimpleNamespace(status_code=404, content=b"")
        if "Range" in (headers or {}):
            return SimpleNamespace(status_code=206, content=body[:262144])
        return SimpleNamespace(status_code=200, content=body)


class SiblingUrlTests(unittest.TestCase):
    """Where each collection files a second opinion off the same page."""

    def test_the_loc_appends_a_letter(self):
        self.assertEqual(PAGE_NS["_us_reports_loc_url"]("71 U.S. 2"), LOC_FIRST)
        self.assertEqual(PAGE_NS["_us_reports_sibling_url"](LOC_FIRST, 2),
                         LOC_SECOND)
        self.assertEqual(PAGE_NS["_us_reports_sibling_url"](LOC_FIRST, 3),
                         LOC_SECOND.replace("002a", "002b"))

    def test_govinfo_appends_a_number(self):
        self.assertEqual(
            PAGE_NS["_us_reports_govinfo_url"]("71 U.S. 2")[1], GOV_FIRST)
        self.assertEqual(PAGE_NS["_us_reports_sibling_url"](GOV_FIRST, 2),
                         GOV_SECOND)
        self.assertEqual(PAGE_NS["_us_reports_sibling_url"](GOV_FIRST, 3),
                         GOV_SECOND[:-5] + "3.pdf")

    def test_the_sibling_comes_from_the_host_the_scan_did(self):
        # tile.loc.gov and cdn.loc.gov serve the same files by different
        # paths; a sibling must be fetched from wherever the first one was.
        tile = ("https://tile.loc.gov/storage-services/service/ll/usrep/"
                "usrep071/usrep071002/usrep071002.pdf")
        self.assertEqual(
            PAGE_NS["_us_reports_sibling_url"](tile, 2),
            tile.replace("usrep071002/usrep071002.pdf",
                         "usrep071002a/usrep071002a.pdf"))

    def test_the_letters_run_out_before_the_pages_do(self):
        self.assertIsNone(PAGE_NS["_us_reports_sibling_url"](LOC_FIRST, 99))
        self.assertIsNone(PAGE_NS["_us_reports_sibling_url"](LOC_SECOND, 2))
        self.assertIsNone(
            PAGE_NS["_us_reports_sibling_url"]("https://example.test/x.pdf", 2))


class PdfTitleTests(unittest.TestCase):
    """Reading a scan's own title out of its bytes."""

    def test_the_loc_gives_the_citation_with_the_name(self):
        title = PAGE_NS["_pdf_title"](LOC_INFO)
        self.assertEqual(
            title,
            "U.S. Reports: Brobst et al. v. Brobst, 71 U.S. (4 Wall.) 2 (1867).")
        self.assertEqual(PAGE_NS["_us_reports_title_name"](title),
                         "Brobst et al. v. Brobst")

    def test_govinfo_gives_the_caption_alone(self):
        title = PAGE_NS["_pdf_title"](GOVINFO_INFO)
        self.assertEqual(title, "Brobst et al. v. Brobst")
        self.assertEqual(PAGE_NS["_us_reports_title_name"](title),
                         "Brobst et al. v. Brobst")

    def test_a_caption_of_its_own_commas_survives_the_citation_being_cut(self):
        self.assertEqual(
            PAGE_NS["_us_reports_title_name"](
                "U.S. Reports: Smith, Executor v. Jones, 71 U.S. (4 Wall.) 9."),
            "Smith, Executor v. Jones")

    def test_a_hexadecimal_title(self):
        self.assertEqual(
            PAGE_NS["_pdf_title"](b"/Title<45782070617274652E>"),
            "Ex parte.")

    def test_a_utf_16_title(self):
        self.assertEqual(
            PAGE_NS["_pdf_title"](b"/Title<FEFF00450078002000700061007200740065>"),
            "Ex parte")

    def test_an_octal_escape(self):
        self.assertEqual(PAGE_NS["_pdf_title"](rb"/Title(Ex\040parte)"),
                         "Ex parte")

    def test_an_escape_that_means_nothing_stands_for_itself(self):
        self.assertEqual(PAGE_NS["_pdf_title"](rb"/Title(Ex\ parte\z)"),
                         "Ex partez")

    def test_a_line_continuation_closes_up(self):
        self.assertEqual(PAGE_NS["_pdf_title"](b"/Title(Ex par\\\nte)"),
                         "Ex parte")

    def test_a_title_with_parentheses_of_its_own(self):
        self.assertEqual(
            PAGE_NS["_pdf_title"](rb"/Title(Brobst \(4 Wall.\) 2)"),
            "Brobst (4 Wall.) 2")

    def test_a_title_cut_off_by_a_short_read_is_no_title(self):
        # A range request that stopped mid-string must not yield half a name.
        self.assertEqual(PAGE_NS["_pdf_title"](LOC_INFO[:40]), "")

    def test_and_neither_is_a_scan_without_one(self):
        self.assertEqual(PAGE_NS["_pdf_title"](b"%PDF-1.4\ntrailer\n"), "")


class PageOpinionTests(unittest.TestCase):
    """Discovering that a page begins more than one opinion, and naming them."""

    def setUp(self):
        PAGE_NS["_us_reports_page_cache"].clear()

    def _run(self, bodies, url=LOC_FIRST, cite="71 U.S. 2"):
        session = _FakeCollection(bodies)
        PAGE_NS["_anon_session"] = session
        return PAGE_NS["_us_reports_page_opinions"](cite, url), session

    def test_the_ordinary_page_costs_one_head_and_nothing_else(self):
        opinions, session = self._run({LOC_FIRST: _scan("Brobst v. Brobst")})
        self.assertEqual(opinions, [])
        self.assertEqual(session.heads, [LOC_SECOND])
        self.assertEqual(session.gets, [])

    def test_a_second_opinion_is_found_and_both_are_named(self):
        opinions, _s = self._run({
            LOC_FIRST: _scan("U.S. Reports: Brobst et al. v. Brobst, "
                             "71 U.S. (4 Wall.) 2 (1867)."),
            LOC_SECOND: _scan("U.S. Reports: Ex parte Milligan, "
                              "71 U.S. (4 Wall.) 2 (1866)."),
        })
        self.assertEqual([o.url for o in opinions], [LOC_FIRST, LOC_SECOND])
        self.assertEqual([o.name for o in opinions],
                         ["Brobst et al. v. Brobst", "Ex parte Milligan"])

    def test_govinfo_files_the_same_page_its_own_way(self):
        opinions, session = self._run(
            {GOV_FIRST: _scan("Brobst et al. v. Brobst"),
             GOV_SECOND: _scan("Ex parte Milligan")},
            url=GOV_FIRST)
        self.assertEqual([o.name for o in opinions],
                         ["Brobst et al. v. Brobst", "Ex parte Milligan"])
        self.assertEqual(session.heads[0], GOV_SECOND)

    def test_the_probe_stops_at_the_first_one_that_is_not_there(self):
        third = LOC_SECOND.replace("002a", "002b")
        opinions, session = self._run({
            LOC_FIRST: _scan("One v. One"),
            LOC_SECOND: _scan("Two v. Two"),
        })
        self.assertEqual(len(opinions), 2)
        self.assertEqual(session.heads, [LOC_SECOND, third])

    def test_the_title_is_read_from_the_head_of_the_scan(self):
        _opinions, session = self._run({
            LOC_FIRST: _scan("Brobst v. Brobst"),
            LOC_SECOND: _scan("Ex parte Milligan"),
        })
        self.assertTrue(all("Range" in headers for _u, headers in session.gets))

    def test_a_scan_that_keeps_its_title_further_in_is_fetched_whole(self):
        opinions, session = self._run({
            LOC_FIRST: _scan("Brobst v. Brobst"),
            LOC_SECOND: _scan("Ex parte Milligan", pad=300000),
        })
        self.assertEqual(opinions[1].name, "Ex parte Milligan")
        self.assertEqual([headers.get("Range", "whole")
                          for url, headers in session.gets if url == LOC_SECOND],
                         ["bytes=0-262143", "whole"])

    def test_a_sibling_does_not_go_looking_for_siblings_of_its_own(self):
        opinions, session = self._run({LOC_SECOND: _scan("Ex parte Milligan")},
                                      url=LOC_SECOND)
        self.assertEqual(opinions, [])
        self.assertEqual(session.heads, [])

    def test_the_answer_is_kept_so_the_page_is_only_read_once(self):
        bodies = {LOC_FIRST: _scan("One v. One"), LOC_SECOND: _scan("Two v. Two")}
        first, _s = self._run(bodies)
        second, session = self._run(bodies)
        self.assertEqual([o.name for o in second], [o.name for o in first])
        self.assertEqual(session.heads, [])
        self.assertEqual(session.gets, [])


class WhichOpinionTests(unittest.TestCase):
    """Which of two opinions on one page a citation opens."""

    def setUp(self):
        PAGE_NS["_us_reports_page_cache"].clear()
        PAGE_NS["_anon_session"] = _FakeCollection({
            LOC_FIRST: _scan("U.S. Reports: Brobst et al. v. Brobst, "
                             "71 U.S. (4 Wall.) 2 (1867)."),
            LOC_SECOND: _scan("U.S. Reports: Ex parte Milligan, "
                              "71 U.S. (4 Wall.) 2 (1866)."),
        })

    def _choose(self, name):
        return PAGE_NS["_us_reports_page_choice"](
            "71 U.S. 2", LOC_FIRST, name)

    def test_the_case_being_opened_picks_its_own_scan(self):
        url, choices = self._choose("Ex parte Milligan")
        self.assertEqual(url, LOC_SECOND)
        self.assertEqual(choices, [])

    def test_and_the_other_one_gets_the_first(self):
        url, choices = self._choose("Brobst v. Brobst")
        self.assertEqual(url, LOC_FIRST)
        self.assertEqual(choices, [])

    def test_a_courtlistener_style_caption_still_matches(self):
        self.assertEqual(self._choose("MILLIGAN, EX PARTE")[0], LOC_SECOND)

    def test_a_name_that_matches_neither_offers_both(self):
        url, choices = self._choose("Ex parte Merryman")
        self.assertEqual(url, LOC_FIRST)   # as the citation always opened
        self.assertEqual([c.url for c in choices], [LOC_FIRST, LOC_SECOND])
        self.assertEqual([c.label for c in choices],
                         ["71 U.S. 2 — Brobst et al. v. Brobst",
                          "71 U.S. 2 — Ex parte Milligan"])
        self.assertEqual({c.cite for c in choices}, {"71 U.S. 2"})

    def test_and_so_does_a_record_with_no_name_at_all(self):
        url, choices = self._choose("")
        self.assertEqual(url, LOC_FIRST)
        self.assertEqual(len(choices), 2)

    def test_a_third_opinion_with_no_name_is_still_offered(self):
        third = LOC_SECOND.replace("002a", "002b")
        PAGE_NS["_us_reports_page_cache"].clear()
        PAGE_NS["_anon_session"] = _FakeCollection({
            LOC_FIRST: _scan("Brobst et al. v. Brobst"),
            LOC_SECOND: _scan("Ex parte Milligan"),
            third: b"%PDF-1.4\ntrailer\n",     # no title in it
        })
        url, choices = self._choose("Ex parte Merryman")
        self.assertEqual(url, LOC_FIRST)
        self.assertEqual([c.label for c in choices],
                         ["71 U.S. 2 — Brobst et al. v. Brobst",
                          "71 U.S. 2 — Ex parte Milligan",
                          "71 U.S. 2 — Opinion 3"])

    def test_a_second_scan_that_names_no_case_is_no_second_scan(self):
        # Nothing could be chosen between, and a source that answers 200 to
        # anything would otherwise invent a whole page of opinions.
        PAGE_NS["_us_reports_page_cache"].clear()
        PAGE_NS["_anon_session"] = _FakeCollection({
            LOC_FIRST: _scan("Brobst et al. v. Brobst"),
            LOC_SECOND: b"%PDF-1.4\ntrailer\n",
        })
        self.assertEqual(self._choose("Ex parte Milligan"), (LOC_FIRST, []))

    def test_a_source_that_says_yes_to_everything_is_not_believed(self):
        PAGE_NS["_us_reports_page_cache"].clear()

        class _Yes:
            heads: list = []

            def head(self, url, **_kw):
                _Yes.heads.append(url)
                return SimpleNamespace(status_code=200)

            def get(self, _url, headers=None, **_kw):
                return SimpleNamespace(status_code=200, content=b"%PDF-1.4\n")

        PAGE_NS["_anon_session"] = _Yes()
        self.assertEqual(self._choose("Ex parte Milligan"), (LOC_FIRST, []))
        self.assertEqual(len(_Yes.heads), 1)   # it gave up after the first

    def test_the_page_of_one_opinion_is_left_exactly_as_it_was(self):
        PAGE_NS["_us_reports_page_cache"].clear()
        PAGE_NS["_anon_session"] = _FakeCollection(
            {LOC_FIRST: _scan("Brobst v. Brobst")})
        self.assertEqual(self._choose("Brobst v. Brobst"), (LOC_FIRST, []))


if __name__ == "__main__":
    unittest.main()
