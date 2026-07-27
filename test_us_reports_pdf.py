"""U.S. Reports PDFs: the citation spelling, and the page-proof watermark.

Two things the app gets from a preliminary print off supremecourt.gov.  The
citation has to be recognised however it is spelled, since that is what names
the saved file; and the Reporter's "Page Proof Pending Publication" stamp has to
come off before the opinion is shown or printed.

Both are lifted out of ``courtlistener_gui`` with ``ast`` — the module imports
tkinter, which a headless run does not have.
"""

import ast
import pathlib
import re
import threading
import unittest

try:
    import pypdfium2  # noqa: F401
    HAVE_PDFIUM = True
except ImportError:  # pragma: no cover - depends on the environment
    HAVE_PDFIUM = False


def _load(*names, consts=()):
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    ns = {"re": re, "_PDFIUM_LOCK": threading.RLock()}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            target = node.targets[0]
            if getattr(target, "id", "") in consts:
                exec(ast.get_source_segment(src, node), ns)
    found = {n.name: ast.get_source_segment(src, n)
             for n in tree.body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found at module level: {missing}")
    for name in names:
        exec(found[name], ns)
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


if __name__ == "__main__":
    unittest.main()
