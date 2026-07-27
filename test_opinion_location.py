"""Pure location matching between text opinions and reporter PDFs."""

from types import SimpleNamespace
import unittest

from opinion_location import (
    TextAddress,
    align_opinion_locations,
    build_plain_text_source,
    build_text_source,
)


def _glyphs(text, x, y, size=10.0):
    out = []
    for index, char in enumerate(text):
        left = x + index * size * 0.5
        out.append((char, (left, y, left + size * 0.5, y + size)))
    return out


def _page(lines, x=72.0, top=700.0, step=14.0):
    chars = []
    for row, text in enumerate(lines):
        chars.extend(_glyphs(text, x, top - row * step))
    return chars


def _two_column_interleaved(left, right):
    """Content stream is row/right-first; reading order is column-major."""
    chars = []
    for row in range(max(len(left), len(right))):
        y = 700.0 - row * 14.0
        if row < len(right):
            chars.extend(_glyphs(right[row], 360.0, y, size=5.0))
        if row < len(left):
            chars.extend(_glyphs(left[row], 72.0, y, size=5.0))
    return chars


def _span(text, *, pagenum=False):
    return SimpleNamespace(text=text, pagenum=pagenum)


def _part(*blocks):
    return SimpleNamespace(
        blocks=[
            SimpleNamespace(spans=list(spans))
            for spans in blocks
        ],
        footnotes=[],
    )


class TextSourceTests(unittest.TestCase):
    def test_styled_page_markers_are_hidden_but_count_in_addresses(self):
        part = _part((
            _span("Alpha "),
            _span("*899", pagenum=True),
            _span("Beta"),
        ))
        source = build_text_source([part], "754 F.2d 898")

        self.assertNotIn("*899", source.text)
        beta_offset = source.text.index("Beta")
        address = source.address_at(beta_offset)
        self.assertEqual(address.block_offset, len("Alpha ") + len("*899"))
        self.assertEqual(source.offset_for(address), beta_offset)
        self.assertEqual([page.page for page in source.native_pages], [898, 899])

    def test_plain_page_markers_are_hidden_and_round_trip(self):
        raw = "Before *23 after the marker."
        source = build_plain_text_source(raw, "12 F.4th 22")

        self.assertNotIn("*23", source.text)
        offset = source.text.index("after")
        address = source.address_at(offset)
        self.assertEqual(address.block_offset, raw.index("after"))
        self.assertEqual(source.offset_for(address), offset)


class AlignmentTests(unittest.TestCase):
    def test_same_reporter_page_markers_are_hard_boundaries(self):
        first = (
            "The first reporter page discusses the statutory question and "
            "the governing standard of review."
        )
        second = (
            "The next reporter page applies that standard to the unusual "
            "facts and affirms the judgment."
        )
        source = build_text_source(
            [_part((
                _span(first + " "),
                _span("*899", pagenum=True),
                _span(second),
            ))],
            "754 F.2d 898",
        )
        location_map = align_opinion_locations(
            source,
            [_page([first]), _page([second])],
            pdf_cite="754 F.2d 898",
        )

        self.assertEqual(
            [(b.pdf_page, b.reporter_page, b.exact)
             for b in location_map.boundaries],
            [(0, 898, True), (1, 899, True)],
        )
        second_location = location_map.pdf_location(source.text.index(second))
        self.assertEqual(second_location.pdf_page, 1)

    def test_normalized_fuzzy_text_still_aligns(self):
        first = (
            "Congress adopted the provision to protect a person's settled "
            "expectations under federal law."
        )
        second = (
            "The constitutional analysis therefore requires careful review "
            "of history and longstanding practice."
        )
        source = build_plain_text_source(first + "\n\n" + second)
        pages = [
            _page([
                "RUNNING HEADER",
                "Congress adopted the pro-",
                "vision to protect a persons settled expectations under federal law.",
            ]),
            _page([
                "The constitutional analysis therefore requires careful revlew",
                "of history and long-standing practice.",
            ]),
        ]
        location_map = align_opinion_locations(source, pages)

        self.assertEqual(len(location_map.boundaries), 2)
        self.assertEqual(
            location_map.pdf_location(source.text.index("constitutional")).pdf_page,
            1,
        )
        self.assertGreater(location_map.confidence, 0.25)

    def test_dual_column_geometry_uses_column_reading_order(self):
        left = [
            "alpha tribunal considered jurisdiction and statutory authority",
            "bravo precedent supplied the controlling analytical framework",
            "charlie evidence established every necessary historical fact",
        ]
        right = [
            "delta conclusion follows from the foregoing legal principles",
            "echo application confirms the district courts final judgment",
            "foxtrot mandate shall issue without any additional proceedings",
        ]
        source = build_plain_text_source(" ".join(left + right))
        page = _two_column_interleaved(left, right)
        location_map = align_opinion_locations(source, [page])

        self.assertTrue(location_map.anchors)
        final_location = location_map.pdf_location(
            source.text.index("foxtrot mandate")
        )
        self.assertEqual(final_location.pdf_page, 0)
        self.assertGreater(location_map.confidence, 0.7)

    def test_sct_text_infers_us_reports_pages_for_copy(self):
        page_1663 = (
            "The Court granted certiorari to resolve the recurring question "
            "presented by the parties in this proceeding."
        )
        page_1664 = (
            "Our precedents establish the answer and require affirmance of "
            "the judgment entered by the court of appeals."
        )
        raw = f"*1663 {page_1663} *1664 {page_1664}"
        source = build_plain_text_source(raw, "138 S. Ct. 1663")
        location_map = align_opinion_locations(
            source,
            [_page([page_1663]), _page([page_1664])],
            pdf_cite="584 U.S. 586",
            us_cite="584 U.S. 586",
        )

        self.assertEqual(
            [page.page for page in source.native_pages], [1663, 1664]
        )
        self.assertEqual(
            [boundary.reporter_page for boundary in location_map.boundaries],
            [586, 587],
        )
        self.assertTrue(location_map.copy_ready)
        self.assertEqual(location_map.copy_cite, "584 U.S. 586")
        self.assertEqual(
            location_map.reporter_pages_for_range(0, len(source.text)),
            (586, 587),
        )

    def test_repeated_heads_and_collected_notes_do_not_reverse_pages(self):
        body = [
            "alpha opening explains the question presented and governing law",
            "bravo analysis applies the governing law to the record before us",
            "charlie discussion considers the remaining constitutional claim",
            "delta conclusion affirms the judgment entered by the lower court",
            "echo mandate closes the opinion and allocates the parties costs",
        ]
        blocks = [
            SimpleNamespace(spans=[_span(value)]) for value in body
        ]
        # Text renderers collect this note after the body, although the same
        # language occurs at the bottom of physical PDF page two.
        note = SimpleNamespace(spans=[_span(
            body[1] + " additional authorities are collected in this note"
        )])
        part = SimpleNamespace(blocks=blocks, footnotes=[note])
        source = build_text_source([part], "140 S. Ct. 100")
        pages = [
            _page([body[0]]),
            _page([body[1], body[1] + " additional authorities"]),
            # A repeated head matches text near the start, while OCR has lost
            # the page's body. Neighboring pages must repair that outlier.
            _page([body[0]]),
            _page([body[3]]),
            _page([body[4]]),
        ]
        location_map = align_opinion_locations(
            source, pages,
            pdf_cite="590 U.S. 10",
            native_cite="140 S. Ct. 100",
            us_cite="590 U.S. 10",
        )

        rows = sorted(location_map.boundaries, key=lambda row: row.pdf_page)
        offsets = [row.source_offset for row in rows]
        self.assertEqual(offsets, sorted(offsets))
        self.assertTrue(all(
            row.address is None or not row.address.footnote for row in rows
        ))


if __name__ == "__main__":
    unittest.main()
