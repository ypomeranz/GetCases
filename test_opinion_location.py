"""Pure location matching between text opinions and reporter PDFs."""

from types import SimpleNamespace
import unittest

from opinion_location import (
    OpinionLocationMap,
    PageBoundary,
    TextAddress,
    align_opinion_locations,
    build_plain_text_source,
    build_text_source,
    us_reports_cite_from_pdf,
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

    def test_westlaw_star_pin_is_not_mistaken_for_pagination(self):
        raw = "The related order appears at 2010 WL 123456, at *23."
        source = build_plain_text_source(raw, "12 F.4th 22")

        self.assertIn("at *23", source.text)
        self.assertEqual([page.page for page in source.native_pages], [22])

    def test_reporter_page_tie_uses_later_physical_page_at_boundary(self):
        source = build_plain_text_source("alpha")
        location_map = OpinionLocationMap(
            source=source,
            anchors=(),
            boundaries=(
                PageBoundary(0, 10, 0, source.address_at(0), 0.5),
                PageBoundary(1, 11, 0, source.address_at(0), 0.5),
            ),
            confidence=0.5,
            native_cite="",
            pdf_cite="590 U.S. 10",
            us_cite="590 U.S. 10",
            copy_ready=True,
            copy_cite="590 U.S. 10",
        )

        self.assertEqual(location_map.reporter_page(0), 11)


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

    def test_full_width_running_head_does_not_disable_two_columns(self):
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
        page = _glyphs(
            "FULL WIDTH REPORTER RUNNING HEAD ACROSS THE ENTIRE PAGE " * 2,
            72.0, 735.0, size=10.0,
        )
        page.extend(_two_column_interleaved(left, right))

        location_map = align_opinion_locations(source, [page])

        self.assertEqual(
            location_map.pdf_location(
                source.text.index("foxtrot mandate")
            ).pdf_page,
            0,
        )
        self.assertGreater(location_map.confidence, 0.6)

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

    def test_unmatched_middle_page_is_interpolated_for_navigation_and_pin(self):
        first = (
            "alpha opening presents a distinctive jurisdictional question "
            "under federal law and the record"
        )
        middle = (
            "bravo middle analysis carefully applies the governing statutory "
            "framework to disputed evidence"
        )
        last = (
            "charlie conclusion resolves every remaining claim and affirms "
            "the judgment of the court below"
        )
        source = build_plain_text_source(
            " ".join((first, middle, last)), "140 S. Ct. 100"
        )
        location_map = align_opinion_locations(
            source,
            [
                _page([first]),
                _page(["zzqx vvjk qqqx pppz nnmx kkqz " * 3]),
                _page([last]),
            ],
            pdf_cite="590 U.S. 10",
            us_cite="590 U.S. 10",
        )

        middle_boundary = next(
            row for row in location_map.boundaries if row.pdf_page == 1
        )
        self.assertEqual(middle_boundary.reporter_page, 11)
        self.assertIsNotNone(location_map.text_location(1, 700.0))
        self.assertEqual(
            location_map.reporter_page(source.text.index("bravo")), 11
        )

    def test_single_middle_match_cannot_enable_inferred_reporter_pins(self):
        blocks = [
            (
                f"section {number} contains distinctive analysis concerning "
                f"issue token{number} and the governing federal rule"
            )
            for number in range(5)
        ]
        source = build_plain_text_source(
            " ".join(blocks), "140 S. Ct. 100"
        )
        pages = [
            _page([f"unmatched OCR noise {number} qzxv jkkp"])
            for number in range(5)
        ]
        pages[2] = _page([blocks[2]])

        location_map = align_opinion_locations(
            source,
            pages,
            pdf_cite="590 U.S. 10",
            us_cite="590 U.S. 10",
        )

        self.assertEqual(
            [row.pdf_page for row in location_map.boundaries], [2]
        )
        self.assertEqual(
            location_map.reporter_page(source.text.index("section 2")), 12
        )
        self.assertFalse(location_map.navigation_ready)
        self.assertFalse(location_map.copy_ready)

    def test_unmatched_physical_edges_cannot_enable_inferred_pins(self):
        blocks = [
            (
                f"section {number} contains distinctive appellate analysis "
                f"of issue token{number} and the governing federal standard"
            )
            for number in range(5)
        ]
        source = build_plain_text_source(
            " ".join(blocks), "140 S. Ct. 100"
        )
        pages = [
            _page([f"unmatched edge OCR {number} qzxv jkkp"])
            for number in range(5)
        ]
        for index in (1, 2, 3):
            pages[index] = _page([blocks[index]])

        location_map = align_opinion_locations(
            source,
            pages,
            pdf_cite="590 U.S. 10",
            us_cite="590 U.S. 10",
        )

        self.assertEqual(location_map.matched_pdf_pages, (1, 2, 3))
        self.assertFalse(location_map.physical_edges_covered)
        self.assertFalse(location_map.navigation_ready)
        self.assertFalse(location_map.copy_ready)

    def test_long_interpolated_run_can_navigate_but_cannot_supply_pins(self):
        blocks = [
            (
                f"section {number} gives a distinctive discussion of federal "
                f"issue token{number} and applies the controlling legal rule "
                "to the appellate record"
            )
            for number in range(10)
        ]
        source = build_plain_text_source(
            " ".join(blocks), "140 S. Ct. 100"
        )
        pages = [
            _page([f"unmatched interior OCR {number} qzxv jkkp"])
            for number in range(10)
        ]
        pages[0] = _page([blocks[0]])
        pages[9] = _page([blocks[9]])

        location_map = align_opinion_locations(
            source,
            pages,
            pdf_cite="590 U.S. 10",
            us_cite="590 U.S. 10",
        )

        self.assertTrue(location_map.navigation_ready)
        self.assertEqual(location_map.matched_pdf_pages, (0, 9))
        self.assertEqual(location_map.max_unmatched_pages, 8)
        self.assertFalse(location_map.copy_ready)

    def test_collected_footnote_keeps_its_physical_page_navigation(self):
        first = (
            "alpha body explains the distinctive statutory question and "
            "the governing standard"
        )
        second = (
            "bravo body applies that standard to the evidentiary record and "
            "affirms the judgment"
        )
        note_text = (
            "footnote discussion collects additional historical authorities "
            "supporting the first pages analysis"
        )
        part = SimpleNamespace(
            blocks=[
                SimpleNamespace(spans=[_span(first)]),
                SimpleNamespace(spans=[_span(second)]),
            ],
            footnotes=[
                SimpleNamespace(spans=[_span(note_text)])
            ],
        )
        source = build_text_source([part])
        first_page = _page([first])
        first_page.extend(_page([note_text], top=100.0))
        location_map = align_opinion_locations(
            source,
            [first_page, _page([second])],
        )

        note_offset = source.text.index("footnote discussion")
        self.assertEqual(
            location_map.pdf_location(note_offset).pdf_page, 0
        )
        returned = location_map.text_location(0, 110.0)
        self.assertIsNotNone(returned)
        self.assertTrue(returned.address.footnote)

    def test_repeated_running_head_is_not_a_dense_return_anchor(self):
        body = [
            "alpha opening explains the unique question presented under "
            "federal statutory law",
            "bravo analysis contains enough distinctive words and applies "
            "the doctrine carefully",
            "charlie third page discussion is uniquely situated later in "
            "the source document",
        ]
        source = build_text_source([
            _part(*[(_span(value),) for value in body])
        ])
        location_map = align_opinion_locations(
            source,
            [
                _page([body[0]]),
                _page([body[0], body[1]], step=100.0),
                _page([body[2]]),
            ],
        )

        returned = location_map.text_location(1, 705.0)
        self.assertIsNotNone(returned)
        self.assertGreaterEqual(returned.source_offset, source.text.index("bravo"))

    def test_exact_boundary_wins_when_page_has_only_a_repeated_head(self):
        first = (
            "alpha opening explains the unique question presented under "
            "federal statutory law"
        )
        second = (
            "bravo analysis applies the governing doctrine to the record"
        )
        third = (
            "charlie conclusion affirms the lower courts final judgment"
        )
        source = build_text_source(
            [_part((
                _span(first + " "),
                _span("*899", pagenum=True),
                _span(second + " "),
                _span("*900", pagenum=True),
                _span(third),
            ))],
            "754 F.2d 898",
        )
        location_map = align_opinion_locations(
            source,
            [
                _page([first]),
                _page([second]),
                _page([first]),  # OCR retained only the repeated running head
            ],
            pdf_cite="754 F.2d 898",
        )

        returned = location_map.text_location(2, 710.0)
        self.assertIsNotNone(returned)
        self.assertGreaterEqual(
            returned.source_offset, source.text.index(third)
        )

    def test_partial_exact_pagination_still_repairs_a_fuzzy_reversal(self):
        body = [
            "alpha opening explains the question presented and governing "
            "federal law",
            "bravo analysis applies governing law to the distinctive record "
            "before us",
            "charlie discussion considers the remaining unusual "
            "constitutional claim",
            "delta conclusion affirms the judgment entered by the lower court",
        ]
        source = build_text_source(
            [_part(*[(_span(value),) for value in body])],
            "754 F.2d 898",
        )
        location_map = align_opinion_locations(
            source,
            [
                _page([body[0]]),
                _page([body[1]]),
                _page([body[0]]),
                _page([body[3]]),
            ],
            pdf_cite="754 F.2d 898",
        )

        rows = sorted(location_map.boundaries, key=lambda row: row.pdf_page)
        offsets = [row.source_offset for row in rows]
        self.assertEqual([row.pdf_page for row in rows], [0, 1, 2, 3])
        self.assertEqual(offsets, sorted(offsets))
        self.assertTrue(rows[0].exact)
        self.assertGreaterEqual(
            rows[2].source_offset, source.text.index("bravo")
        )

    def test_leading_fuzzy_outlier_is_dropped_instead_of_clamped(self):
        body = [
            "alpha first trustworthy page begins the courts analysis of the "
            "distinctive federal question",
            "bravo second trustworthy page applies the rule to this record",
            "charlie final trustworthy page affirms the judgment below",
        ]
        source = build_plain_text_source(
            " ".join(body), "140 S. Ct. 100"
        )
        location_map = align_opinion_locations(
            source,
            [
                _page([body[2]]),  # false early match to text near the end
                _page([body[0]]),
                _page([body[1]]),
                _page([body[2]]),
            ],
            pdf_cite="590 U.S. 10",
            us_cite="590 U.S. 10",
        )

        rows = sorted(location_map.boundaries, key=lambda row: row.pdf_page)
        self.assertEqual([row.pdf_page for row in rows], [1, 2, 3])
        self.assertEqual(
            location_map.reporter_page(source.text.index("alpha")), 11
        )
        self.assertFalse(location_map.navigation_ready)
        self.assertFalse(location_map.copy_ready)

    def test_collected_footnote_star_cannot_replace_first_page_anchor(self):
        body = SimpleNamespace(spans=[_span(
            "The opening body discusses the controlling rule and applies it "
            "to the record before the Court."
        )])
        note = SimpleNamespace(spans=[
            _span("*898", pagenum=True),
            _span("This collected note supplies additional authority."),
        ])
        part = SimpleNamespace(blocks=[body], footnotes=[note])
        source = build_text_source([part], "754 F.2d 898")
        location_map = align_opinion_locations(
            source,
            [_page(["The opening body discusses the controlling rule"])],
            pdf_cite="754 F.2d 898",
        )

        first = location_map.boundaries[0]
        self.assertTrue(first.exact)
        self.assertFalse(first.address.footnote)

    def test_a_preliminary_print_supplies_its_own_citation(self):
        # Clark v. Sweeney, 607 U. S. 7 (2025), published as a preliminary
        # print months before the bound volume.  The Reporter revises the
        # pagination precisely so it can be cited, and prints the citation in
        # the running head of every interior recto.  Nothing else knows it.
        pages = [
            _page(["OCTOBER TERM, 2025 7", "Syllabus", "CLARK v. SWEENEY"]),
            _page(["8 CLARK v. SWEENEY", "Per Curiam", "A Maryland jury"]),
            _page(["Cite as: 607 U. S. 7 (2025) 9", "Per Curiam",
                   "U. S. C. 2254 in Federal District Court."]),
        ]
        self.assertEqual(us_reports_cite_from_pdf(pages), "607 U.S. 7")

    def test_a_preliminary_prints_cover_names_the_volume_too(self):
        # Before the cover is stripped, it says the same thing — and it is
        # all a two-page opinion has, since "Cite as:" first appears on the
        # opinion's second recto.
        pages = [
            _page(["PRELIMINARY PRINT", "Volume 607 U. S. Part 1",
                   "Pages 7-10", "OFFICIAL REPORTS"]),
            _page(["OCTOBER TERM, 2025 7", "Syllabus"]),
        ]
        self.assertEqual(us_reports_cite_from_pdf(pages), "607 U.S. 7")

    def test_a_reporters_note_trailing_the_opinion_is_not_a_failed_page(self):
        # A draft reporter appends a "Reporter's Note" that constitutes no
        # part of the opinion and matches nothing in it.  Requiring the last
        # physical sheet to match took the page switching and the pin cites
        # down with it — Bost v. Illinois State Bd. of Elections, 607 U. S.
        # 71, is 36 pages of opinion and one of that note.
        body = [
            "The candidates challenged the procedure for counting mail-in "
            "ballots received after election day in this matter.",
            "We consider whether those candidates have standing to maintain "
            "their suit against the board of elections here.",
            "Reputational harms are classic Article III injuries and are "
            "concrete for those whose jobs depend on public support.",
        ]
        source = build_plain_text_source(" ".join(body), "607 U. S. 71")
        pages = [_page([line]) for line in body]
        pages.append(_page([
            "Reporter's Note",
            "The attached opinion has been revised to reflect the usual "
            "publication and citation style of the United States Reports.",
            "The syllabus has been prepared by the Reporter of Decisions and "
            "constitutes no part of the opinion of the Court.",
        ]))
        location_map = align_opinion_locations(
            source, pages, pdf_cite="607 U. S. 71", us_cite="607 U. S. 71")

        self.assertTrue(location_map.navigation_ready)
        self.assertEqual(location_map.copy_cite, "607 U. S. 71")
        self.assertEqual(
            [b.reporter_page for b in location_map.boundaries], [71, 72, 73])

    def test_losing_the_opinions_own_last_pages_is_still_a_failure(self):
        # The tail allowance must not paper over an alignment that really did
        # lose the end of the opinion: there, the source's own end is
        # uncovered, and that is what the allowance turns on.
        body = [
            "The candidates challenged the procedure for counting mail-in "
            "ballots received after election day in this matter.",
            "We consider whether those candidates have standing to maintain "
            "their suit against the board of elections here.",
        ]
        tail = (" The remainder of this opinion runs on at considerable "
                "length about entirely unrelated statutory questions, none "
                "of which appears anywhere among the pages supplied, and it "
                "continues in that vein for a good while yet.") * 3
        source = build_plain_text_source(" ".join(body) + tail, "607 U. S. 71")
        pages = [_page([line]) for line in body]
        pages.append(_page(["Reporter's Note", "no part of the opinion"]))
        location_map = align_opinion_locations(
            source, pages, pdf_cite="607 U. S. 71", us_cite="607 U. S. 71")

        self.assertFalse(location_map.copy_ready)

    def test_a_pdf_that_prints_no_reporter_citation_yields_none(self):
        pages = [_page(["CLARK v. SWEENEY (2025) - per curiam",
                        "his right to trial by an impartial jury."])]
        self.assertEqual(us_reports_cite_from_pdf(pages), "")

    def test_counsel_listing_at_a_page_foot_does_not_swallow_the_next_page(self):
        # Carpenter v. United States, 585 U. S. 296: the reporter prints the
        # amici listings at the foot of page 300, where the opinion opens,
        # while the text version carries them up in the header.  Those
        # listings match nothing nearby, so they scatter one- and two-word
        # runs ("and", "of the", "for the") across the source — and the
        # furthest of them used to drag the frontier past almost the whole of
        # page 301, whose boundary then landed twenty lines down the page.
        header = (
            "Briefs of amici curiae were filed for the State of Alabama and "
            "for the National District Attorneys Association and for their "
            "several jurisdictions as follows and for the respondents. "
        )
        first = (
            "Chief Justice Roberts delivered the opinion of the Court. This "
            "case presents the question whether the Government conducts a "
            "search when it accesses historical cell phone records. "
        )
        second = (
            "phone's features. Each time the phone connects to a cell site "
            "it generates a time-stamped record known as cell-site location "
            "information. The precision of that information depends on the "
            "size of the area the cell site covers. "
        )
        third = (
            "several other suspects. That statute permits the Government to "
            "compel the disclosure of certain telecommunications records."
        )
        source = build_text_source(
            [_part((_span(header),)),
             _part((_span(first), _span(second), _span(third)))],
            "585 U. S. 296",
        )
        location_map = align_opinion_locations(
            source,
            [
                # Page 300 prints the opinion's opening above a foot of amici
                # listings; only the stray function words of those listings
                # find anything to match.
                _page([first, "and Denise M. Harle and Jordan E. Pratt of",
                       "Montana and for the several jurisdictions and"]),
                _page([second]),
                _page([third]),
            ],
            pdf_cite="585 U. S. 296",
            us_cite="585 U. S. 296",
        )

        starts = {
            b.reporter_page: source.text[b.source_offset:b.source_offset + 20]
            for b in location_map.boundaries
        }
        self.assertEqual(starts.get(297), second[:20])
        self.assertEqual(starts.get(298), third[:20])


if __name__ == "__main__":
    unittest.main()
