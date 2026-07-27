"""Jumping to a separate opinion that begins partway down a PDF page.

The parts strip used to scroll to the top of the page a part starts on.  Where
the previous opinion runs on above it — the usual thing in a bound reporter —
that leaves the reader hunting for the line.  ``SlipSection.start_at`` carries
the height of the opening line for exactly that case, and these tests pin down
when it is offered and when it is not.

``slip_opinion`` is free of tkinter, so it runs headless as-is; the reader's
footnote-position lookup is lifted out of ``courtlistener_gui`` with ``ast``.
"""

import ast
import pathlib
import re
import typing
import unittest

import slip_opinion as so


def _glyphs(text: str, x: float, y: float, size: float = 10.0):
    """Glyph tuples for *text* laid out left to right at (x, y)."""
    out = []
    for i, ch in enumerate(text):
        left = x + i * size * 0.5
        out.append((ch, (left, y, left + size * 0.5, y + size)))
    return out


def _page(lines, x=72.0, top=700.0, step=14.0, size=10.0):
    """A page of glyphs from ``lines``, one text line per entry."""
    chars = []
    for n, text in enumerate(lines):
        chars.extend(_glyphs(text, x, top - n * step, size))
    return chars


def _two_column_page(left_lines, right_lines):
    chars = []
    chars.extend(_page(left_lines, x=72.0))
    chars.extend(_page(right_lines, x=330.0))
    return chars


class SingleColumnTests(unittest.TestCase):
    def test_a_full_measure_page_is_one_column(self):
        page = _page(["The Court holds that the statute is plain enough " * 2]
                     * 25)
        self.assertTrue(so.is_single_column(page))

    def test_a_page_with_a_centre_gutter_is_two_columns(self):
        page = _two_column_page(["left column text here"] * 20,
                                ["right column text here"] * 20)
        self.assertFalse(so.is_single_column(page))

    def test_a_nearly_empty_page_is_treated_as_one_column(self):
        self.assertTrue(so.is_single_column(_page(["short"])))

    def test_glyphs_without_boxes_are_ignored(self):
        page = [("x", None)] * 200
        self.assertTrue(so.is_single_column(page))


# A US Reports-style opinion: running head, body, and a concurrence opening
# partway down the third page with the majority's close still above it.
_MIDPAGE_DOC = [
    _page(["48 OCTOBER TERM, 2017",
           "Syllabus",
           "The syllabus text begins here and runs on for a while."]),
    _page(["Cite as: 583 U. S. 48 (2018) 49",
           "Opinion of the Court",
           "Justice Thomas delivered the opinion of the Court.",
           "The opinion runs on for several lines of body text here."]),
    # The running head names the new part from this page on, but the
    # majority's last lines are still above the attribution.
    _page(["50 DISTRICT OF COLUMBIA v. WESBY",
           "Opinion of Sotomayor, J.",
           "and the case is remanded for further proceedings.",
           "It is so ordered.",
           "Justice Sotomayor, concurring in the judgment.",
           "I agree with the majority that the officers are entitled."]),
]

# A slip opinion: each part opens its own page, under a divider rule.
_FRESH_PAGE_DOC = [
    _page(["(Slip Opinion) OCTOBER TERM, 2025",
           "Syllabus",
           "The syllabus text begins here and runs on for a while."]),
    _page(["Cite as: 609 U. S. ____ (2026) 1",
           "Opinion of the Court",
           "Justice Barrett delivered the opinion of the Court.",
           "The opinion runs on for several lines of body text here."]),
    _page(["Cite as: 609 U. S. ____ (2026) 1",
           "JACKSON, J., concurring",
           "SUPREME COURT OF THE UNITED STATES",
           "_________________",
           "No. 25-112",
           "_________________",
           "OKELLO T. CHATRIE, PETITIONER v. UNITED STATES",
           "ON WRIT OF CERTIORARI TO THE UNITED STATES COURT",
           "Justice Jackson, concurring.",
           "I join the opinion of the Court in full."]),
]


class StartAtTests(unittest.TestCase):
    def _sections(self, pages):
        return {s.kind: s for s in so.detect_sections(pages)}

    def test_a_part_opening_midpage_carries_its_line(self):
        secs = self._sections(_MIDPAGE_DOC)
        concur = secs.get("concurrence")
        self.assertIsNotNone(concur, secs)
        self.assertIsNotNone(concur.start_at,
                             "a mid-page opening should name its line")
        page, y = concur.start_at
        self.assertEqual(page, 2)
        # The attribution sits below "It is so ordered." and above the body.
        lines = so.group_lines(_MIDPAGE_DOC[2])
        att = next(ln for ln in lines if ln.text.startswith("Justice Sotomayor"))
        self.assertAlmostEqual(y, att.y, places=3)

    def test_a_part_opening_its_own_page_does_not(self):
        secs = self._sections(_FRESH_PAGE_DOC)
        concur = secs.get("concurrence")
        self.assertIsNotNone(concur, secs)
        self.assertIsNone(
            concur.start_at,
            "a part under a divider rule opens the page; the top is right")

    def test_the_syllabus_and_majority_never_carry_a_line(self):
        for doc in (_MIDPAGE_DOC, _FRESH_PAGE_DOC):
            for sec in so.detect_sections(doc):
                if sec.kind in ("syllabus", "majority"):
                    self.assertIsNone(sec.start_at, sec.label)

    def test_start_page_is_unchanged_by_the_new_field(self):
        # The field is additive: where a part starts is still start_page.
        secs = self._sections(_MIDPAGE_DOC)
        self.assertEqual(secs["syllabus"].start_page, 0)
        self.assertEqual(secs["majority"].start_page, 1)
        self.assertEqual(secs["concurrence"].start_page, 2)

    def test_sections_default_to_no_line(self):
        self.assertIsNone(so.SlipSection("X", "majority", 3).start_at)


# ---------------------------------------------------------------------------
# The reader's footnote markers, which have to survive justification
# ---------------------------------------------------------------------------

def _load_fn_marker_pos():
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    wanted = ("_fn_marker_pos", "_fn_link_map")
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            found[node.name] = ast.get_source_segment(src, node)
    missing = [w for w in wanted if w not in found]
    if missing:
        raise AssertionError(f"not found: {missing}")
    ns = {"re": re, "tk": type("tk", (), {"TclError": Exception}),
          "Optional": typing.Optional}
    for name in wanted:
        exec(found[name], ns)
    return ns


class _FakeText:
    """Just enough Text for the marker lookup: tags with movable ranges."""

    def __init__(self, ranges):
        self._ranges = ranges

    def tag_ranges(self, tag):
        return self._ranges.get(tag, ())


class FootnoteMarkerTests(unittest.TestCase):
    """The jump target is read from the marker's tag, not a stored index.

    Justification inserts padding into the text as the window is resized, which
    shifts every index recorded at render time.  A tag moves with its
    characters, so it still points at the marker.
    """

    @classmethod
    def setUpClass(cls):
        ns = _load_fn_marker_pos()
        cls.Reader = type("Reader", (), {
            "_fn_marker_pos": ns["_fn_marker_pos"],
            "_fn_link_map": ns["_fn_link_map"],
        })

    def _reader(self, actions, ranges, ref_pos=None, def_pos=None):
        r = self.Reader()
        r._link_actions = actions
        r._text = _FakeText(ranges)
        r._fn_ref_pos = ref_pos or {}
        r._fn_def_pos = def_pos or {}
        return r

    def test_position_comes_from_the_tag_not_the_stored_index(self):
        r = self._reader(
            actions={"lnk1": ("fnref", "f4"), "lnk2": ("fndef", "f4")},
            # Where the tags are *now*, after justification moved them.
            ranges={"lnk1": ("12.30", "12.31"), "lnk2": ("80.4", "80.5")},
            # Where they were when the text was first laid out.
            ref_pos={"f4": "12.5"}, def_pos={"f4": "77.0"},
        )
        self.assertEqual(r._fn_marker_pos("fnref", "f4"), "12.30")
        self.assertEqual(r._fn_marker_pos("fndef", "f4"), "80.4")

    def test_the_stored_index_is_the_fallback(self):
        # A marker never rendered as a link has no tag to read.
        r = self._reader(actions={}, ranges={},
                         ref_pos={"f4": "12.5"}, def_pos={"f4": "77.0"})
        self.assertEqual(r._fn_marker_pos("fnref", "f4"), "12.5")
        self.assertEqual(r._fn_marker_pos("fndef", "f4"), "77.0")

    def test_an_unknown_footnote_has_no_position(self):
        r = self._reader(actions={}, ranges={})
        self.assertIsNone(r._fn_marker_pos("fnref", "nope"))

    def test_a_tag_with_no_range_left_falls_back(self):
        r = self._reader(actions={"lnk1": ("fnref", "f4")}, ranges={"lnk1": ()},
                         ref_pos={"f4": "12.5"})
        self.assertEqual(r._fn_marker_pos("fnref", "f4"), "12.5")

    def test_the_first_reference_wins_when_a_note_is_cited_twice(self):
        r = self._reader(
            actions={"a": ("fnref", "f4"), "b": ("fnref", "f4")},
            ranges={"a": ("10.1", "10.2"), "b": ("40.1", "40.2")},
        )
        self.assertEqual(r._fn_marker_pos("fnref", "f4"), "10.1")

    def test_the_two_sides_do_not_cross(self):
        r = self._reader(
            actions={"a": ("fnref", "f1"), "b": ("fndef", "f2")},
            ranges={"a": ("10.1", "10.2"), "b": ("90.1", "90.2")},
        )
        self.assertEqual(r._fn_marker_pos("fnref", "f1"), "10.1")
        self.assertIsNone(r._fn_marker_pos("fndef", "f1"))
        self.assertIsNone(r._fn_marker_pos("fnref", "f2"))


if __name__ == "__main__":
    unittest.main()
