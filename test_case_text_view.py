"""Three changes to the case-text view.

1. The **side panel** opens beside the opinion instead of out of it: the window
   grows by the panel's width so the text keeps its measure and is not
   re-wrapped, and shrinks back when the panel closes.  A maximized window has
   nowhere to grow, so there the panel takes its width from the text as before.

2. The **part-picker row** at the top is gone.  The strip down the right names
   every separate writing and jumps to it, and the tinted background says which
   one you are reading; a selector and a "Viewing" label only said it again.

3. The **part map** no longer loses a short final dissent or concurrence.  A
   label hangs below its marker, so a part beginning at the very end of the
   opinion had its name drawn past the bottom edge of the map.

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


def _load(cls: str, names, extra=None) -> dict:
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(SRC, n) for n in body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found on {cls}: {missing}")
    ns = {"tk": _Tk, "re": re, "Optional": typing.Optional}
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
# 2. The part-picker row is gone
# ---------------------------------------------------------------------------


class PartPickerRemovedTests(unittest.TestCase):
    """The widgets and the code that only existed to feed them."""

    def test_the_reader_no_longer_builds_a_part_selector(self):
        self.assertNotIn("_part_combo", SRC)

    def test_nor_the_viewing_label_beside_it(self):
        self.assertNotIn("_view_label", SRC)
        self.assertNotIn("_set_view_color", SRC)

    def test_the_row_that_held_them_is_gone_too(self):
        self.assertNotIn("_view_frame", SRC)

    def test_and_the_scroll_handler_that_only_relabelled_it(self):
        # _update_scroll_part existed to rename the label as the reader
        # scrolled between parts; the tinted background does that now.
        self.assertNotIn("_update_scroll_part", SRC)
        self.assertNotIn("_scroll_part", SRC)

    def test_the_source_bar_still_has_somewhere_to_anchor(self):
        # It used to pack itself above the row that has just been deleted.
        body = _source_of("_ScholarTextWindow", "_apply_source_bar_visibility")
        self.assertIn("before=self._text_frame", body)

    def test_the_parts_are_still_marked_in_the_text_and_on_the_map(self):
        # What replaces the row: the tint tags and the right-hand strip.
        self.assertIn("box-dissent", SRC)
        self.assertIn("box-concurrence", SRC)
        self.assertIn("_draw_part_map", SRC)


# ---------------------------------------------------------------------------
# 1. The side panel takes its own width
# ---------------------------------------------------------------------------

PANEL_W = 300

DETAILS_NS = _load(
    "_ScholarTextWindow",
    ["_toggle_details", "_resize_for_details", "_window_is_maximized"],
    {"_CaseTabPage": type("_CaseTabPage", (), {}),
     "_work_area": lambda _w: (0, 0, 1600, 900)},
)


class _FakeWindow:
    """A Toplevel's geometry surface, as the resize sees it."""

    def __init__(self, width=860, height=620, x=40, y=30):
        self.width, self.height, self.x, self.y = width, height, x, y
        self.zoomed = False
        self.applied = []

    # --- what _resize_for_details reads ---
    def wm_geometry(self):
        return f"{self.width}x{self.height}+{self.x}+{self.y}"

    def winfo_width(self):
        return self.width

    def state(self):
        return "zoomed" if self.zoomed else "normal"

    def attributes(self, name):
        if name == "-zoomed":
            return 1 if self.zoomed else 0
        raise _Tk.TclError(name)

    # --- and what it writes ---
    def geometry(self, spec):
        self.applied.append(spec)
        m = re.match(r"^(\d+)x(\d+)([+-]\d+)([+-]\d+)$", spec)
        if m:
            self.width, self.height = int(m.group(1)), int(m.group(2))
            self.x, self.y = int(m.group(3)), int(m.group(4))


class _Panel:
    def __init__(self):
        self.packed = None
        self.forgotten = 0

    def pack(self, **kw):
        self.packed = kw

    def pack_forget(self):
        self.forgotten += 1


class _Reader:
    def __init__(self, mode="scholar", win=None):
        self._win = win or _FakeWindow()
        self._mode = mode
        self._details_panel_w = PANEL_W
        self._details_on = False
        self._pdf_parts_on = False
        self._details_var = mock.Mock()
        self._details_var.get.return_value = True
        self._details_frame = _Panel()
        self._vsb = object()
        self.order = []
        self.refreshed = 0
        for name in ("_toggle_details", "_resize_for_details",
                     "_window_is_maximized"):
            setattr(self, name, DETAILS_NS[name].__get__(self))

    def _details_panel(self):
        self.order.append("pack")
        return self._details_frame

    def _refresh_details_view(self):
        self.refreshed += 1

    def _apply_pdf_parts_visibility(self):
        self.order.append("pdf-strip")


class SidePanelWidthTests(unittest.TestCase):
    def test_opening_the_panel_widens_the_window_by_its_width(self):
        reader = _Reader()
        reader._toggle_details()
        self.assertEqual(reader._win.width, 860 + PANEL_W)

    def test_closing_it_gives_the_width_back(self):
        reader = _Reader()
        reader._toggle_details()
        reader._details_var.get.return_value = False
        reader._toggle_details()
        self.assertEqual(reader._win.width, 860)

    def test_the_window_does_not_drift_across_the_desktop(self):
        # Rebuilding a geometry string from winfo_x/y walks the window down and
        # right by the title bar under some window managers; the manager's own
        # string round-trips.
        reader = _Reader()
        reader._toggle_details()
        self.assertEqual(reader._win.x, 40)
        self.assertEqual(reader._win.y, 30)
        self.assertEqual(reader._win.height, 620)

    def test_it_is_allowed_to_run_off_the_right_of_the_screen(self):
        # 1400 + 300 is past the 1600 desktop; the reader can move the window,
        # which beats re-flowing the opinion under them.
        reader = _Reader(win=_FakeWindow(width=1400, x=100))
        reader._toggle_details()
        self.assertEqual(reader._win.width, 1700)

    def test_a_maximized_window_has_nowhere_to_grow(self):
        win = _FakeWindow()
        win.zoomed = True
        reader = _Reader(win=win)
        reader._toggle_details()
        self.assertEqual(win.applied, [])
        self.assertIsNotNone(reader._details_frame.packed)  # panel still opens

    def test_a_window_already_as_wide_as_the_desktop_counts_as_maximized(self):
        reader = _Reader(win=_FakeWindow(width=1595))
        reader._toggle_details()
        self.assertEqual(reader._win.applied, [])

    def test_a_tab_in_the_shared_window_never_resizes_it(self):
        page = DETAILS_NS["_CaseTabPage"]()
        page.applied = []
        page.geometry = lambda spec: page.applied.append(spec)
        reader = _Reader(win=page)
        reader._toggle_details()
        self.assertEqual(page.applied, [])

    def test_the_window_never_shrinks_below_its_minimum(self):
        reader = _Reader(win=_FakeWindow(width=500))
        reader._details_var.get.return_value = False
        reader._details_on = True
        reader._toggle_details()
        self.assertEqual(reader._win.width, 430)

    def test_the_resize_lands_before_the_panel_is_packed(self):
        # Both in one turn and with no layout flush between them, so Tk lays
        # the window out once and no intermediate width is ever drawn.
        reader = _Reader()
        reader._toggle_details()
        self.assertEqual(reader._win.applied, [f"{860 + PANEL_W}x620+40+30"])
        self.assertEqual(reader.order, ["pack"])
        self.assertEqual(reader.refreshed, 1)

    def test_the_pdf_view_s_own_panel_is_untouched_by_any_of_this(self):
        reader = _Reader(mode="pdf")
        reader._toggle_details()
        self.assertEqual(reader._win.applied, [])
        self.assertTrue(reader._pdf_parts_on)
        self.assertEqual(reader.order, ["pdf-strip"])


# ---------------------------------------------------------------------------
# 3. A short final dissent stays on the part map
# ---------------------------------------------------------------------------

MAP_W = 104
PART_COLORS = {"majority": "#1a3e72", "concurrence": "#1a7a3c",
               "dissent": "#a31515", "separate": "#59636f"}


class _FakeFont:
    def __init__(self, linespace=15):
        self._linespace = linespace

    def metrics(self, _what):
        return self._linespace


class _MapCanvas:
    """Enough canvas to place items and measure what was placed."""

    def __init__(self, height=600, label_lines=1, linespace=15):
        self.height = height
        self.label_lines = label_lines
        self.linespace = linespace
        self.width_set = None
        self.items = {}      # id -> (kind, coords, text)
        self._next = 0

    def delete(self, _what):
        self.items = {}

    def config(self, **kw):
        if "width" in kw:
            self.width_set = kw["width"]

    def winfo_height(self):
        return self.height

    def _add(self, kind, coords, text=None):
        self._next += 1
        self.items[self._next] = [kind, list(coords), text]
        return self._next

    def create_line(self, *coords, **_kw):
        return self._add("line", coords)

    def create_rectangle(self, *coords, **_kw):
        return self._add("rect", coords)

    def create_text(self, x, y, **kw):
        return self._add("text", (x, y), kw.get("text", ""))

    def bbox(self, tid):
        kind, coords, text = self.items[tid]
        if kind != "text":
            return tuple(coords)
        lines = self.label_lines
        return (coords[0], coords[1], coords[0] + 80,
                coords[1] + lines * self.linespace)

    def move(self, item_id, dx, dy):
        item = self.items[item_id]
        item[1] = [c + (dx if i % 2 == 0 else dy)
                   for i, c in enumerate(item[1])]

    def find_all(self):
        return list(self.items)

    def lowest_pixel(self):
        return max(self.bbox(i)[3] for i in self.items)

    def labels(self):
        return [text for kind, _c, text in self.items.values()
                if kind == "text"]


MAP_NS = _load(
    "_ScholarTextWindow",
    ["_draw_part_map", "_partmap_short_label"],
    {"_PARTMAP_COLORS": PART_COLORS},
)


class _FakeText:
    """The map reads the text widget only for a fallback height."""

    def winfo_height(self):
        return 600


class _Part:
    def __init__(self, kind, label):
        self.kind, self.label = kind, label


class _MapReader:
    _PARTMAP_W = MAP_W
    _PARTMAP_COLORS = PART_COLORS

    def __init__(self, parts, starts, total=10000, canvas=None):
        """*starts* are each part's offset in the document, 0..total."""
        self._partmap = canvas or _MapCanvas()
        self._partmap_font = _FakeFont(self._partmap.linespace)
        self._partmap_rows = []
        self._mode = "scholar"
        self._current_part = None
        self._rendered_parts = parts
        self._text = _FakeText()
        self._total = total
        self._starts = {f"start{i}": s for i, s in enumerate(starts)}
        self._regions = [(f"start{i}", f"end{i}", i)
                         for i in range(len(parts))]
        self._draw_part_map = MAP_NS["_draw_part_map"].__get__(self)
        self._partmap_short_label = MAP_NS["_partmap_short_label"]

    def _part_region_indices(self):
        return self._regions

    def _ypixels(self, index):
        return self._total if index == "end-1c" else self._starts[index]


def _reader_with_short_tail(**kw):
    """An opinion whose dissent is two lines at the very end of a long text."""
    parts = [_Part("majority", "Opinion of the Court (Blackmun)"),
             _Part("concurrence", "MR. JUSTICE STEWART, concurring"),
             _Part("dissent", "MR. JUSTICE REHNQUIST, dissenting")]
    return _MapReader(parts, [0, 6000, 9960], **kw)


class PartMapTailTests(unittest.TestCase):
    def test_a_dissent_at_the_very_end_is_drawn_inside_the_map(self):
        reader = _reader_with_short_tail()
        reader._draw_part_map()
        canvas = reader._partmap
        self.assertLessEqual(canvas.lowest_pixel(), canvas.height)

    def test_its_name_is_drawn_and_not_just_its_marker(self):
        reader = _reader_with_short_tail()
        reader._draw_part_map()
        self.assertIn("Rehnquist", reader._partmap.labels())

    def test_the_label_below_the_last_marker_has_room_for_itself(self):
        reader = _reader_with_short_tail()
        reader._draw_part_map()
        canvas = reader._partmap
        last_y = reader._partmap_rows[-1][0]
        gap = canvas.linespace + 3
        self.assertLessEqual(last_y, canvas.height - gap)

    def test_a_part_map_row_matches_what_was_drawn(self):
        reader = _reader_with_short_tail()
        reader._draw_part_map()
        rows = reader._partmap_rows
        self.assertEqual(len(rows), 3)
        for y1, y2, _rs in rows:
            self.assertLess(y1, y2)
            self.assertLessEqual(y2, reader._partmap.height)

    def test_only_the_marker_that_overflows_is_lifted(self):
        # Markers with room between them must keep their places: a wrapped
        # label at the end lifts that marker, not the whole strip.
        canvas = _MapCanvas(label_lines=2)
        parts = [_Part("majority", "Opinion of the Court"),
                 _Part("dissent", "JUSTICE ALITO, dissenting")]
        reader = _MapReader(parts, [0, 9990], canvas=canvas)
        reader._draw_part_map()
        first, last = reader._partmap_rows
        self.assertEqual(first[0], 6)                     # untouched at the top
        self.assertLessEqual(last[1], canvas.height)      # lifted into view
        self.assertGreater(last[0], canvas.height / 2)    # still near the end

    def test_a_name_long_enough_to_wrap_still_fits(self):
        # Two lines of label are taller than the single line reserved for it,
        # so the strip is lifted by the overflow rather than being cut off.
        canvas = _MapCanvas(label_lines=2)
        reader = _reader_with_short_tail(canvas=canvas)
        reader._draw_part_map()
        self.assertLessEqual(canvas.lowest_pixel(), canvas.height)
        self.assertIn("Rehnquist", canvas.labels())

    def test_three_writings_ending_together_all_stay_readable(self):
        parts = [_Part("majority", "Opinion of the Court (Roberts)"),
                 _Part("concurrence", "JUSTICE KAGAN, concurring"),
                 _Part("dissent", "JUSTICE ALITO, dissenting"),
                 _Part("dissent", "JUSTICE THOMAS, dissenting")]
        reader = _MapReader(parts, [0, 9800, 9900, 9980])
        reader._draw_part_map()
        canvas = reader._partmap
        self.assertLessEqual(canvas.lowest_pixel(), canvas.height)
        self.assertEqual(canvas.labels(),
                         ["Opinion (Roberts)", "Kagan", "Alito", "Thomas"])
        gap = canvas.linespace + 3
        ys = [row[0] for row in reader._partmap_rows]
        for a, b in zip(ys, ys[1:]):
            self.assertGreaterEqual(b - a, gap - 1)

    def test_a_short_map_still_draws_every_part(self):
        canvas = _MapCanvas(height=120)
        parts = [_Part("majority", "Opinion of the Court"),
                 _Part("dissent", "JUSTICE ALITO, dissenting")]
        reader = _MapReader(parts, [0, 9990], canvas=canvas)
        reader._draw_part_map()
        self.assertEqual(len(reader._partmap_rows), 2)
        self.assertEqual(canvas.labels(), ["Opinion", "Alito"])

    def test_parts_spread_through_the_opinion_keep_their_places(self):
        # The fix must not disturb a map with room to spare: each marker still
        # sits where its part begins.
        parts = [_Part("majority", "Opinion of the Court"),
                 _Part("dissent", "JUSTICE ALITO, dissenting")]
        reader = _MapReader(parts, [0, 5000])
        reader._draw_part_map()
        canvas = reader._partmap
        y_dissent = reader._partmap_rows[1][0]
        self.assertAlmostEqual(y_dissent, canvas.height * 0.5, delta=8)

    def test_the_map_is_hidden_when_there_is_nothing_to_map(self):
        reader = _MapReader([_Part("majority", "Opinion of the Court")], [0])
        reader._regions = []
        reader._draw_part_map()
        self.assertEqual(reader._partmap.width_set, 0)
        self.assertEqual(reader._partmap_rows, [])

    def test_the_map_stays_out_of_the_pdf_view(self):
        reader = _reader_with_short_tail()
        reader._mode = "pdf"
        reader._draw_part_map()
        self.assertEqual(reader._partmap.width_set, 0)


class PartMapLabelTests(unittest.TestCase):
    """The names on the strip — unchanged, but now the only ones on screen."""

    def setUp(self):
        self.short = MAP_NS["_partmap_short_label"]

    def test_the_court_s_opinion_names_its_author(self):
        self.assertEqual(
            self.short("MR. JUSTICE BLACKMUN delivered the opinion of the "
                       "Court", "majority"),
            "Opinion (Blackmun)")

    def test_a_separate_writing_is_just_the_surname(self):
        self.assertEqual(
            self.short("JUSTICE SOTOMAYOR, dissenting", "dissent"),
            "Sotomayor")

    def test_a_per_curiam_says_so(self):
        self.assertEqual(self.short("PER CURIAM", "majority"),
                         "Opinion (per curiam)")

    def test_an_unattributed_writing_falls_back_to_its_kind(self):
        self.assertEqual(self.short("", "concurrence"), "Concurrence")


if __name__ == "__main__":
    unittest.main()
