"""Headless checks for the reader-side location-map plumbing."""

import ast
import pathlib
import textwrap
import types
import typing
import unittest


def _load_methods(class_name, names):
    source = pathlib.Path(__file__).with_name(
        "courtlistener_gui.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    cls = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    found = {
        node.name: ast.get_source_segment(source, node)
        for node in cls.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    }
    missing = set(names) - set(found)
    if missing:
        raise AssertionError(f"missing methods: {sorted(missing)}")
    namespace = {
        "Optional": typing.Optional,
        "tk": types.SimpleNamespace(TclError=Exception),
    }
    for name in names:
        exec(textwrap.dedent(found[name]), namespace)
    return namespace


def _index_key(value):
    line, char = str(value).split(".", 1)
    return int(line), int(char)


class _FakeText:
    def __init__(self, marks, top="1.0"):
        self.marks = dict(marks)
        self.top = top

    def index(self, value):
        if value == "@0,0":
            return self.top
        return self.marks.get(str(value), str(value))

    def compare(self, left, operator, right):
        a = _index_key(self.index(left))
        b = _index_key(self.index(right))
        return {
            "<": a < b,
            "<=": a <= b,
            ">": a > b,
            ">=": a >= b,
        }[operator]


class PdfViewportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = _load_methods(
            "_PdfPane", ("_page_point_y", "viewport_anchor")
        )
        cls.Pane = type("Pane", (), {
            "_LINE_LEAD_IN": 28,
            "_page_point_y": ns["_page_point_y"],
            "viewport_anchor": ns["viewport_anchor"],
        })

    def test_forward_and_inverse_page_coordinates_round_trip(self):
        pane = self.Pane()
        pane._margin = 18
        pane._slots = [
            (12, 720, (0.0, 0.05, 1.0, 0.95), 1.2),
            (744, 720, (0.0, 0.05, 1.0, 0.95), 1.2),
        ]
        pane._meta = [
            (612.0, 792.0, pane._slots[0][2]),
            (612.0, 792.0, pane._slots[1][2]),
        ]
        point_y = pane._page_point_y(1, 360.0)
        pane._canvas = types.SimpleNamespace(
            canvasy=lambda _y: point_y - pane._LINE_LEAD_IN
        )

        page, y_pt = pane.viewport_anchor()

        self.assertEqual(page, 1)
        self.assertAlmostEqual(y_pt, 360.0, places=6)


class SwitchReadinessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = _load_methods("_ScholarTextWindow", ("_begin_pdf_switch",))
        cls.Reader = type("Reader", (), {
            "_begin_pdf_switch": ns["_begin_pdf_switch"],
        })

    def test_unready_map_preserves_the_old_top_of_pdf_behavior(self):
        reader = self.Reader()
        reader._mode = "scholar"
        reader._location_map_for = lambda *_args: None
        reader._live_location_anchors = []

        reader._begin_pdf_switch("case.pdf")

        self.assertTrue(reader._pending_pdf_switch)
        self.assertIsNone(reader._pending_pdf_target)

    def test_ready_map_captures_the_nearest_preceding_anchor(self):
        reader = self.Reader()
        reader._mode = "scholar"
        reader._location_map_for = lambda *_args: object()
        reader._text = _FakeText(
            {"first": "2.0", "current": "8.0", "later": "12.0"},
            top="9.0",
        )
        first = types.SimpleNamespace(pdf_page=1, y_pt=600.0)
        current = types.SimpleNamespace(pdf_page=3, y_pt=420.0)
        later = types.SimpleNamespace(pdf_page=4, y_pt=700.0)
        reader._live_location_anchors = [
            ("first", first), ("current", current), ("later", later)
        ]
        reader._tk_index_key = staticmethod(_index_key)

        reader._begin_pdf_switch("case.pdf")

        self.assertEqual(reader._pending_pdf_target, (3, 420.0))


class MappedPinTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = (
            "_tk_index_key", "_short_page_range",
            "_mapped_page_at_index", "_mapped_pin_for_range",
        )
        ns = _load_methods("_ScholarTextWindow", names)
        cls.Reader = type("Reader", (), {
            name: (
                staticmethod(ns[name])
                if name in ("_tk_index_key", "_short_page_range")
                else ns[name]
            )
            for name in names
        })

    def test_selection_crossing_inferred_pages_gets_us_page_range(self):
        reader = self.Reader()
        reader._text = _FakeText({
            "p83": "2.0", "p84": "5.0", "p85": "10.0",
        })
        reader._mapped_us_page_pos = {
            83: "p83", 84: "p84", 85: "p85",
        }
        reader._mapped_copy_cite = "590 U.S. 83"

        self.assertEqual(
            reader._mapped_pin_for_range("3.0", "11.0"), "83-85"
        )


if __name__ == "__main__":
    unittest.main()
