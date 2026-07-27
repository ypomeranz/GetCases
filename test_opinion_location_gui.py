"""Headless checks for the reader-side location-map plumbing."""

import ast
import pathlib
import re
import textwrap
import types
import typing
import unittest

import opinion_location


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
        "opinion_location": opinion_location,
        "_US_CITE_RE": re.compile(r"\b\d+\s+U\.\s*S\.\s+\d+\b", re.I),
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
        names = (
            "_location_address", "_location_map_navigation_ready",
            "_begin_pdf_switch", "_consume_pdf_switch_target",
        )
        ns = _load_methods("_ScholarTextWindow", names)
        cls.Reader = type("Reader", (), {
            name: (
                staticmethod(ns[name])
                if name in (
                    "_location_address", "_location_map_navigation_ready",
                )
                else ns[name]
            )
            for name in names
        })

    @staticmethod
    def _ready_map(page=3, y=420.0):
        target = types.SimpleNamespace(pdf_page=page, y_pt=y)
        return types.SimpleNamespace(
            boundaries=[types.SimpleNamespace(
                pdf_page=0, exact=True,
            )],
            anchors=[],
            confidence=1.0,
            pdf_location=lambda _address: target,
        )

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
        reader._location_map_for = lambda *_args: self._ready_map()
        reader._text = _FakeText(
            {"first": "2.0", "current": "8.0", "later": "12.0"},
            top="9.0",
        )
        first = types.SimpleNamespace(
            pdf_page=1, y_pt=600.0, address="first-address"
        )
        current = types.SimpleNamespace(
            pdf_page=99, y_pt=100.0, address="current-address"
        )
        later = types.SimpleNamespace(
            pdf_page=4, y_pt=700.0, address="later-address"
        )
        reader._live_location_anchors = [
            ("first", first), ("current", current), ("later", later)
        ]
        reader._tk_index_key = staticmethod(_index_key)

        reader._begin_pdf_switch("case.pdf")

        self.assertEqual(reader._pending_pdf_target, (3, 420.0))

    def test_map_finishing_after_click_does_not_retroactively_move_switch(self):
        reader = self.Reader()
        reader._mode = "scholar"
        reader._live_location_anchors = []
        reader._location_map_for = lambda *_args: None

        reader._begin_pdf_switch("case.pdf")
        reader._location_map_for = lambda *_args: self._ready_map()

        self.assertIsNone(reader._consume_pdf_switch_target("case.pdf"))

    def test_map_declared_unready_cannot_be_promoted_by_coarse_boundaries(self):
        location_map = self._ready_map()
        location_map.navigation_ready = False

        self.assertFalse(
            self.Reader._location_map_navigation_ready(location_map)
        )

    def test_selected_pdf_map_translates_the_live_text_anchor(self):
        reader = self.Reader()
        reader._mode = "scholar"
        selected = self._ready_map(page=7, y=333.0)
        reader._location_map_for = lambda *_args: selected
        reader._text = _FakeText({"live": "8.0"}, top="9.0")
        reader._live_location_anchors = [(
            "live",
            types.SimpleNamespace(
                pdf_page=91, y_pt=12.0, address="same-text-address"
            ),
        )]
        reader._tk_index_key = staticmethod(_index_key)

        reader._begin_pdf_switch("selected.pdf")

        self.assertEqual(reader._pending_pdf_target, (7, 333.0))


class MappedReporterPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = ("_mapped_pages_are_us", "_mapped_copy_is_us")
        ns = _load_methods("_ScholarTextWindow", names)
        cls.Reader = type("Reader", (), {
            name: ns[name] for name in names
        })

    @staticmethod
    def _map():
        return types.SimpleNamespace(
            copy_cite="590 U.S. 83", copy_ready=True
        )

    @staticmethod
    def _reader(override=""):
        reader = MappedReporterPolicyTests.Reader()
        reader._base_citation_override = override
        reader._bb = {"cite": "140 S. Ct. 1390"}
        reader._page_pos = {1390: "native"}
        return reader

    def test_manual_sct_override_does_not_hide_teal_us_pages(self):
        reader = self._reader(
            "Ramos v. Louisiana, 140 S. Ct. 1390 (2020)"
        )

        self.assertTrue(reader._mapped_pages_are_us(self._map()))
        self.assertFalse(reader._mapped_copy_is_us(self._map()))

    def test_manual_us_override_uses_the_mapped_us_pin(self):
        reader = self._reader(
            "Ramos v. Louisiana, 590 U.S. 83 (2020)"
        )

        self.assertTrue(reader._mapped_copy_is_us(self._map()))

    def test_manual_us_override_for_another_case_rejects_mapped_pin(self):
        reader = self._reader(
            "Different Case, 590 U.S. 100 (2020)"
        )

        self.assertFalse(reader._mapped_copy_is_us(self._map()))


class RestoreTargetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = _load_methods(
            "_ScholarTextWindow", ("_restore_pending_text_location",)
        )
        cls.Reader = type("Reader", (), {
            "_restore_pending_text_location":
                ns["_restore_pending_text_location"],
        })

    def test_return_mark_is_not_cleared_with_async_map_anchors(self):
        callbacks = []
        calls = []

        class Text:
            def mark_set(self, name, index):
                calls.append(("set", name, index))

            def mark_gravity(self, name, gravity):
                calls.append(("gravity", name, gravity))

            def see(self, name):
                calls.append(("see", name))

            def yview(self, name):
                calls.append(("yview", name))

            def mark_unset(self, name):
                calls.append(("unset", name))

        reader = self.Reader()
        address = types.SimpleNamespace(part_index=1)
        reader._pending_text_target = address
        reader._location_render_seq = 3
        reader._location_map_mark_names = ["ordinary-map-anchor"]
        reader._location_index_for_address = lambda _address: "8.4"
        reader._text = Text()
        reader._draw_page_column = lambda: None
        reader._win = types.SimpleNamespace(
            after_idle=lambda callback: callbacks.append(callback),
            after=lambda _delay, callback: callbacks.append(callback),
        )

        reader._restore_pending_text_location()

        return_mark = calls[0][1]
        self.assertNotIn(return_mark, reader._location_map_mark_names)
        callbacks.pop(0)()
        self.assertIn(("see", return_mark), calls)
        self.assertIn(("unset", return_mark), calls)


class ReverseSwitchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = (
            "_location_address", "_location_map_navigation_ready",
            "_back_from_pdf",
        )
        ns = _load_methods("_ScholarTextWindow", names)
        cls.Reader = type("Reader", (), {
            name: (
                staticmethod(ns[name])
                if name in (
                    "_location_address", "_location_map_navigation_ready",
                )
                else ns[name]
            )
            for name in names
        })

    def test_ready_pdf_viewport_returns_to_matching_text_and_reveals_part(self):
        reader = self.Reader()
        address = types.SimpleNamespace(part_index=2)
        location_map = types.SimpleNamespace(
            boundaries=[
                types.SimpleNamespace(pdf_page=0, exact=True),
                types.SimpleNamespace(pdf_page=1, exact=True),
            ],
            anchors=[],
            confidence=1.0,
            text_location=lambda _page, _y: types.SimpleNamespace(
                address=address
            ),
        )
        reader._pre_pdf_mode = "scholar"
        reader._location_map_for = lambda *_args: location_map
        reader._pdf_pane = types.SimpleNamespace(
            viewport_anchor=lambda: (1, 420.0),
            destroy=lambda: None,
        )
        reader._pdf_holder = None
        reader._current_part = 0
        reader._part_combo = types.SimpleNamespace(current=lambda _value: None)
        reader._pdf_parts_nav = object()
        reader._text_frame = types.SimpleNamespace(pack=lambda **_kwargs: None)
        reader._btn_frame = object()
        reader._render_scholar = lambda: None

        reader._back_from_pdf()

        self.assertIs(reader._pending_text_target, address)
        self.assertIsNone(reader._current_part)
        self.assertIsNone(reader._pdf_pane)

    def test_low_confidence_map_preserves_old_top_of_text_fallback(self):
        reader = self.Reader()
        location_map = types.SimpleNamespace(
            boundaries=[
                types.SimpleNamespace(pdf_page=i, exact=False)
                for i in range(5)
            ],
            anchors=[
                types.SimpleNamespace(pdf_page=2, pdf_char=3)
            ],
            confidence=0.1,
        )
        reader._pre_pdf_mode = "scholar"
        reader._location_map_for = lambda *_args: location_map
        reader._pdf_pane = types.SimpleNamespace(
            viewport_anchor=lambda: (2, 420.0),
            destroy=lambda: None,
        )
        reader._pdf_holder = None
        reader._current_part = None
        reader._pdf_parts_nav = object()
        reader._text_frame = types.SimpleNamespace(pack=lambda **_kwargs: None)
        reader._btn_frame = object()
        reader._render_scholar = lambda: None

        reader._back_from_pdf()

        self.assertIsNone(reader._pending_text_target)


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

    def test_coincident_boundaries_use_the_later_page_after_the_mark(self):
        reader = self.Reader()
        reader._text = _FakeText({
            "p10": "5.0", "p11": "5.0",
        })
        reader._mapped_us_page_pos = {
            10: "p10", 11: "p11",
        }

        self.assertEqual(reader._mapped_page_at_index("6.0"), 11)


class PdfFirstSwitchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = _load_methods(
            "_PdfWindow",
            ("_discovered_text_target", "_open_discovered_text"),
        )
        cls.created = []

        def make_text_window(*args, **kwargs):
            cls.created.append((args, kwargs))

        ns["_ScholarTextWindow"] = make_text_window
        cls.Reader = type("Reader", (), {
            "_discovered_text_target": ns["_discovered_text_target"],
            "_open_discovered_text": ns["_open_discovered_text"],
        })

    def setUp(self):
        self.created.clear()

    def test_ready_pdf_first_map_hands_viewport_address_to_text_window(self):
        address = opinion_location.TextAddress(0, False, 123, 45)
        reader = self.Reader()
        reader._text_source = types.SimpleNamespace(
            item={"caseName": "Example"},
            text="Example opinion text",
            parts=[],
            blocks=[],
            source_label="static.case.law",
            source_url="https://static.case.law/example.json",
            kind="case_law",
        )
        reader._app = types.SimpleNamespace(root=object())
        reader._bytes = b"%PDF"
        reader._url = (
            "https://static.case.law/f2d/754/case-pdfs/0000898-01.pdf"
        )
        reader._pane = types.SimpleNamespace(
            viewport_anchor=lambda: (2, 410.0)
        )
        reader._location_map = types.SimpleNamespace(
            navigation_ready=True,
            text_location=lambda page, y: types.SimpleNamespace(
                address=address if (page, y) == (2, 410.0) else None
            ),
        )
        reader._win = types.SimpleNamespace(destroy=lambda: None)
        reader._status_var = types.SimpleNamespace(set=lambda _value: None)
        analysis = {"maps": {"courtlistener": reader._location_map}}
        reader._initial_pdf_analysis = lambda: analysis

        reader._open_discovered_text()

        self.assertEqual(len(self.created), 1)
        self.assertIs(
            self.created[0][1]["initial_text_target"], address
        )
        self.assertIs(
            self.created[0][1]["initial_pdf_analysis"], analysis
        )


if __name__ == "__main__":
    unittest.main()
