"""Footnote regions survive the LaTeX export's section split.

A note's extent is bracketed by two Tk *marks* (``__fnreg_start_N`` /
``__fnreg_end_N``), not by "line.char" indices.  The export's
``_section_note_map`` fed those mark names straight into ``_tk_ix``, which
parses "line.char" — so exporting any opinion that has a footnote crashed with
``ValueError: invalid literal for int() with base 10: '__fnreg_start_0'``.

``_section_note_map`` and ``_section_body_ranges`` are lifted out of
``courtlistener_gui`` with ``ast`` (it imports tkinter, absent on a headless
run) and driven against a fake Text whose ``index`` resolves marks the way Tk
does.
"""

import ast
import pathlib
import typing
import unittest


def _load(fns, methods_of=None):
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    found = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in fns:
            found[node.name] = ast.get_source_segment(src, node)
    if methods_of:
        cls, names = methods_of
        body = next(n.body for n in tree.body
                    if isinstance(n, ast.ClassDef) and n.name == cls)
        for node in body:
            if isinstance(node, ast.FunctionDef) and node.name in names:
                found[node.name] = ast.get_source_segment(src, node)
    ns = {"tk": type("tk", (), {"TclError": Exception, "Text": object}),
          "Optional": typing.Optional, "_ExpPara": object}
    for name, seg in found.items():
        exec(seg, ns)
    return ns


class _FakeText:
    """Resolves marks like Tk: __fnreg_start_0 → its stored "line.char"."""

    def __init__(self, marks):
        self._marks = dict(marks)

    def index(self, spec):
        spec = str(spec)
        if spec in self._marks:
            return self._marks[spec]
        # A literal "line.char" (or "end-1c" in these tests) is returned as-is.
        int(spec.split(".")[0])  # raises for an unknown mark name, as Tk would
        return spec


class SectionNoteMapTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = _load(
            ["_tk_ix", "_section_body_ranges", "_dump_export_paragraphs",
             "_strip_note_marker"],
            methods_of=("_ScholarTextWindow", ["_section_note_map"]),
        )
        # Stub the paragraph machinery — this test is about the index/mark
        # handling, not the note-body parsing.
        ns["_dump_export_paragraphs"] = lambda txt, ranges, links: [
            ("note", ranges)]
        ns["_strip_note_marker"] = lambda paras: ("fid1", "1")
        cls.ns = ns

    def _reader(self, marks, regions):
        r = type("R", (), {})()
        r._text = _FakeText(marks)
        r._fn_regions = regions
        r._section_note_map = self.ns["_section_note_map"].__get__(r)
        return r

    def test_a_mark_bracketed_note_does_not_crash_the_export(self):
        # The exact shape that raised: endpoints are mark names.
        marks = {"__fnreg_start_0": "40.0", "__fnreg_end_0": "44.0"}
        r = self._reader(marks, [("__fnreg_start_0", "__fnreg_end_0", "1", 5)])
        regions, parsed = r._section_note_map("1.0", "100.0", {})
        self.assertEqual(len(regions), 1)
        self.assertEqual(len(parsed), 1)

    def test_the_returned_regions_carry_real_indices(self):
        # So the downstream _section_body_ranges, which _tk_ix's them, is safe.
        marks = {"__fnreg_start_0": "40.0", "__fnreg_end_0": "44.0"}
        r = self._reader(marks, [("__fnreg_start_0", "__fnreg_end_0", "1", 5)])
        regions, _parsed = r._section_note_map("1.0", "100.0", {})
        (rs, rend, num, page), = regions
        self.assertEqual((rs, rend, num, page), ("40.0", "44.0", "1", 5))
        # _section_body_ranges must now consume them without raising.
        ranges = self.ns["_section_body_ranges"]("1.0", "100.0", regions)
        self.assertEqual(ranges, [("1.0", "40.0"), ("44.0", "100.0")])

    def test_notes_outside_the_section_are_dropped(self):
        marks = {"__fnreg_start_0": "10.0", "__fnreg_end_0": "12.0",
                 "__fnreg_start_1": "80.0", "__fnreg_end_1": "82.0"}
        regions_in = [("__fnreg_start_0", "__fnreg_end_0", "1", 1),
                      ("__fnreg_start_1", "__fnreg_end_1", "2", 2)]
        r = self._reader(marks, regions_in)
        regions, _ = r._section_note_map("50.0", "100.0", {})
        self.assertEqual([num for _rs, _re, num, _pg in regions], ["2"])

    def test_notes_are_ordered_by_position(self):
        marks = {"__fnreg_start_0": "80.0", "__fnreg_end_0": "82.0",
                 "__fnreg_start_1": "40.0", "__fnreg_end_1": "42.0"}
        regions_in = [("__fnreg_start_0", "__fnreg_end_0", "2", 2),
                      ("__fnreg_start_1", "__fnreg_end_1", "1", 1)]
        r = self._reader(marks, regions_in)
        regions, _ = r._section_note_map("1.0", "100.0", {})
        self.assertEqual([num for _rs, _re, num, _pg in regions], ["1", "2"])

    def test_no_notes_is_handled(self):
        r = self._reader({}, [])
        regions, parsed = r._section_note_map("1.0", "100.0", {})
        self.assertEqual((regions, parsed), ([], []))


if __name__ == "__main__":
    unittest.main()
