"""Reporter pagination that outlives the scan, and footnote pin cites.

Two reader behaviours meet here.  A recent Supreme Court opinion reaches Google
Scholar with the Supreme Court Reporter's star pagination — or none — so its
U.S. Reports pages are recovered by aligning the text against the Court's own
PDF; those pages are now saved with the opinion so a later pin cite lands
without waiting for the download.  And a pin cite may name a footnote rather
than a page ("200 U.S. 12, 13 n.4"), which has to open the note.

The reader itself imports tkinter, so its methods are lifted out with ``ast``
and exercised against fakes, as in ``test_opinion_location_gui``.
"""

import ast
import json
import pathlib
import re
import tempfile
import textwrap
import threading
import types
import typing
import unittest
from pathlib import Path

import citations
import opinion_location
from opinion_db import OpinionDB, extract_record


def _load_methods(class_name, names, **extra):
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
        "re": re,
        "threading": threading,
        "tk": types.SimpleNamespace(TclError=Exception),
        "opinion_location": opinion_location,
        "_split_note_pin": citations.split_note_pin,
        "_pin_display": citations.pin_display,
        **extra,
    }
    for name in names:
        exec(textwrap.dedent(found[name]), namespace)
    return namespace


def _index_key(value):
    line, char = str(value).split(".", 1)
    return int(line), int(char)


class _FakeText:
    """Just enough Tk text widget for mark comparison and flashing."""

    def __init__(self, marks):
        self.marks = dict(marks)
        self.flashed = None
        self.seen = None

    def index(self, value):
        return self.marks.get(str(value), str(value))

    def compare(self, left, operator, right):
        a = _index_key(self.index(left))
        b = _index_key(self.index(right))
        return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[operator]

    def tag_nextrange(self, _tag, _start):
        return ()


def _span(text, **kwargs):
    return types.SimpleNamespace(text=text, pagenum=False, **kwargs)


def _block(*spans):
    return types.SimpleNamespace(spans=list(spans))


def _part(blocks, footnotes=()):
    return types.SimpleNamespace(
        kind="majority", blocks=list(blocks), footnotes=list(footnotes),
    )


# ---------------------------------------------------------------------------
# Storing the pages with the opinion
# ---------------------------------------------------------------------------

def _opinion_html(body: str) -> str:
    return (
        '<div id="gs_opinion">'
        "<center>146 S. Ct. 1131</center>"
        "<center>SMITH v. JONES</center>"
        "<center>Supreme Court of the United States.</center>"
        "<center>Decided April 29, 2026.</center>"
        f"<p>{body}</p></div>"
    )


_URL = "https://scholar.google.com/scholar_case?case=12345678901234567890"


class DatabasePaginationTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        root = Path(self._dir.name)
        self.db = OpinionDB(root / "opinions.jsonl", root / "index.db")
        self.addCleanup(self.db.close)
        self.db.add(extract_record(
            _URL, _opinion_html("The judgment is affirmed. " * 30)
        ))
        self.sid = "12345678901234567890"

    def test_pagination_is_saved_and_read_back(self):
        pages = {"v": 1, "cite": "608 U. S. 1", "fingerprint": "abc",
                 "pdf_pages": 3, "confidence": 0.9,
                 "pages": [[1, 0, 0, 0, 0]]}

        self.assertTrue(self.db.save_pagination(self.sid, pages))
        self.assertEqual(self.db.stored_pagination(self.sid), pages)

    def test_saving_the_same_pagination_again_does_not_rewrite_the_store(self):
        # The JSONL is the Git-synced source of truth; an identical value must
        # not churn it on every open.
        pages = {"v": 1, "cite": "608 U. S. 1", "fingerprint": "abc",
                 "pdf_pages": 3, "confidence": 0.9,
                 "pages": [[1, 0, 0, 0, 0]]}
        self.db.save_pagination(self.sid, pages)
        before = self.db.jsonl_path.read_bytes()

        self.assertFalse(self.db.save_pagination(self.sid, dict(pages)))
        self.assertEqual(self.db.jsonl_path.read_bytes(), before)

    def test_pagination_does_not_disturb_the_stored_opinion(self):
        self.db.save_pagination(
            self.sid,
            {"v": 1, "cite": "608 U. S. 1", "fingerprint": "abc",
             "pdf_pages": 1, "confidence": 1.0, "pages": [[1, 0, 0, 0, 0]]},
        )
        record = self.db.get_by_scholar_id(self.sid)

        self.assertIn("judgment is affirmed", record["html"])
        self.assertEqual(len(self.db.find("146 S. Ct. 1131")), 1)

    def test_an_unstored_opinion_is_not_invented(self):
        self.assertFalse(self.db.save_pagination("404", {"v": 1}))
        self.assertIsNone(self.db.stored_pagination("404"))

    def test_pagination_can_be_cleared(self):
        pages = {"v": 1, "cite": "608 U. S. 1", "fingerprint": "abc",
                 "pdf_pages": 1, "confidence": 1.0, "pages": [[1, 0, 0, 0, 0]]}
        self.db.save_pagination(self.sid, pages)

        self.assertTrue(self.db.save_pagination(self.sid, None))
        self.assertIsNone(self.db.stored_pagination(self.sid))

    def test_every_stored_line_stays_valid_json(self):
        self.db.save_pagination(
            self.sid,
            {"v": 1, "cite": "608 U. S. 1", "fingerprint": "abc",
             "pdf_pages": 1, "confidence": 1.0, "pages": [[1, 0, 0, 0, 0]]},
        )
        for line in self.db.jsonl_path.read_text(encoding="utf-8").splitlines():
            json.loads(line)


# ---------------------------------------------------------------------------
# The reader's use of it
# ---------------------------------------------------------------------------

class StoredPaginationReaderTests(unittest.TestCase):
    """The saved pages stand in for a location map until the scan arrives."""

    @classmethod
    def setUpClass(cls):
        names = ("_location_map_for", "_apply_stored_pagination",
                 "_store_scholar_pagination", "_opinion_scholar_id")
        ns = _load_methods("_ScholarTextWindow", names)
        cls.Reader = type("Reader", (), {name: ns[name] for name in names})

    def _reader(self):
        reader = self.Reader()
        reader._mode = "scholar"
        reader._cl_primary = False
        reader._pdf_analysis_cache = {}
        reader._pdf_url_keys = {}
        reader._active_pdf_analysis_key = None
        reader._stored_location_maps = {}
        reader._saved_pagination_json = None
        reader._pending_pin_jump = None
        reader._app = None
        reader._parts = []
        reader._scholar_url = _URL
        reader._install_current_location_map = lambda: None
        reader._retry_pending_pin_jump = lambda: None
        reader._comparable_pdf_url = lambda url: url
        return reader

    def test_stored_map_answers_when_no_scan_has_been_aligned(self):
        reader = self._reader()
        stored = object()
        reader._stored_location_maps["scholar"] = stored

        self.assertIs(reader._location_map_for("scholar"), stored)

    def test_a_real_alignment_outranks_the_stored_pages(self):
        reader = self._reader()
        aligned = object()
        reader._active_pdf_analysis_key = ("k",)
        reader._pdf_analysis_cache[("k",)] = {"maps": {"scholar": aligned}}
        reader._stored_location_maps["scholar"] = object()

        self.assertIs(reader._location_map_for("scholar"), aligned)

    def test_stored_pages_are_not_offered_as_an_answer_about_a_named_pdf(self):
        # "Does *this* file have a map?" is a question about the file.
        reader = self._reader()
        reader._stored_location_maps["scholar"] = object()

        self.assertIsNone(
            reader._location_map_for("scholar", "https://x/opinion.pdf")
        )

    def test_a_late_arriving_stored_map_never_displaces_a_scan(self):
        reader = self._reader()
        aligned = object()
        reader._active_pdf_analysis_key = ("k",)
        reader._pdf_analysis_cache[("k",)] = {"maps": {"scholar": aligned}}

        reader._apply_stored_pagination(object(), {"v": 1})

        self.assertNotIn("scholar", reader._stored_location_maps)

    def test_installing_stored_pages_refreshes_the_view(self):
        reader = self._reader()
        installed = []
        reader._install_current_location_map = lambda: installed.append(True)
        location_map = types.SimpleNamespace(copy_cite="608 U. S. 1")

        reader._apply_stored_pagination(location_map, {"v": 1})

        self.assertIs(reader._stored_location_maps["scholar"], location_map)
        self.assertEqual(installed, [True])

    def test_a_fresh_alignment_replaces_the_stored_stand_in(self):
        parts = [_part([_block(_span("Opening passage of the opinion. "))])]
        reader = self._reader()
        reader._parts = parts
        reader._stored_location_maps["scholar"] = object()
        saved = []
        reader._app = types.SimpleNamespace(
            _get_opinion_db=lambda: types.SimpleNamespace(
                save_pagination=lambda sid, payload: saved.append(
                    (sid, payload)) or True
            )
        )

        source = opinion_location.build_text_source(parts)
        reader._store_scholar_pagination(opinion_location.OpinionLocationMap(
            source=source,
            anchors=(),
            boundaries=(opinion_location.PageBoundary(
                0, 1, 0, source.address_at(0), 1.0, False),),
            confidence=1.0,
            native_cite="",
            pdf_cite="608 U. S. 1",
            us_cite="608 U. S. 1",
            copy_ready=True,
            copy_cite="608 U. S. 1",
            navigation_ready=True,
            pdf_page_count=1,
        ))

        self.assertNotIn("scholar", reader._stored_location_maps)
        self.assertEqual(reader._saved_pagination_json["cite"], "608 U. S. 1")

    def test_nothing_is_saved_for_an_opinion_with_no_scholar_id(self):
        reader = self._reader()
        reader._scholar_url = "https://example.test/not-scholar"
        reader._parts = [_part([_block(_span("Text. "))])]
        reader._app = types.SimpleNamespace(_get_opinion_db=lambda: None)

        reader._store_scholar_pagination(types.SimpleNamespace(
            copy_ready=True, copy_cite="608 U. S. 1", boundaries=(),
        ))

        self.assertIsNone(reader._saved_pagination_json)


class InstalledPageTrackTests(unittest.TestCase):
    """Which map supplies the reporter pages shown beside the text."""

    @classmethod
    def setUpClass(cls):
        cls.ns = _load_methods(
            "_ScholarTextWindow",
            ("_install_current_location_map", "_mapped_pages_are_us",
             "_location_address"),
            _US_CITE_RE=re.compile(r"\b\d+\s+U\.\s*S\.\s+\d+\b", re.I),
        )

    def _reader(self, live, stored=None):
        ns = self.ns
        reader = type("Reader", (), {
            "_install_current_location_map": ns["_install_current_location_map"],
            "_mapped_pages_are_us": ns["_mapped_pages_are_us"],
            "_location_map_for": lambda _self, *_a, **_k: live,
            "_location_address": staticmethod(ns["_location_address"]),
        })()
        reader._mode = "scholar"
        reader._bb = {"cite": "146 S. Ct. 1131"}
        reader._page_pos = {}
        reader._base_citation_override = ""
        reader._stored_location_maps = {"scholar": stored} if stored else {}
        reader._live_location_anchors = []
        reader._mapped_us_page_pos = {}
        reader._mapped_us_note_pages = {}
        reader._mapped_copy_cite = ""
        reader._fn_region_fids = {}
        reader._current_part = None
        reader._clear_location_map_marks = lambda: None
        reader._clear_justification = lambda: None
        reader._schedule_text_justify = lambda: None
        reader._schedule_gutter_redraw = lambda: None
        reader._retry_pending_pin_jump = lambda: None
        reader._retry_pdf_viewport_request = lambda: None
        reader._fn_marker_pos = lambda _side, _fid: None
        reader._location_index_for_address = lambda address: "1.0"
        reader._new_location_mark = lambda stem, _index: stem
        return reader

    @staticmethod
    def _map(*, copy_ready, pages=(1, 2)):
        return types.SimpleNamespace(
            copy_ready=copy_ready,
            copy_cite="608 U. S. 1" if copy_ready else "",
            anchors=(),
            source=types.SimpleNamespace(runs=()),
            boundaries=tuple(
                types.SimpleNamespace(reporter_page=page, address=object())
                for page in pages
            ),
        )

    def test_a_pin_citable_alignment_supplies_its_own_pages(self):
        reader = self._reader(self._map(copy_ready=True, pages=(5, 6)))
        reader._install_current_location_map()

        self.assertEqual(sorted(reader._mapped_us_page_pos), [5, 6])
        self.assertEqual(reader._mapped_copy_cite, "608 U. S. 1")

    def test_a_coarse_alignment_falls_back_to_the_saved_pages(self):
        # Good enough to scroll between text and scan, too coarse to pin-cite
        # from — the pages already measured and saved stay in charge.
        reader = self._reader(
            self._map(copy_ready=False),
            stored=self._map(copy_ready=True, pages=(9, 10)),
        )
        reader._install_current_location_map()

        self.assertEqual(sorted(reader._mapped_us_page_pos), [9, 10])
        self.assertIs(
            reader._active_text_location_map,
            reader._stored_location_maps["scholar"],
        )

    def test_no_pages_anywhere_leaves_the_track_empty(self):
        reader = self._reader(self._map(copy_ready=False))
        reader._install_current_location_map()

        self.assertEqual(reader._mapped_us_page_pos, {})
        self.assertEqual(reader._mapped_copy_cite, "")


class PinPageTrackTests(unittest.TestCase):
    """A pin cite is looked up in the track its own reporter paginates."""

    @classmethod
    def setUpClass(cls):
        ns = _load_methods("_ScholarTextWindow", ("_pin_page_positions",))
        cls.Reader = type(
            "Reader", (), {"_pin_page_positions": ns["_pin_page_positions"]},
        )

    def _reader(self, native_cite, mapped_cite=""):
        reader = self.Reader()
        reader._bb = {"cite": native_cite}
        reader._page_pos = {1131: "5.0", 1132: "9.0"}
        reader._mapped_us_page_pos = {1: "5.0", 2: "9.0"}
        reader._mapped_copy_cite = mapped_cite
        return reader

    def test_a_us_pin_uses_the_inferred_us_pages(self):
        reader = self._reader("146 S. Ct. 1131", "608 U. S. 1")
        self.assertIs(
            reader._pin_page_positions("608 U. S. 1"),
            reader._mapped_us_page_pos,
        )

    def test_a_reporter_pin_uses_the_printed_star_pages(self):
        reader = self._reader("146 S. Ct. 1131", "608 U. S. 1")
        self.assertIs(
            reader._pin_page_positions("146 S. Ct. 1131"), reader._page_pos,
        )

    def test_the_printed_pages_are_used_when_nothing_was_inferred(self):
        reader = self._reader("146 S. Ct. 1131")
        reader._mapped_us_page_pos = {}
        self.assertIs(
            reader._pin_page_positions("608 U. S. 1"), reader._page_pos,
        )


class FootnoteJumpTests(unittest.TestCase):
    """A footnote pin opens the note, and the page says whose note it is."""

    @classmethod
    def setUpClass(cls):
        names = ("_note_jump_range", "jump_to_cite_page",
                 "_retry_pending_pin_jump", "_tk_index_key")
        ns = _load_methods("_ScholarTextWindow", names)
        cls.Reader = type("Reader", (), {
            "_note_jump_range": ns["_note_jump_range"],
            "jump_to_cite_page": ns["jump_to_cite_page"],
            "_retry_pending_pin_jump": ns["_retry_pending_pin_jump"],
            "_tk_index_key": staticmethod(ns["_tk_index_key"]),
        })

    def _reader(self):
        reader = self.Reader()
        # Two writings, each with its own note 4: the majority's is called on
        # page 13, the dissent's on page 20.
        reader._fn_regions = [
            ("maj4.start", "maj4.end", "4", 13),
            ("dis4.start", "dis4.end", "4", 20),
        ]
        reader._fn_region_fids = {"maj4.start": "fmaj4", "dis4.start": "fdis4"}
        reader._fn_ref_positions = {"fmaj4": "4.0", "fdis4": "30.0"}
        reader._fn_marker_pos = (
            lambda side, fid: reader._fn_ref_positions.get(fid)
        )
        reader._mapped_us_page_pos = {}
        reader._mapped_us_note_pages = {}
        reader._text = _FakeText({
            "maj4.start": "20.0", "maj4.end": "22.0",
            "dis4.start": "48.0", "dis4.end": "50.0",
        })
        reader._pending_pin_jump = None
        reader._bb = {"cite": "200 U.S. 12"}
        reader._page_pos = {13: "3.0", 20: "29.0"}
        reader._mapped_copy_cite = ""
        reader._flashed = []
        reader._flash_range = lambda a, b: reader._flashed.append((a, b))
        reader._status = []
        reader._status_var = types.SimpleNamespace(
            set=lambda msg: reader._status.append(msg)
        )
        reader._win = types.SimpleNamespace(
            after=lambda _ms, fn, *args: fn(*args)
        )
        reader._pin_page_positions = lambda _cite: reader._page_pos
        return reader

    def test_the_majority_note_is_chosen_by_its_page(self):
        reader = self._reader()
        reader.jump_to_cite_page("200 U.S. 12", "13n4")

        self.assertEqual(reader._flashed, [("20.0", "22.0")])
        self.assertEqual(reader._status, ["Jumped to 13 n.4."])

    def test_the_dissent_note_is_chosen_by_its_page(self):
        reader = self._reader()
        reader.jump_to_cite_page("200 U.S. 12", "20n4")

        self.assertEqual(reader._flashed, [("48.0", "50.0")])

    def test_one_set_of_notes_needs_no_page_to_disambiguate(self):
        reader = self._reader()
        reader._fn_regions = [("maj4.start", "maj4.end", "4", None)]
        reader._fn_region_fids = {"maj4.start": "fmaj4"}

        reader.jump_to_cite_page("200 U.S. 12", "13n4")

        self.assertEqual(reader._flashed, [("20.0", "22.0")])

    def test_an_unrecorded_page_falls_back_to_the_nearest_reference(self):
        # Neither note records the page it is called on; the pin page's own
        # position in the document picks the writing.
        reader = self._reader()
        reader._fn_regions = [
            ("maj4.start", "maj4.end", "4", None),
            ("dis4.start", "dis4.end", "4", None),
        ]

        reader.jump_to_cite_page("200 U.S. 12", "20n4")

        self.assertEqual(reader._flashed, [("48.0", "50.0")])

    def test_a_page_pin_still_flashes_the_page(self):
        reader = self._reader()
        reader.jump_to_cite_page("200 U.S. 12", "13")

        self.assertEqual(reader._flashed, [("3.0", "29.0")])
        self.assertEqual(reader._status, ["Jumped to page *13."])

    def test_a_missing_note_is_reported_rather_than_jumped_to(self):
        reader = self._reader()
        reader._fn_regions = []
        reader._fn_region_fids = {}
        reader._page_pos = {}
        reader._pin_page_positions = lambda _cite: {}

        reader.jump_to_cite_page("200 U.S. 12", "13n4")

        self.assertEqual(reader._flashed, [])
        self.assertEqual(reader._status[-1], "Note 4 not found in this text.")

    def test_a_jump_with_nothing_to_land_on_is_kept_for_a_retry(self):
        # The inferred U.S. pages arrive after the first render; the pin cite
        # the window was opened for must not be silently dropped.
        reader = self._reader()
        reader._fn_regions = []
        reader._fn_region_fids = {}
        reader._pin_page_positions = lambda _cite: {}

        reader.jump_to_cite_page("608 U. S. 1", "5")
        self.assertEqual(reader._pending_pin_jump, ("608 U. S. 1", "5"))

        reader._pin_page_positions = lambda _cite: {5: "7.0"}
        reader._retry_pending_pin_jump()

        self.assertEqual(reader._flashed[-1][0], "7.0")
        self.assertIsNone(reader._pending_pin_jump)


if __name__ == "__main__":
    unittest.main()
