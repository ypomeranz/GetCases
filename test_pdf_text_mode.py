"""The floating PDF viewer's text mode — its "T" button.

The minimal viewer shows a scan.  Pressing **T** puts the opinion's text in the
window instead, rendered by an ordinary ``_ScholarTextWindow`` built
*chromeless* into the viewer's body: no button bar of its own, no labelled
parts strip, the page-number gutter narrowed and its numbers shrunk to fit, and
the separate writings marked on the slim rail beside the scrollbar the scan
already uses — so the opinion runs the full width of the window.  The button
becomes **P** and puts the scan back.

While the text is showing the strip changes with it: −/+ size the type instead
of zooming the page, *Fit* gives its place to a *Copy* menu, the readout says
points rather than percent, and save/print export RTF and typeset with LaTeX
(what the case window's own Export menu does) rather than writing the scan out.

Lifted out of ``courtlistener_gui`` with ``ast`` (importing it needs tkinter,
absent on a headless run) and driven against stubs.
"""

import ast
import pathlib
import re
import sys
import typing
import unittest
from unittest import mock


SRC = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
TREE = ast.parse(SRC)


class _Tk:
    """Just the tkinter surface the extracted methods touch — including the
    names used only in their annotations, which Python evaluates at def time."""

    TclError = Exception
    Menu = Misc = Frame = Toplevel = object


def _base_ns(extra=None) -> dict:
    ns = {"tk": _Tk, "sys": sys, "re": re, "Optional": typing.Optional,
          "_ACCEL": "Ctrl"}
    ns.update(extra or {})
    return ns


def _load(cls: str, names, extra=None) -> dict:
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(SRC, n) for n in body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found on {cls}: {missing}")
    ns = _base_ns(extra)
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


def _class_attr(cls: str, name: str):
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    for node in body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return eval(ast.get_source_segment(SRC, node.value),  # noqa: S307
                        {"frozenset": frozenset})
    raise AssertionError(f"{cls} has no {name}")


RAIL_W = _class_attr("_PdfPane", "_RAIL_W")
RAIL_TICK_H = _class_attr("_PdfPane", "_RAIL_TICK_H")
RAIL_EDGE = _class_attr("_PdfPane", "_RAIL_EDGE")
RAIL_BAND_WASH = _class_attr("_PdfPane", "_RAIL_BAND_WASH")
PAGECOL_W = _class_attr("_ScholarTextWindow", "_PAGECOL_W")
PAGECOL_W_TIGHT = _class_attr("_ScholarTextWindow", "_PAGECOL_W_TIGHT")
PARTMAP_W = _class_attr("_ScholarTextWindow", "_PARTMAP_W")
PART_COLORS = {"majority": "#1a3e72", "concurrence": "#1a7a3c",
               "dissent": "#a31515", "separate": "#59636f",
               "syllabus": "#7a5c1a"}


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _Var:
    def __init__(self, value=None):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class _FakeMenu:
    def __init__(self, *_a, **_kw):
        self.items = []

    def add_command(self, **kw):
        self.items.append(("command", kw))

    def add_radiobutton(self, **kw):
        self.items.append(("radiobutton", kw))

    def add_separator(self):
        self.items.append(("separator", {}))

    def tk_popup(self, x, y):
        self.posted = (x, y)

    def grab_release(self):
        pass

    def labels(self):
        return [kw.get("label") for _kind, kw in self.items]


class _FakeButton:
    def __init__(self, name=""):
        self.name = name
        self.text = name
        self.packed = False
        self.pack_calls = []

    def configure(self, **kw):
        self.text = kw.get("text", self.text)

    def pack(self, **kw):
        self.packed = True
        self.pack_calls.append(kw)

    def pack_forget(self):
        self.packed = False

    def winfo_rootx(self):
        return 10

    def winfo_rooty(self):
        return 20

    def winfo_height(self):
        return 22


class _FakePane:
    """The bits of _PdfPane the viewer drives."""

    _MARGIN = 18

    def __init__(self):
        self.zoomed = []
        self.packed = True
        self.destroyed = False
        self.focused = 0
        self.scrolled = []
        self.stepped = []
        self.find_pages = None

    def zoom(self, delta):
        self.zoomed.append(delta)

    def zoom_percent(self):
        return 125 if self.zoomed else 100

    def pack(self, **_kw):
        self.packed = True

    def pack_forget(self):
        self.packed = False

    def take_focus(self):
        self.focused += 1

    def scroll_key(self, direction):
        self.scrolled.append(direction)
        return True

    def has_find(self):
        return self.find_pages is not None

    def _open_find(self):
        self.stepped.append("open")
        return "break"

    def _find_step(self, direction):
        self.stepped.append(direction)
        return "break"

    def destroy(self):
        self.destroyed = True


class _FakeReader:
    """The chromeless _ScholarTextWindow the viewer builds in text mode."""

    def __init__(self, host=None):
        self.host = host
        self._base_size = 11
        self._text = mock.Mock()
        self.zoomed = []
        self.stepped = []
        self.scrolled = []
        self.exported = []

    def _zoom(self, delta):
        self.zoomed.append(delta)
        self._base_size = 11 if delta == 0 else self._base_size + delta

    def _find_open(self):
        self.stepped.append("open")
        return "break"

    def _find_step(self, direction):
        self.stepped.append(direction)
        return "break"

    def _scroll_reader(self, direction):
        self.scrolled.append(direction)
        return True

    def _export_rtf(self):
        self.exported.append("rtf")

    def _export_pdf_latex(self):
        self.exported.append("latex")


class _FakeHostFrame:
    """The _EmbeddedCaseHost the opinion is built into."""

    made = []

    def __init__(self, master, window):
        self.master, self.window = master, window
        self.packed = False
        self.destroyed = False
        _FakeHostFrame.made.append(self)

    def pack(self, **_kw):
        self.packed = True

    def pack_forget(self):
        self.packed = False

    def destroy(self):
        self.destroyed = True


VIEWER_NAMES = [
    "showing_text", "_mode_button_tip", "_toggle_mode", "_build_reader",
    "zoom",
    "_show_text", "_show_scan", "_sync_bar", "_refresh_scale", "_flash",
    "_post_copy_menu", "_bigger", "_smaller", "_reset_scale", "_rescale",
    "_scroll_key", "_find_open", "_find_step", "_drop_reader", "_show_zoom",
    "_save_label", "_print_label", "_save", "_print", "_on_destroy", "alive",
]

VIEWER_NS = _load(
    "_FloatingPdfWindow", VIEWER_NAMES,
    {"_EmbeddedCaseHost": _FakeHostFrame,
     "_build_copy_menu": lambda master, reader: (
         None if reader is None else _FakeMenu())},
)


class _Viewer:
    def __init__(self, build_text="reader", pane=None):
        self._pane = _FakePane() if pane is None else pane
        self._url = "https://example.test/a.pdf"
        self._closing = False
        self._mode = "pdf"
        self._text_host = None
        self._reader = None
        self._flash_after = None
        self._zoom_var = _Var("100%")
        self._win = mock.Mock()
        self._win.after.return_value = "after#1"
        self._body = object()
        self._app = None
        self.built = []
        self.printed = []
        self.saved = 0
        self.closed_with = []
        self._on_save = lambda: setattr(self, "saved", self.saved + 1)
        self._on_print = self.printed.append
        self._on_close = self.closed_with.append
        if build_text == "reader":
            def build(host):
                reader = _FakeReader(host)
                self.built.append(reader)
                return reader
            self._on_build_text = build
        else:
            self._on_build_text = build_text
        self._mode_btn = _FakeButton("T")
        self._fit_btn = _FakeButton("Fit")
        self._fit_btn.packed = True
        self._copy_btn = _FakeButton("Copy ▾")
        self._zoom_label = object()
        for name in VIEWER_NAMES:
            setattr(self, name, VIEWER_NS[name].__get__(self))


def _in_text_mode(**kw):
    viewer = _Viewer(**kw)
    viewer._toggle_mode()
    return viewer


# ---------------------------------------------------------------------------
# The switch itself
# ---------------------------------------------------------------------------


class ModeSwitchTests(unittest.TestCase):
    def setUp(self):
        _FakeHostFrame.made = []

    def test_the_viewer_opens_on_the_scan(self):
        viewer = _Viewer()
        self.assertFalse(viewer.showing_text())
        self.assertEqual(viewer._mode_btn.text, "T")

    def test_t_puts_the_opinion_text_in_the_window(self):
        viewer = _in_text_mode()
        self.assertTrue(viewer.showing_text())
        self.assertIs(viewer._reader, viewer.built[0])
        self.assertTrue(viewer._text_host.packed)

    def test_the_scan_is_only_hidden_not_thrown_away(self):
        # Switching back has to be instant: re-rendering the pages would make
        # the button feel like re-opening the PDF.
        viewer = _in_text_mode()
        self.assertFalse(viewer._pane.packed)
        self.assertIsNotNone(viewer._pane)
        self.assertFalse(viewer._pane.destroyed)

    def test_the_button_becomes_p(self):
        viewer = _in_text_mode()
        self.assertEqual(viewer._mode_btn.text, "P")

    def test_and_p_puts_the_scan_back(self):
        viewer = _in_text_mode()
        viewer._toggle_mode()
        self.assertFalse(viewer.showing_text())
        self.assertEqual(viewer._mode_btn.text, "T")
        self.assertTrue(viewer._pane.packed)
        self.assertFalse(viewer._text_host.packed)

    def test_the_scan_takes_the_keyboard_back_with_it(self):
        viewer = _in_text_mode()
        before = viewer._pane.focused
        viewer._toggle_mode()
        self.assertEqual(viewer._pane.focused, before + 1)

    def test_the_opinion_takes_the_keyboard_when_it_appears(self):
        viewer = _in_text_mode()
        viewer._reader._text.focus_set.assert_called_once_with()

    def test_the_tooltip_names_what_the_press_would_show(self):
        viewer = _Viewer()
        self.assertEqual(viewer._mode_button_tip(), "Show the opinion text")
        viewer._toggle_mode()
        self.assertEqual(viewer._mode_button_tip(), "Show the scanned pages")

    def test_the_opinion_is_built_once_and_kept(self):
        viewer = _in_text_mode()
        viewer._toggle_mode()   # back to the scan
        viewer._toggle_mode()   # and to the text again
        self.assertEqual(len(viewer.built), 1)
        self.assertEqual(len(_FakeHostFrame.made), 1)

    def test_it_is_built_into_a_frame_of_the_viewer_s_body(self):
        viewer = _in_text_mode()
        host = _FakeHostFrame.made[0]
        self.assertIs(host.master, viewer._body)
        self.assertIs(host.window, viewer)
        self.assertIs(viewer._reader.host, host)


class NoTextYetTests(unittest.TestCase):
    """The background fetch may not have finished — or found nothing."""

    def setUp(self):
        _FakeHostFrame.made = []

    def test_the_window_stays_on_the_scan(self):
        viewer = _Viewer(build_text=lambda host: None)
        viewer._toggle_mode()
        self.assertFalse(viewer.showing_text())
        self.assertTrue(viewer._pane.packed)
        self.assertEqual(viewer._mode_btn.text, "T")

    def test_and_says_so_where_the_scale_is(self):
        viewer = _Viewer(build_text=lambda host: None)
        viewer._toggle_mode()
        self.assertEqual(viewer._zoom_var.get(), "Text not ready")

    def test_the_message_is_taken_back_off_again(self):
        viewer = _Viewer(build_text=lambda host: None)
        viewer._toggle_mode()
        restore = viewer._win.after.call_args[0][1]
        viewer._flash_after = None      # what the callback does first
        restore()
        self.assertEqual(viewer._zoom_var.get(), "100%")

    def test_the_empty_frame_is_not_left_behind(self):
        viewer = _Viewer(build_text=lambda host: None)
        viewer._toggle_mode()
        self.assertTrue(_FakeHostFrame.made[0].destroyed)
        self.assertIsNone(viewer._text_host)

    def test_a_viewer_with_no_text_to_offer_at_all_is_harmless(self):
        viewer = _Viewer(build_text=None)
        viewer._toggle_mode()
        self.assertFalse(viewer.showing_text())

    def test_a_builder_that_raises_leaves_the_scan_showing(self):
        def boom(host):
            raise RuntimeError("no")

        viewer = _Viewer(build_text=boom)
        viewer._toggle_mode()
        self.assertFalse(viewer.showing_text())
        self.assertTrue(viewer._pane.packed)

    def test_asking_again_later_builds_it(self):
        # The text usually arrives a moment after the scan does.
        holder = {}
        viewer = _Viewer(build_text=lambda host: holder.get("reader"))
        viewer._toggle_mode()
        self.assertFalse(viewer.showing_text())
        holder["reader"] = _FakeReader()
        viewer._toggle_mode()
        self.assertTrue(viewer.showing_text())


# ---------------------------------------------------------------------------
# The strip follows the surface
# ---------------------------------------------------------------------------


class StripFollowsTheSurfaceTests(unittest.TestCase):
    def test_fit_belongs_to_the_page(self):
        viewer = _Viewer()
        self.assertTrue(viewer._fit_btn.packed)
        self.assertFalse(viewer._copy_btn.packed)

    def test_the_copy_menu_takes_its_place_on_the_text(self):
        viewer = _in_text_mode()
        self.assertTrue(viewer._copy_btn.packed)
        self.assertFalse(viewer._fit_btn.packed)

    def test_and_gives_it_back(self):
        viewer = _in_text_mode()
        viewer._toggle_mode()
        self.assertTrue(viewer._fit_btn.packed)
        self.assertFalse(viewer._copy_btn.packed)

    def test_whichever_it_is_sits_where_fit_did(self):
        # Before the readout, after the T/P button: the same slot.
        viewer = _in_text_mode()
        self.assertIs(viewer._copy_btn.pack_calls[-1]["before"],
                      viewer._zoom_label)
        self.assertEqual(viewer._copy_btn.pack_calls[-1]["side"], "left")

    def test_the_readout_is_a_percentage_on_the_scan(self):
        viewer = _Viewer()
        viewer._refresh_scale()
        self.assertEqual(viewer._zoom_var.get(), "100%")

    def test_and_a_type_size_on_the_text(self):
        viewer = _in_text_mode()
        self.assertEqual(viewer._zoom_var.get(), "11 pt")

    def test_a_zoom_report_from_the_hidden_pane_is_ignored(self):
        # The pane keeps its own zoom while the text is showing; it must not
        # write a percentage over the type size.
        viewer = _in_text_mode()
        viewer._show_zoom(150)
        self.assertEqual(viewer._zoom_var.get(), "11 pt")

    def test_a_zoom_report_does_not_wipe_a_message(self):
        viewer = _Viewer()
        viewer._flash("Text not ready")
        viewer._show_zoom(150)
        self.assertEqual(viewer._zoom_var.get(), "Text not ready")


class CopyMenuTests(unittest.TestCase):
    def test_the_button_posts_the_reader_s_copy_menu(self):
        viewer = _in_text_mode()
        viewer._post_copy_menu()   # must not raise

    def test_it_is_the_menu_the_case_window_carries(self):
        # One builder, so the styles offered here are the styles offered there.
        self.assertIn("_build_copy_menu",
                      _source_of("_FloatingPdfWindow", "_post_copy_menu"))
        built_by = next(ast.get_source_segment(SRC, n) for n in TREE.body
                        if isinstance(n, ast.FunctionDef)
                        and n.name == "_add_copy_cascade")
        self.assertIn("_build_copy_menu", built_by)

    def test_nothing_to_copy_from_posts_nothing(self):
        viewer = _Viewer()
        viewer._post_copy_menu()   # no reader built yet; must not raise

    def test_the_shared_menu_offers_every_copy_style(self):
        ns = _base_ns({"COPY_MODE_LABELS": (("plain", "Plain"),
                                            ("cite", "With citation")),
                       "tk": type("T", (_Tk,), {"Menu": _FakeMenu})})
        src = next(ast.get_source_segment(SRC, n) for n in TREE.body
                   if isinstance(n, ast.FunctionDef)
                   and n.name == "_build_copy_menu")
        exec(src, ns)
        reader = mock.Mock()
        menu = ns["_build_copy_menu"](None, reader)
        self.assertEqual(
            menu.labels(),
            ["Copy citation to clipboard", None, "Plain", "With citation",
             None, "Copy now \tCtrl+C"],
        )


# ---------------------------------------------------------------------------
# − and + drive whichever surface is showing
# ---------------------------------------------------------------------------


class ScaleTests(unittest.TestCase):
    def test_plus_zooms_the_page(self):
        viewer = _Viewer()
        viewer._bigger()
        self.assertEqual(viewer._pane.zoomed, [+1])

    def test_minus_zooms_the_page_out(self):
        viewer = _Viewer()
        viewer._smaller()
        self.assertEqual(viewer._pane.zoomed, [-1])

    def test_zero_fits_the_page(self):
        viewer = _Viewer()
        viewer._reset_scale()
        self.assertEqual(viewer._pane.zoomed, [0])

    def test_plus_grows_the_type_on_the_text(self):
        viewer = _in_text_mode()
        viewer._bigger()
        self.assertEqual(viewer._reader.zoomed, [+1])
        self.assertEqual(viewer._reader._base_size, 12)
        self.assertEqual(viewer._pane.zoomed, [])   # the scan is left alone

    def test_minus_shrinks_it(self):
        viewer = _in_text_mode()
        viewer._smaller()
        self.assertEqual(viewer._reader._base_size, 10)

    def test_zero_restores_the_default_size(self):
        viewer = _in_text_mode()
        viewer._bigger()
        viewer._bigger()
        viewer._reset_scale()
        self.assertEqual(viewer._reader._base_size, 11)

    def test_the_readout_follows_the_type_size(self):
        viewer = _in_text_mode()
        viewer._bigger()
        self.assertEqual(viewer._zoom_var.get(), "12 pt")


class ScrollAndFindTests(unittest.TestCase):
    def test_the_arrows_scroll_the_page(self):
        viewer = _Viewer()
        self.assertTrue(viewer._scroll_key(+1))
        self.assertEqual(viewer._pane.scrolled, [+1])

    def test_and_the_opinion_when_that_is_what_is_showing(self):
        viewer = _in_text_mode()
        self.assertTrue(viewer._scroll_key(-1))
        self.assertEqual(viewer._reader.scrolled, [-1])
        self.assertEqual(viewer._pane.scrolled, [])

    def test_nothing_to_scroll_leaves_the_key_alone(self):
        viewer = _Viewer()
        viewer._pane = None
        self.assertFalse(viewer._scroll_key(+1))

    def test_find_searches_the_scan_s_text_layer(self):
        viewer = _Viewer()
        viewer._pane.find_pages = [["c"]]
        viewer._find_open()
        self.assertEqual(viewer._pane.stepped, ["open"])

    def test_a_scan_with_no_text_layer_has_nothing_to_find(self):
        viewer = _Viewer()
        self.assertIsNone(viewer._find_open())

    def test_find_searches_the_opinion_when_that_is_showing(self):
        viewer = _in_text_mode()
        viewer._find_open()
        viewer._find_step(+1)
        self.assertEqual(viewer._reader.stepped, ["open", +1])
        self.assertEqual(viewer._pane.stepped, [])

    def test_the_pane_no_longer_grabs_the_find_keys_for_itself(self):
        # Two find bars on one window otherwise, once the text is embedded.
        self.assertIn("bind_keys=False",
                      _source_of("_FloatingPdfWindow", "apply_analysis"))

    def test_the_window_binds_them_instead(self):
        self.assertIn("_bind_find_keys(self._win, self._find_open",
                      _source_of("_FloatingPdfWindow", "__init__"))


# ---------------------------------------------------------------------------
# Save and print
# ---------------------------------------------------------------------------


class SaveAndPrintTests(unittest.TestCase):
    def test_the_scan_is_saved_and_printed_as_before(self):
        viewer = _Viewer()
        viewer._save()
        viewer._print()
        self.assertEqual(viewer.saved, 1)
        self.assertEqual(viewer.printed, [viewer._pane])

    def test_the_download_button_writes_rtf_on_the_text(self):
        viewer = _in_text_mode()
        viewer._save()
        self.assertEqual(viewer._reader.exported, ["rtf"])
        self.assertEqual(viewer.saved, 0)

    def test_and_print_typesets_it_with_latex(self):
        viewer = _in_text_mode()
        viewer._print()
        self.assertEqual(viewer._reader.exported, ["latex"])
        self.assertEqual(viewer.printed, [])

    def test_the_hover_labels_say_which(self):
        viewer = _Viewer()
        self.assertIn("Save PDF As…", viewer._save_label())
        self.assertIn("Print…", viewer._print_label())
        viewer._toggle_mode()
        self.assertIn("Rich Text", viewer._save_label())
        self.assertIn("LaTeX", viewer._print_label())

    def test_the_accelerators_are_named_on_both(self):
        viewer = _Viewer()
        for label in (viewer._save_label(), viewer._print_label()):
            self.assertIn("Ctrl+", label)


# ---------------------------------------------------------------------------
# Lifetime
# ---------------------------------------------------------------------------


class LifetimeTests(unittest.TestCase):
    def setUp(self):
        _FakeHostFrame.made = []

    def test_another_document_lets_go_of_the_opinion(self):
        # A different scan is a different case; the text rendered for the last
        # one must not be what T shows now.
        viewer = _in_text_mode()
        host = viewer._text_host
        viewer._drop_reader()
        self.assertIsNone(viewer._reader)
        self.assertTrue(host.destroyed)
        self.assertEqual(viewer._mode, "pdf")

    def test_replacing_the_document_does_exactly_that(self):
        self.assertIn("self._drop_reader()",
                      _source_of("_FloatingPdfWindow", "set_pdf"))

    def test_closing_the_window_lets_go_of_it_too(self):
        viewer = _in_text_mode()
        viewer._on_destroy(mock.Mock(widget=viewer._win))
        self.assertIsNone(viewer._reader)
        self.assertIsNone(viewer._text_host)

    def test_an_inner_widget_going_away_is_not_the_window_closing(self):
        viewer = _in_text_mode()
        reader = viewer._reader
        viewer._on_destroy(mock.Mock(widget=object()))
        self.assertIs(viewer._reader, reader)


# ---------------------------------------------------------------------------
# The chromeless reader: what it drops, and what it narrows
# ---------------------------------------------------------------------------


class ChromelessReaderTests(unittest.TestCase):
    """The opinion built into the viewer, versus the same one in a window."""

    def test_the_flag_is_a_constructor_argument(self):
        src = _source_of("_ScholarTextWindow", "__init__")
        self.assertIn("chromeless: bool = False", src)
        self.assertIn("self._chromeless = bool(chromeless)", src)

    def test_it_fills_the_frame_it_was_given(self):
        # No new Toplevel, and no page in the shared tab window: the caller has
        # already made the frame.
        src = _source_of("_ScholarTextWindow", "__init__")
        self.assertIn(
            "self._win = parent if self._chromeless else "
            "_case_view_host(parent, app)", src)

    def test_its_own_button_bar_is_never_packed(self):
        src = _source_of("_ScholarTextWindow", "_build_ui")
        self.assertIn("if not self._chromeless:\n            btn_frame.pack",
                      src)

    def test_the_opinion_runs_to_the_edge_of_the_window(self):
        src = _source_of("_ScholarTextWindow", "_build_ui")
        self.assertIn("padx=0 if self._chromeless else 8", src)
        self.assertIn("pady=0 if self._chromeless else 4", src)

    def test_it_takes_no_menu_bar(self):
        src = _source_of("_ScholarTextWindow", "__init__")
        self.assertIn("None if self._chromeless else", src)

    def test_it_is_not_a_case_window_of_its_own(self):
        # History and "bring the open text forward" must find the real reader,
        # not this second rendering of the same case.
        self.assertIn("if self._chromeless:",
                      _source_of("_ScholarTextWindow", "_record_history"))

    def test_it_never_widens_the_window_for_the_side_panel(self):
        src = _source_of("_ScholarTextWindow", "_can_resize_for_details")
        self.assertIn("_EmbeddedCaseHost", src)

    def test_and_does_not_open_that_panel_on_its_own(self):
        src = _source_of("_ScholarTextWindow", "_build_ui")
        self.assertIn("if self._is_scotus and not self._chromeless:", src)

    def test_the_window_keys_belong_to_the_viewer(self):
        src = _source_of("_ScholarTextWindow", "_build_ui")
        self.assertIn("window_keys=not self._chromeless", src)
        self.assertIn("if not self._chromeless:   # the viewer's −/+ buttons",
                      src)

    def test_it_does_not_go_looking_for_a_pdf(self):
        # The window it is inside is *showing* the scan, and there is no PDF
        # button here to enable or grey out — the lookup would be a network
        # round trip for nothing.
        src = _source_of("_ScholarTextWindow", "_locate_pdf")
        self.assertIn("if self._chromeless:", src)
        self.assertLess(src.index("if self._chromeless:"),
                        src.index("_pdf_locate_started"))


PAGECOL_NS = _load(
    "_ScholarTextWindow", ["_pagecol_width", "_fit_pagecol_font"])


class _FakeFont:
    """A monospace-ish font: every digit is `per_digit` pixels wide."""

    def __init__(self, size=9, per_digit=3):
        self._size = size
        self._per_digit = per_digit

    def configure(self, size=None):
        if size is not None:
            self._size = size

    def cget(self, _what):
        return self._size

    def measure(self, text):
        return len(text) * self._per_digit * self._size // 9


class _GutterReader:
    _PAGECOL_W = PAGECOL_W
    _PAGECOL_W_TIGHT = PAGECOL_W_TIGHT

    def __init__(self, chromeless=False, per_digit=3, base_size=11):
        self._chromeless = chromeless
        self._base_size = base_size
        self._pagecol_font = _FakeFont(max(base_size - 2, 7), per_digit)
        for name in ("_pagecol_width", "_fit_pagecol_font"):
            setattr(self, name, PAGECOL_NS[name].__get__(self))


class PageGutterTests(unittest.TestCase):
    def test_a_window_reader_keeps_the_roomy_gutter(self):
        self.assertEqual(_GutterReader()._pagecol_width(), PAGECOL_W)

    def test_the_embedded_one_narrows_it(self):
        self.assertEqual(_GutterReader(chromeless=True)._pagecol_width(),
                         PAGECOL_W_TIGHT)
        self.assertLess(PAGECOL_W_TIGHT, PAGECOL_W)

    def test_three_digit_pages_need_no_shrinking(self):
        reader = _GutterReader(chromeless=True, per_digit=3)
        reader._fit_pagecol_font({113: "i", 114: "i"}, {}, PAGECOL_W_TIGHT)
        self.assertEqual(reader._pagecol_font.cget("size"), 9)

    def test_four_digit_pages_shrink_until_they_fit(self):
        reader = _GutterReader(chromeless=True, per_digit=9)
        reader._fit_pagecol_font({1132: "i", 1133: "i"}, {}, PAGECOL_W_TIGHT)
        size = reader._pagecol_font.cget("size")
        self.assertLess(size, 9)
        self.assertLessEqual(reader._pagecol_font.measure("0000"),
                             PAGECOL_W_TIGHT - 6)

    def test_two_series_share_the_gutter(self):
        # Inferred U.S. pages sit beside the source's own, so each column has
        # about half the room and the numbers shrink sooner.
        alone = _GutterReader(chromeless=True, per_digit=6)
        alone._fit_pagecol_font({113: "i"}, {}, PAGECOL_W_TIGHT)
        shared = _GutterReader(chromeless=True, per_digit=6)
        shared._fit_pagecol_font({113: "i"}, {450: "i"}, PAGECOL_W_TIGHT)
        self.assertLess(shared._pagecol_font.cget("size"),
                        alone._pagecol_font.cget("size"))

    def test_it_never_shrinks_past_legibility(self):
        reader = _GutterReader(chromeless=True, per_digit=40)
        reader._fit_pagecol_font({1132: "i"}, {}, PAGECOL_W_TIGHT)
        self.assertGreaterEqual(reader._pagecol_font.cget("size"), 6)

    def test_no_pages_leaves_the_font_alone(self):
        reader = _GutterReader(chromeless=True)
        reader._fit_pagecol_font({}, {}, PAGECOL_W_TIGHT)
        self.assertEqual(reader._pagecol_font.cget("size"), 9)

    def test_the_gutter_is_drawn_at_the_width_the_reader_allows(self):
        src = _source_of("_ScholarTextWindow", "_draw_page_column")
        self.assertIn("width = self._pagecol_width()", src)
        self.assertIn("self._fit_pagecol_font(page_pos, us_page_pos, width)",
                      src)


# ---------------------------------------------------------------------------
# The parts rail replaces the labelled map
# ---------------------------------------------------------------------------


class _RailCanvas:
    def __init__(self, height=600):
        self.height = height
        self.width_set = None
        self.rects = []
        self.lines = []
        self.texts = []

    def delete(self, _what):
        self.rects, self.lines, self.texts = [], [], []

    def config(self, **kw):
        if "width" in kw:
            self.width_set = kw["width"]

    def winfo_height(self):
        return self.height

    def create_rectangle(self, *coords, **kw):
        self.rects.append((tuple(coords), kw.get("fill")))

    def create_line(self, *coords, **kw):
        self.lines.append((tuple(coords), kw.get("fill")))

    def create_text(self, *coords, **kw):
        self.texts.append(kw.get("text"))
        return len(self.texts)

    def bbox(self, tid):
        return (0, 0, 80, 15)      # the labelled map measures what it drew

    def move(self, _item, _dx, _dy):
        pass


RAIL_NS = _load(
    "_ScholarTextWindow",
    ["_draw_part_map", "_draw_part_rail", "_partmap_short_label"],
    {"_PdfPane": type("_PdfPane", (), {
        "_RAIL_W": RAIL_W, "_RAIL_TICK_H": RAIL_TICK_H,
        "_RAIL_EDGE": RAIL_EDGE, "_RAIL_BAND_WASH": RAIL_BAND_WASH}),
     "_wash_hex": lambda color, toward: f"wash({color})",
     "_PARTMAP_COLORS": PART_COLORS},
)


class _Part:
    def __init__(self, kind, label=""):
        self.kind, self.label = kind, label or kind.title()


class _RailReader:
    _PARTMAP_W = PARTMAP_W
    _PARTMAP_COLORS = PART_COLORS

    def __init__(self, kinds, starts, total=10000, chromeless=True,
                 height=600):
        self._chromeless = chromeless
        self._partmap = _RailCanvas(height)
        self._partmap_rows = []
        self._mode = "scholar"
        self._current_part = None
        self._rendered_parts = [_Part(k) for k in kinds]
        self._text = mock.Mock(winfo_height=lambda: height)
        self._total = total
        self._starts = {f"s{i}": s for i, s in enumerate(starts)}
        self._regions = [(f"s{i}", f"e{i}", i) for i in range(len(kinds))]
        self._partmap_font = mock.Mock(metrics=lambda _w: 15)
        for name in ("_draw_part_map", "_draw_part_rail"):
            setattr(self, name, RAIL_NS[name].__get__(self))
        self._partmap_short_label = RAIL_NS["_partmap_short_label"]

    def _part_region_indices(self):
        return self._regions

    def _ypixels(self, index):
        return self._total if index == "end-1c" else self._starts[index]


def _rail(**kw):
    reader = _RailReader(["majority", "concurrence", "dissent"],
                         [0, 5000, 8000], **kw)
    reader._draw_part_map()
    return reader


class PartRailTests(unittest.TestCase):
    def test_a_chromeless_reader_gets_the_rail_not_the_map(self):
        reader = _rail()
        self.assertEqual(reader._partmap.width_set, RAIL_W)
        self.assertEqual(reader._partmap.texts, [])   # no labels on a rail

    def test_a_window_reader_still_gets_the_labelled_map(self):
        reader = _RailReader(["majority", "dissent"], [0, 8000],
                             chromeless=False)
        reader._draw_part_map()
        self.assertEqual(reader._partmap.width_set, PARTMAP_W)
        self.assertTrue(reader._partmap.texts)

    def test_the_rail_is_narrow_enough_to_leave_the_text_the_window(self):
        self.assertLess(RAIL_W, PARTMAP_W // 4)

    def test_each_part_covers_the_stretch_it_occupies(self):
        reader = _rail()
        bands = [c for c, fill in reader._partmap.rects
                 if str(fill).startswith("wash(")]
        self.assertEqual(len(bands), 3)
        # 0/10000, 5000/10000, 8000/10000 of a 600px rail.
        self.assertEqual([round(b[1]) for b in bands], [0, 300, 480])
        self.assertEqual([round(b[3]) for b in bands], [300, 480, 600])

    def test_the_bands_tile_the_rail_with_no_gaps(self):
        reader = _rail()
        bands = sorted(c for c, fill in reader._partmap.rects
                       if str(fill).startswith("wash("))
        for earlier, later in zip(bands, bands[1:]):
            self.assertEqual(round(earlier[3]), round(later[1]))

    def test_each_band_is_tipped_in_its_own_colour(self):
        reader = _rail()
        ticks = [(c, fill) for c, fill in reader._partmap.rects
                 if not str(fill).startswith("wash(")]
        self.assertEqual([fill for _c, fill in ticks],
                         [PART_COLORS["majority"], PART_COLORS["concurrence"],
                          PART_COLORS["dissent"]])
        for coords, _fill in ticks:
            self.assertLessEqual(coords[3] - coords[1], RAIL_TICK_H)

    def test_the_rail_is_drawn_beside_the_scrollbar_not_over_the_text(self):
        reader = _rail()
        self.assertEqual([c for c, fill in reader._partmap.lines
                          if fill == RAIL_EDGE][0][:2], (0, 0))

    def test_a_click_on_a_band_jumps_to_that_part(self):
        reader = _rail()
        self.assertEqual([row[2] for row in reader._partmap_rows],
                         ["s0", "s1", "s2"])

    def test_an_opinion_with_nothing_to_map_takes_no_rail_at_all(self):
        reader = _RailReader(["majority"], [0])
        reader._draw_part_map()
        self.assertEqual(reader._partmap.width_set, 0)
        self.assertEqual(reader._partmap_rows, [])

    def test_the_syllabus_is_a_band_of_its_own(self):
        # On a scan it is the first thing the rail shows; the text is no
        # different.
        reader = _RailReader(["syllabus", "majority"], [0, 2000])
        reader._draw_part_map()
        self.assertEqual(len(reader._partmap_rows), 2)

    def test_a_single_part_view_takes_the_rail_away(self):
        reader = _rail()
        reader._current_part = 1
        reader._draw_part_map()
        self.assertEqual(reader._partmap.width_set, 0)

    def test_so_does_the_pdf_view(self):
        reader = _rail()
        reader._mode = "pdf"
        reader._draw_part_map()
        self.assertEqual(reader._partmap.width_set, 0)


# ---------------------------------------------------------------------------
# Where the text comes from
# ---------------------------------------------------------------------------


class _FakeEmbedded:
    made = []

    def __init__(self, host, app, url, html, **kw):
        self.host, self.app, self.url, self.html, self.kw = (
            host, app, url, html, kw)
        _FakeEmbedded.made.append(self)


EMBED_NS = _load("_ScholarTextWindow", ["_embed_text_reader"],
                 {"_ScholarTextWindow": _FakeEmbedded})


class _SourceReader:
    def __init__(self, **kw):
        self._app = object()
        self._item = {"caseName": "Roe v. Wade"}
        self._cl_primary = False
        self._cl_text = None
        self._cl_parts = None
        self._cl_blocks = None
        self._primary_source_kind = "courtlistener"
        self._primary_source_label = "CourtListener"
        self._primary_source_url = ""
        self._history_pdf_url = ""
        self._scholar_url = "https://scholar.test/roe"
        self._history_html = "<p>opinion</p>"
        self.__dict__.update(kw)
        self._embed_text_reader = EMBED_NS["_embed_text_reader"].__get__(self)


class TextSourceTests(unittest.TestCase):
    def setUp(self):
        _FakeEmbedded.made = []

    def test_the_reader_renders_the_opinion_it_already_holds(self):
        built = _SourceReader()._embed_text_reader("host")
        self.assertEqual(built.url, "https://scholar.test/roe")
        self.assertEqual(built.html, "<p>opinion</p>")
        self.assertEqual(built.host, "host")

    def test_it_is_built_chromeless(self):
        built = _SourceReader()._embed_text_reader("host")
        self.assertTrue(built.kw["chromeless"])

    def test_and_fetches_no_pdf_of_its_own(self):
        # The viewer around it already has the scan.
        built = _SourceReader()._embed_text_reader("host")
        self.assertFalse(built.kw["prefetch_pdf"])

    def test_a_courtlistener_opinion_carries_its_parts_over(self):
        reader = _SourceReader(_cl_primary=True, _cl_text="text",
                               _cl_parts=["p"], _cl_blocks=["b"])
        built = reader._embed_text_reader("host")
        self.assertEqual(built.kw["cl_parts"], ["p"])
        self.assertEqual(built.kw["cl_blocks"], ["b"])
        self.assertEqual(built.url, "")

    def test_a_case_law_scan_keeps_being_one(self):
        reader = _SourceReader(_cl_primary=True, _primary_source_kind="case_law",
                               _primary_source_label="Caselaw Access Project",
                               _history_pdf_url="https://static.case.law/x.pdf")
        built = reader._embed_text_reader("host")
        self.assertEqual(built.kw["primary_source_kind"], "case_law")
        self.assertEqual(built.kw["history_pdf_url"],
                         "https://static.case.law/x.pdf")

    def test_the_viewer_is_told_how_to_build_it(self):
        self.assertIn("on_build_text=self._embed_text_reader",
                      _source_of("_ScholarTextWindow", "_show_pdf_floating"))

    def test_a_cited_case_renders_the_page_warmed_behind_the_scan(self):
        src = _source_of("CourtListenerGUI", "_embed_cited_case_text")
        self.assertIn("chromeless=True", src)
        self.assertIn("prefetch_pdf=False", src)

    def test_and_nothing_at_all_until_that_page_has_arrived(self):
        src = _source_of("CourtListenerGUI", "_embed_cited_case_text")
        self.assertIn("if not page:", src)
        self.assertIn("return None", src)

    def test_the_warm_fetch_keeps_the_page_for_it(self):
        src = _source_of("CourtListenerGUI", "_warm_case_text")
        self.assertIn("on_page", src)
        self.assertIn("on_page(*fetched)", src)


# ---------------------------------------------------------------------------
# The host frame the opinion is built into
# ---------------------------------------------------------------------------


class EmbeddedHostTests(unittest.TestCase):
    def test_it_answers_the_toplevel_api_a_reader_expects(self):
        body = next(n for n in TREE.body if isinstance(n, ast.ClassDef)
                    and n.name == "_EmbeddedCaseHost")
        names = {n.name for n in body.body if isinstance(n, ast.FunctionDef)}
        self.assertTrue(
            {"title", "geometry", "minsize", "config", "protocol", "transient",
             "bind"} <= names)

    def test_geometry_requests_are_ignored(self):
        src = _source_of("_EmbeddedCaseHost", "geometry")
        self.assertIn('return "" if spec is None else None', src)

    def test_the_menu_bar_is_dropped(self):
        self.assertIn('options.pop("menu", None)',
                      _source_of("_EmbeddedCaseHost", "config"))

    def test_window_keys_go_to_the_window_the_keys_arrive_at(self):
        src = _source_of("_EmbeddedCaseHost", "bind")
        self.assertIn("self.winfo_toplevel().bind(sequence, func", src)

    def test_it_does_not_rename_the_viewer(self):
        # The viewer names itself after the case as the reporter on screen
        # cites it — more particular than the reader's own title.
        src = _source_of("_EmbeddedCaseHost", "title")
        self.assertNotIn("set_title", src)


if __name__ == "__main__":
    unittest.main()
