"""Which keys open the find bar, and which view they search.

Two separate faults were behind "Ctrl-F does nothing":

  * On macOS the find key is Cmd-F.  Tk does not fold it into ``<Control-f>``,
    so a Ctrl-F-only binding leaves a Mac keyboard with no way into the find
    bar in any window.
  * In the opinion reader the PDF pane was created with ``bind_keys=False``
    and the window's Ctrl-F went to the text finder, which bails out quietly
    when the text view is hidden — so with the PDF showing, the key did
    nothing on every platform.

``courtlistener_gui`` imports tkinter, so the binder and the reader's three
routing methods are lifted out with ``ast`` and driven against stubs.
"""

import ast
import pathlib
import sys
import typing
import unittest
from unittest import mock


def _load(*names, cls=None):
    """Source-exec the named functions out of the GUI module.

    ``cls`` picks the class whose methods to take — ``_find_step`` is defined
    on both the PDF pane and the reader, so the name alone is ambiguous.
    """
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    scope = tree.body
    if cls is not None:
        scope = next(n.body for n in tree.body
                     if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(src, n) for n in scope
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found in {cls or 'module'}: {missing}")
    ns = {"sys": sys, "Optional": typing.Optional,
          "tk": type("tk", (), {
              "TclError": Exception, "Misc": object, "Text": object,
          })}
    for name in names:
        exec(found[name], ns)
    return ns


class _FakeWindow:
    """Records bindings the way Tk stores them: one callback per sequence."""

    def __init__(self):
        self.bindings = {}

    def bind(self, sequence, callback):
        self.bindings[sequence] = callback

    def press(self, sequence):
        return self.bindings[sequence]()


class _Calls:
    def __init__(self):
        self.log = []

    def cb(self, name):
        return lambda: self.log.append(name)


class BindFindKeysTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bind = staticmethod(_load("_bind_find_keys")["_bind_find_keys"])

    def _bound(self, platform):
        win, calls = _FakeWindow(), _Calls()
        with mock.patch.object(sys, "platform", platform):
            self.bind(win, calls.cb("open"), calls.cb("next"),
                      calls.cb("prev"))
        return win, calls

    def test_the_portable_keys_are_bound_everywhere(self):
        for platform in ("win32", "linux", "darwin"):
            with self.subTest(platform=platform):
                win, _calls = self._bound(platform)
                for seq in ("<Control-f>", "<F3>", "<Shift-F3>"):
                    self.assertIn(seq, win.bindings)

    def test_macos_also_gets_the_command_keys(self):
        win, _calls = self._bound("darwin")
        self.assertIn("<Command-f>", win.bindings)
        self.assertIn("<Command-g>", win.bindings)
        self.assertTrue({"<Command-Shift-G>", "<Command-Shift-g>"}
                        <= set(win.bindings))

    def test_other_platforms_do_not_get_them(self):
        for platform in ("win32", "linux"):
            with self.subTest(platform=platform):
                win, _calls = self._bound(platform)
                self.assertFalse([s for s in win.bindings if "Command" in s])

    def test_each_key_runs_its_own_callback(self):
        win, calls = self._bound("darwin")
        for seq, want in [("<Control-f>", "open"), ("<Command-f>", "open"),
                          ("<F3>", "next"), ("<Command-g>", "next"),
                          ("<Shift-F3>", "prev"), ("<Command-Shift-G>", "prev")]:
            with self.subTest(seq=seq):
                calls.log.clear()
                win.press(seq)
                self.assertEqual(calls.log, [want])

    def test_the_key_is_swallowed_so_the_widget_below_ignores_it(self):
        # Without "break", Ctrl-F also runs the Text widget's own class
        # binding — on macOS that moves the insertion cursor.
        win, _calls = self._bound("darwin")
        for seq in win.bindings:
            with self.subTest(seq=seq):
                self.assertEqual(win.press(seq), "break")


class _ScrollWidget:
    def __init__(self, cls="Button"):
        self.cls = cls
        self.bindings = {}
        self.scrolls = []

    def winfo_class(self):
        return self.cls

    def bind(self, sequence, callback, add=None):
        self.bindings[sequence] = callback

    def yview_scroll(self, direction, units):
        self.scrolls.append((direction, units))


class _ScrollWindow(_ScrollWidget):
    pass


class ReaderScrollKeyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = _load(
            "_reader_scroll_key_owns",
            "_bind_reader_scroll_keys",
            "_bind_text_scroll_keys",
        )
        cls.owns = staticmethod(ns["_reader_scroll_key_owns"])
        cls.bind_reader = staticmethod(ns["_bind_reader_scroll_keys"])
        cls.bind_text = staticmethod(ns["_bind_text_scroll_keys"])

    @staticmethod
    def _event(widget):
        return type("Event", (), {"widget": widget})()

    def test_up_and_down_scroll_the_reader_and_are_consumed(self):
        win = _ScrollWindow()
        calls = []
        self.bind_reader(win, lambda direction: calls.append(direction))

        self.assertEqual(
            win.bindings["<KeyPress-Up>"](self._event(_ScrollWidget())),
            "break",
        )
        self.assertEqual(
            win.bindings["<KeyPress-Down>"](self._event(_ScrollWidget())),
            "break",
        )
        self.assertEqual(calls, [-1, 1])

    def test_arrow_keys_remain_native_in_inputs_and_selectors(self):
        for cls_name in (
            "Entry", "TEntry", "Spinbox", "TCombobox", "Listbox",
            "Treeview", "TScale", "TScrollbar",
        ):
            with self.subTest(widget_class=cls_name):
                self.assertFalse(self.owns(_ScrollWidget(cls_name)))

        win = _ScrollWindow()
        calls = []
        self.bind_reader(win, lambda direction: calls.append(direction))
        result = win.bindings["<KeyPress-Down>"](
            self._event(_ScrollWidget("TEntry")),
        )
        self.assertIsNone(result)
        self.assertEqual(calls, [])

    def test_a_secondary_text_panel_scrolls_itself(self):
        win = _ScrollWindow()
        calls = []
        panel = _ScrollWidget("Text")
        self.bind_reader(win, lambda direction: calls.append(direction))

        result = win.bindings["<KeyPress-Down>"](self._event(panel))

        self.assertEqual(result, "break")
        self.assertEqual(panel.scrolls, [(1, "units")])
        self.assertEqual(calls, [])

    def test_text_widget_and_toolbar_focus_both_scroll_the_document(self):
        win = _ScrollWindow()
        text = _ScrollWidget("Text")
        self.bind_text(win, text)

        direct = text.bindings["<KeyPress-Up>"](self._event(text))
        routed = win.bindings["<KeyPress-Down>"](
            self._event(_ScrollWidget("TButton")),
        )

        self.assertEqual((direct, routed), ("break", "break"))
        self.assertEqual(text.scrolls, [(-1, "units"), (1, "units")])

    def test_pdf_keyboard_step_uses_the_existing_wheel_distance(self):
        ns = _load("scroll_key", cls="_PdfPane")
        Pane = type("Pane", (), {"scroll_key": ns["scroll_key"]})
        pane = Pane()
        pane._disposed = False
        pane.steps = []
        pane._wheel = pane.steps.append

        self.assertTrue(pane.scroll_key(-20))
        self.assertTrue(pane.scroll_key(4))
        self.assertEqual(pane.steps, [-1, 1])
        pane._disposed = True
        self.assertFalse(pane.scroll_key(1))
        self.assertEqual(pane.steps, [-1, 1])


def _pdf_pane_constants(*names) -> dict:
    """The real ``_PdfPane`` class constants, so the page arithmetic below is
    tested against the values the pane lays its pages out with."""
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    body = next(n.body for n in ast.parse(src).body
                if isinstance(n, ast.ClassDef) and n.name == "_PdfPane")
    out = {}
    for node in body:
        for target in getattr(node, "targets", ()):
            if isinstance(target, ast.Name) and target.id in names:
                out[target.id] = ast.literal_eval(node.value)
    missing = [n for n in names if n not in out]
    if missing:
        raise AssertionError(f"_PdfPane has no {missing}")
    return out


class _TurnPane:
    """A pane laid out the way ``_layout`` lays one out: a slot per page,
    stacked from the top with the pane's own padding between them."""

    consts = _pdf_pane_constants("_PAD", "_PAGE_TOP_SLOP")
    _PAD = consts["_PAD"]
    _PAGE_TOP_SLOP = consts["_PAGE_TOP_SLOP"]
    turn_page = _load("turn_page", cls="_PdfPane")["turn_page"]

    def __init__(self, pages=3, height=1000):
        self._disposed = False
        self._slots = []
        y = self._PAD
        for _ in range(pages):
            self._slots.append((y, height, (0.0, 0.0, 1.0, 1.0), 1.0))
            y += height + self._PAD
        self._view_top = 0.0
        self.went: list = []
        self._canvas = type(
            "Canvas", (), {"canvasy": lambda _s, _n, pane=self: pane._view_top},
        )()

    def page_top(self, i: int) -> float:
        """Where scroll_to_page would put page *i*."""
        return self._slots[i][0] - self._PAD

    def at(self, i: int, past: float = 0.0) -> "_TurnPane":
        """Put the view at page *i*, *past* pixels down it."""
        self._view_top = self.page_top(i) + past
        return self

    def scroll_to_page(self, i, y_pt=None):
        self.went.append(i)
        self._view_top = self.page_top(i)


class PdfPageKeyTests(unittest.TestCase):
    """Left and Right turn the pages of a PDF (``_PdfPane.turn_page``)."""

    def test_forward_goes_to_the_next_page(self):
        pane = _TurnPane().at(0)
        self.assertTrue(pane.turn_page(1))
        self.assertEqual(pane.went, [1])

    def test_back_goes_to_the_page_before(self):
        pane = _TurnPane().at(2)
        self.assertTrue(pane.turn_page(-1))
        self.assertEqual(pane.went, [1])

    def test_a_page_read_part_way_down_goes_back_to_its_own_top_first(self):
        # Otherwise Back from the middle of page 2 skips the top of it, which
        # the reader has not seen yet.
        pane = _TurnPane().at(1, past=400)
        pane.turn_page(-1)
        pane.turn_page(-1)
        self.assertEqual(pane.went, [1, 0])

    def test_and_forward_from_there_still_goes_to_the_next_one(self):
        pane = _TurnPane().at(1, past=400)
        pane.turn_page(1)
        self.assertEqual(pane.went, [2])

    def test_a_view_just_short_of_a_page_does_not_skip_over_it(self):
        pane = _TurnPane()
        pane._view_top = pane.page_top(1) - 4   # the last of page 1 in view
        pane.turn_page(1)
        self.assertEqual(pane.went, [1])

    def test_the_ends_of_the_document_stay_where_they_are(self):
        first = _TurnPane().at(0)
        first.turn_page(-1)
        self.assertEqual(first.went, [0])
        last = _TurnPane().at(2)
        last.turn_page(1)
        self.assertEqual(last.went, [2])

    def test_a_rounded_scroll_still_counts_as_being_at_the_page_top(self):
        # yview_moveto takes a fraction, so landing a pixel off is normal.
        pane = _TurnPane().at(1, past=_TurnPane._PAGE_TOP_SLOP - 1)
        pane.turn_page(-1)
        self.assertEqual(pane.went, [0])

    def test_a_pane_with_no_pages_leaves_the_key_alone(self):
        empty = _TurnPane(pages=0)
        self.assertFalse(empty.turn_page(1))
        gone = _TurnPane()
        gone._disposed = True
        self.assertFalse(gone.turn_page(1))
        self.assertEqual(gone.went, [])


class ReaderPageKeyTests(unittest.TestCase):
    """Which window the Left/Right keys reach, and which they leave alone."""

    @classmethod
    def setUpClass(cls):
        ns = _load("_reader_scroll_key_owns", "_bind_reader_page_keys")
        cls.bind_pages = staticmethod(ns["_bind_reader_page_keys"])

    @staticmethod
    def _event(widget):
        return type("Event", (), {"widget": widget})()

    def _bound(self, answer=True):
        win, calls = _ScrollWindow(), []
        self.bind_pages(win, lambda direction: calls.append(direction) or answer)
        return win, calls

    def test_left_and_right_turn_the_page_and_are_consumed(self):
        win, calls = self._bound()
        for seq in ("<KeyPress-Left>", "<KeyPress-Right>"):
            with self.subTest(seq=seq):
                self.assertEqual(
                    win.bindings[seq](self._event(_ScrollWidget())), "break")
        self.assertEqual(calls, [-1, 1])

    def test_a_window_with_no_pages_showing_leaves_them_alone(self):
        # The opinion text, or a viewer whose scan has not arrived.
        win, calls = self._bound(answer=False)
        self.assertIsNone(
            win.bindings["<KeyPress-Right>"](self._event(_ScrollWidget())))
        self.assertEqual(calls, [1])

    def test_the_caret_keeps_them_in_a_text_widget(self):
        win, calls = self._bound()
        self.assertIsNone(
            win.bindings["<KeyPress-Left>"](self._event(_ScrollWidget("Text"))))
        self.assertEqual(calls, [])

    def test_and_so_do_inputs_and_selectors(self):
        win, calls = self._bound()
        for cls_name in ("Entry", "TEntry", "Spinbox", "TCombobox", "Listbox",
                         "Treeview", "TScale", "TScrollbar"):
            with self.subTest(widget_class=cls_name):
                self.assertIsNone(win.bindings["<KeyPress-Left>"](
                    self._event(_ScrollWidget(cls_name))))
        self.assertEqual(calls, [])

    def test_the_case_reader_turns_pages_only_while_its_scan_is_showing(self):
        ns = _load("_turn_pdf_page", cls="_ScholarTextWindow")
        Reader = type("Reader", (), {"_turn_pdf_page": ns["_turn_pdf_page"]})
        turned: list = []
        reader = Reader()
        reader._pdf_pane = type("Pane", (), {
            "turn_page": lambda _self, d: turned.append(d) or True})()

        reader._mode = "pdf"
        self.assertTrue(reader._turn_pdf_page(+1))
        reader._mode = "text"
        self.assertFalse(reader._turn_pdf_page(+1))
        self.assertEqual(turned, [+1])

    def test_the_page_canvas_binds_them_and_panning_moves_to_shift(self):
        src = _source_of("_PdfPane", "__init__")
        self.assertIn('canvas.bind("<KeyPress-Left>",', src)
        self.assertIn("self.turn_page(-1)", src)
        self.assertIn("self.turn_page(1)", src)
        self.assertIn('canvas.bind("<Shift-KeyPress-Left>", '
                      "lambda _e: self._hwheel(-1))", src)
        self.assertIn('canvas.bind("<Shift-KeyPress-Right>", '
                      "lambda _e: self._hwheel(1))", src)

    def test_every_window_that_shows_pages_routes_them(self):
        # The point of the window-level binding is that it is on every view
        # that shows a PDF; a new one must not quietly go without.
        src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
        windows = [
            (node.name, ast.get_source_segment(src, node) or "")
            for node in ast.parse(src).body
            if isinstance(node, ast.ClassDef)
            and any(isinstance(n, ast.Call)
                    and getattr(n.func, "id", "") == "_PdfPane"
                    for n in ast.walk(node))
        ]
        self.assertTrue(windows)
        for name, body in windows:
            with self.subTest(window=name):
                self.assertIn("_bind_reader_page_keys(", body)


class _FakePane:
    def __init__(self, exists=True, has_find=True, showing=False):
        self._exists, self._has_find = exists, has_find
        self.opened = 0
        self.closed = 0
        self.showing = showing
        self.steps = []

    def winfo_exists(self):
        return self._exists

    def has_find(self):
        return self._has_find

    def find_is_open(self):
        return self.showing

    def _toggle_find(self):
        # The real one, in miniature: the key that opened the bar closes it.
        if self.showing:
            self.closed += 1
        else:
            self.opened += 1
        self.showing = not self.showing

    def _find_step(self, delta):
        self.steps.append(delta)


class _FakeFinder:
    def __init__(self, showing=False):
        self.opened = 0
        self.closed = 0
        self.showing = showing
        self.steps = []

    def is_open(self):
        return self.showing

    def toggle(self):
        if self.showing:
            self.close()
        else:
            self.open()

    def open(self):
        self.opened += 1
        self.showing = True

    def close(self):
        self.closed += 1
        self.showing = False

    def step(self, delta):
        self.steps.append(delta)


class ReaderRoutingTests(unittest.TestCase):
    """The reader shows either its text view or a PDF pane; find follows."""

    @classmethod
    def setUpClass(cls):
        ns = _load("_find_pane", "_find_open", "_find_step",
                   cls="_ScholarTextWindow")
        cls.Reader = type("Reader", (), {k: ns[k] for k in
                                         ("_find_pane", "_find_open",
                                          "_find_step")})

    def _reader(self, mode, pane):
        r = self.Reader()
        r._mode, r._pdf_pane = mode, pane
        r._finder = _FakeFinder()
        return r

    def test_pdf_view_searches_the_pdf(self):
        pane = _FakePane()
        r = self._reader("pdf", pane)
        r._find_open()
        self.assertEqual((pane.opened, r._finder.opened), (1, 0))

    def test_text_view_searches_the_text(self):
        pane = _FakePane()
        r = self._reader("scholar", pane)
        r._find_open()
        self.assertEqual((pane.opened, r._finder.opened), (0, 1))

    def test_a_scan_with_no_text_layer_falls_back_to_the_text_view(self):
        pane = _FakePane(has_find=False)
        r = self._reader("pdf", pane)
        r._find_open()
        self.assertEqual((pane.opened, r._finder.opened), (0, 1))

    def test_a_destroyed_pane_falls_back_to_the_text_view(self):
        pane = _FakePane(exists=False)
        r = self._reader("pdf", pane)
        r._find_open()
        self.assertEqual((pane.opened, r._finder.opened), (0, 1))

    def test_no_pane_at_all_falls_back_to_the_text_view(self):
        r = self._reader("pdf", None)
        r._find_open()
        self.assertEqual(r._finder.opened, 1)

    def test_opening_the_pdf_finder_closes_the_text_one(self):
        # Its bar anchors to a frame that is not on screen in PDF view.
        r = self._reader("pdf", _FakePane())
        r._find_open()
        self.assertEqual(r._finder.closed, 1)

    def test_the_find_key_pressed_again_puts_the_bar_away(self):
        pane = _FakePane()
        r = self._reader("pdf", pane)
        r._find_open()
        r._find_open()
        self.assertEqual((pane.opened, pane.closed), (1, 1))

    def test_and_the_same_on_the_text_view(self):
        r = self._reader("scholar", _FakePane())
        r._find_open()
        r._find_open()
        self.assertEqual((r._finder.opened, r._finder.closed), (1, 1))

    def test_find_again_follows_the_same_view(self):
        pane = _FakePane()
        r = self._reader("pdf", pane)
        r._find_step(+1)
        r._find_step(-1)
        self.assertEqual((pane.steps, r._finder.steps), ([1, -1], []))

        pane = _FakePane()
        r = self._reader("courtlistener", pane)
        r._find_step(+1)
        self.assertEqual((pane.steps, r._finder.steps), ([], [1]))


class FindKeyTogglesTests(unittest.TestCase):
    """The key that opens the find bar is the key that puts it away."""

    def test_the_text_finder_binds_its_toggle_not_its_open(self):
        src = _source_of("_TextFinder", "__init__")
        self.assertIn("_bind_find_keys(win, self.toggle,", src)

    def test_and_the_toggle_closes_a_bar_already_showing(self):
        ns = _load("toggle", "is_open", cls="_TextFinder")
        finder = type("F", (), dict(ns))()
        finder._visible = True
        finder.calls = []
        finder.close = lambda: finder.calls.append("close")
        finder.open = lambda: finder.calls.append("open")
        finder.toggle()
        self.assertEqual(finder.calls, ["close"])
        finder._visible = False
        finder.toggle()
        self.assertEqual(finder.calls, ["close", "open"])

    def test_the_pdf_pane_binds_its_toggle_too(self):
        src = _source_of("_PdfPane", "enable_find")
        self.assertIn("_bind_find_keys(self.winfo_toplevel(), "
                      "self._toggle_find,", src)

    def test_and_that_toggle_closes_an_open_bar(self):
        ns = _load("_toggle_find", cls="_PdfPane")
        pane = type("P", (), dict(ns))()
        pane.calls = []
        pane.find_is_open = lambda: True
        pane._close_find = lambda: pane.calls.append("close")
        pane._open_find = lambda: pane.calls.append("open")
        pane._toggle_find()
        pane.find_is_open = lambda: False
        pane._toggle_find()
        self.assertEqual(pane.calls, ["close", "open"])

    def test_the_floating_viewer_routes_the_key_to_the_toggle(self):
        src = _source_of("_FloatingPdfWindow", "_find_open")
        self.assertIn("pane._toggle_find()", src)

    def test_and_so_does_the_reader(self):
        src = _source_of("_ScholarTextWindow", "_find_open")
        self.assertIn("pane._toggle_find()", src)
        self.assertIn("self._finder.toggle()", src)


def _source_of(cls: str, name: str) -> str:
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    body = next(n.body for n in tree.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    for node in body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node)
    raise AssertionError(f"{cls} has no {name}")


if __name__ == "__main__":
    unittest.main()
