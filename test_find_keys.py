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


class _FakePane:
    def __init__(self, exists=True, has_find=True):
        self._exists, self._has_find = exists, has_find
        self.opened = 0
        self.steps = []

    def winfo_exists(self):
        return self._exists

    def has_find(self):
        return self._has_find

    def _open_find(self):
        self.opened += 1

    def _find_step(self, delta):
        self.steps.append(delta)


class _FakeFinder:
    def __init__(self):
        self.opened = 0
        self.closed = 0
        self.steps = []

    def open(self):
        self.opened += 1

    def close(self):
        self.closed += 1

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


if __name__ == "__main__":
    unittest.main()
