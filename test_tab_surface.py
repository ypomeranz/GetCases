"""A new tab surfaces the window it lands in.

When a case, statute, or other view is opened from Spotlight into the shared
tab window, that window may be behind other windows or minimized — the tab
would appear unseen.  ``_CaseTabsWindow.add_page`` now calls ``surface`` so the
window comes to the front (and un-minimizes); when it is already the front
window this is a no-op.

Both methods are lifted out of ``courtlistener_gui`` with ``ast`` (it imports
tkinter, absent on a headless run) and driven against stubs.
"""

import ast
import pathlib
import sys
import typing
import unittest
from unittest import mock


def _load(cls, names):
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    body = next(n.body for n in tree.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    found = {n.name: ast.get_source_segment(src, n) for n in body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found on {cls}: {missing}")
    ns = {"sys": sys, "tk": type("tk", (), {"TclError": Exception}),
          "Optional": typing.Optional, "_CaseTabPage": object}
    for name in names:
        exec(found[name], ns)
    return ns


NS = _load("_CaseTabsWindow", ["add_page", "surface"])


class _FakeWin:
    def __init__(self):
        self.calls = []

    def deiconify(self):
        self.calls.append("deiconify")

    def lift(self):
        self.calls.append("lift")

    def focus_force(self):
        self.calls.append("focus_force")


class _FakeNotebook:
    def __init__(self):
        self.selected = None
        self.inserted = []

    def index(self, _which):
        return 0

    def insert(self, at, page, **kw):
        self.inserted.append((at, page))

    def add(self, page, **kw):
        self.inserted.append(("end", page))

    def select(self, page):
        self.selected = page


class _Manager:
    """Just the attributes add_page / surface touch."""

    def __init__(self, win):
        self.win = win
        self.app = mock.Mock()
        self.notebook = _FakeNotebook()
        self._pages = []
        self._tab_close_image = None
        self.surfaced = 0
        self.refreshed = []
        self.add_page = NS["add_page"].__get__(self)
        self.surface = NS["surface"].__get__(self)

    def active_page(self):
        return None

    def refresh_page(self, page):
        self.refreshed.append(page)


class SurfaceTests(unittest.TestCase):
    def test_surface_deiconifies_lifts_and_focuses(self):
        win = _FakeWin()
        mgr = _Manager(win)
        with mock.patch.object(sys, "platform", "darwin"):
            mgr.surface()
        self.assertEqual(win.calls, ["deiconify", "lift", "focus_force"])

    def test_surface_un_minimizes_before_raising(self):
        # deiconify must come first: a minimized window can't be lifted.
        win = _FakeWin()
        mgr = _Manager(win)
        with mock.patch.object(sys, "platform", "linux"):
            mgr.surface()
        self.assertLess(win.calls.index("deiconify"), win.calls.index("lift"))

    def test_windows_gets_the_force_foreground_call(self):
        win = _FakeWin()
        mgr = _Manager(win)
        with mock.patch.object(sys, "platform", "win32"):
            mgr.surface()
        mgr.app._win_force_foreground.assert_called_once_with(win)

    def test_other_platforms_do_not_force_foreground(self):
        win = _FakeWin()
        mgr = _Manager(win)
        with mock.patch.object(sys, "platform", "darwin"):
            mgr.surface()
        mgr.app._win_force_foreground.assert_not_called()

    def test_a_tcl_error_is_swallowed(self):
        win = _FakeWin()
        win.deiconify = mock.Mock(side_effect=Exception("gone"))
        mgr = _Manager(win)
        # tk.TclError is stubbed to Exception, so this exercises the guard.
        mgr.surface()  # must not raise


class AddPageSurfacesTests(unittest.TestCase):
    def test_adding_a_tab_surfaces_the_window(self):
        win = _FakeWin()
        mgr = _Manager(win)
        page = object()
        with mock.patch.object(sys, "platform", "darwin"):
            mgr.add_page(page)
        self.assertIn(page, mgr._pages)
        self.assertEqual(mgr.notebook.selected, page)
        # The window was brought forward as part of adding the page.
        self.assertEqual(win.calls, ["deiconify", "lift", "focus_force"])


if __name__ == "__main__":
    unittest.main()
