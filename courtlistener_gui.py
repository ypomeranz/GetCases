"""
CourtListener GUI
=================
A Tkinter interface for searching US case law via the CourtListener API
and downloading opinion PDFs.

Requires:
    pip install requests

Usage:
    python courtlistener_gui.py

Token lookup order:
  1. COURTLISTENER_TOKEN environment variable
  2. ~/.config/courtlistener/config.json  (saved automatically after first use)
"""

from __future__ import annotations

import difflib
import gc
import html as _html
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
import urllib.parse
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace as _dc_replace
from functools import lru_cache

from pathlib import Path
from tkinter import filedialog, font as tkfont, messagebox, ttk
from typing import Optional


def _ensure_dependencies() -> None:
    """
    Check for the third-party packages this GUI needs and offer to
    pip-install any that are missing before the imports below run.

    ``requests`` is required; the rest enable features and declining just
    disables them: ``beautifulsoup4`` (Google Scholar / opinion parsing),
    ``pynput`` (global hotkey), ``pypdfium2`` + ``Pillow`` (the in-app PDF
    viewer), ``customtkinter`` (the modern spotlight and pop-up window styling;
    without it those windows fall back to plain Tk), and ``curl_cffi`` +
    ``browser_cookie3`` (fetching the CommonLII English Reports scans in-app
    through CloudFlare; without them E.R. cases open in the browser instead).
    """
    import importlib
    import importlib.util

    def missing_packages() -> list[str]:
        return [
            pip_name
            for module, pip_name in (
                ("requests", "requests"),
                ("bs4", "beautifulsoup4"),
                ("pynput", "pynput"),
                ("pypdfium2", "pypdfium2"),  # in-app PDF viewer
                ("PIL", "Pillow"),           # in-app PDF viewer (imports as PIL)
                ("customtkinter", "customtkinter"),  # modern spotlight / window chrome
                ("curl_cffi", "curl_cffi"),  # English Reports scan fetch (CloudFlare)
                ("browser_cookie3", "browser_cookie3"),  # reads Firefox clearance cookie
                ("selenium","selenium"),
            )
            if importlib.util.find_spec(module) is None
        ]

    missing = missing_packages()
    if not missing:
        return
    root = tk.Tk()
    root.withdraw()
    try:
        if messagebox.askyesno(
            "Missing Packages",
            "This application needs the following Python package(s), "
            "which are not installed:\n\n    " + ", ".join(missing)
            + "\n\nInstall them now with pip?",
        ):
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", *missing],
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                messagebox.showerror(
                    "Install Failed",
                    "pip install failed:\n\n" + (proc.stderr or proc.stdout)[-800:],
                )
            else:
                importlib.invalidate_caches()
                messagebox.showinfo(
                    "Packages Installed", "Installed: " + ", ".join(missing)
                )
        if importlib.util.find_spec("requests") is None:
            messagebox.showerror(
                "Missing Dependency",
                "The 'requests' package is required to run this application.\n\n"
                "Install it with:\n    pip install requests",
            )
            sys.exit(1)
    finally:
        root.destroy()


if not os.environ.get("GETCASES_SKIP_DEPENDENCY_PROMPT"):
    _ensure_dependencies()

import requests as _requests

# ----------------------------------------------------------------------------
# Modern UI toolkit (CustomTkinter)
# ----------------------------------------------------------------------------
# CustomTkinter gives the spotlight and the pop-up windows rounded, themed
# chrome.  It is optional: when it is not installed the same windows are built
# with plain Tk widgets instead, so the app keeps working (just less polished).
# The main window is deliberately left on classic Tk/ttk for now.
if importlib.util.find_spec("customtkinter") is not None:
    import customtkinter as ctk  # type: ignore
else:  # pragma: no cover - exercised only where the package is absent
    ctk = None  # type: ignore
_CTK_AVAILABLE = ctk is not None

# One cohesive light palette shared by every modernised window, so the
# spotlight, dialogs, and viewers read as one product rather than a grab-bag of
# Tk defaults.
_UI = {
    "window":      "#ffffff",  # card / window background
    "surface":     "#f2f3f5",  # inset surfaces (search bar, toolbars)
    "surface_alt": "#e9ebef",  # hover on inset surfaces
    "border":      "#e2e4e9",  # hairline separators
    "text":        "#1c1d21",  # primary text
    "muted":       "#8b909a",  # secondary text / placeholders
    "accent":      "#2f6bd8",  # primary action / links
    "accent_dim":  "#255ac0",  # accent hover
    "selection":   "#e8f0fd",  # highlighted list row
    "badge":       "#3a5a8c",  # court badge (high courts)
    "badge_alt":   "#737d8c",  # court badge (other courts)
    "danger":      "#c0392b",
}

_MODERN_THEME_READY = False


def _ensure_modern_theme() -> None:
    """Initialise CustomTkinter once, in a light appearance that matches the
    document-style windows.  A no-op when the package is unavailable."""
    global _MODERN_THEME_READY
    if not _CTK_AVAILABLE or _MODERN_THEME_READY:
        return
    try:
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
    except Exception:
        pass
    _MODERN_THEME_READY = True


def _ui_font(size: int, weight: str = "normal"):
    """A CTkFont at *size* when CustomTkinter is present, else a Tk font tuple.
    Centralised so every modern window shares one type ramp."""
    if _CTK_AVAILABLE:
        try:
            return ctk.CTkFont(size=size, weight=weight)
        except Exception:
            pass
    return ("TkDefaultFont", size, "bold" if weight == "bold" else "normal")


def _bind_recursive(widget, sequence: str, handler) -> None:
    """Bind *handler* for *sequence* on *widget* and all of its descendants.

    CustomTkinter renders each widget from nested internal canvases/labels, so a
    single ``bind`` on a composite row misses clicks that land on those inner
    widgets.  Binding the whole subtree makes a click (or hover) anywhere on the
    row register regardless of which internal piece received the event."""
    try:
        widget.bind(sequence, handler, add="+")
    except tk.TclError:
        pass
    for child in widget.winfo_children():
        _bind_recursive(child, sequence, handler)


# ---- Normalised widget factories -------------------------------------------
# Each returns a CustomTkinter widget when the package is available and an
# equivalent Tk/ttk widget otherwise, so a window is written once and still
# works (just less styled) without CustomTkinter.  Geometry (pack/grid) is left
# to the caller.

def _ui_toplevel(parent: tk.Misc) -> tk.Toplevel:
    """A Toplevel with the modern light background.  Kept as a plain Toplevel
    (native title bar, reliable modality) and filled with themed widgets."""
    win = tk.Toplevel(parent)
    if _CTK_AVAILABLE:
        _ensure_modern_theme()
        try:
            win.configure(bg=_UI["window"])
        except tk.TclError:
            pass
    return win


def _work_area(widget: tk.Misc) -> tuple[int, int, int, int]:
    """Return the usable desktop rectangle: left, top, width, height."""
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            class RECT(ctypes.Structure):
                _fields_ = [
                    ("left", wintypes.LONG),
                    ("top", wintypes.LONG),
                    ("right", wintypes.LONG),
                    ("bottom", wintypes.LONG),
                ]

            class MONITORINFO(ctypes.Structure):
                _fields_ = [
                    ("cbSize", wintypes.DWORD),
                    ("rcMonitor", RECT),
                    ("rcWork", RECT),
                    ("dwFlags", wintypes.DWORD),
                ]

            user32 = ctypes.windll.user32
            HMONITOR = getattr(wintypes, "HMONITOR", wintypes.HANDLE)
            user32.MonitorFromWindow.argtypes = [wintypes.HWND, wintypes.DWORD]
            user32.MonitorFromWindow.restype = HMONITOR
            user32.GetMonitorInfoW.argtypes = [
                HMONITOR, ctypes.POINTER(MONITORINFO)
            ]
            user32.GetMonitorInfoW.restype = wintypes.BOOL
            hwnd = wintypes.HWND(widget.winfo_id())
            monitor = user32.MonitorFromWindow(hwnd, 2)  # nearest monitor
            info = MONITORINFO()
            info.cbSize = ctypes.sizeof(MONITORINFO)
            if monitor and user32.GetMonitorInfoW(monitor, ctypes.byref(info)):
                r = info.rcWork
                return r.left, r.top, r.right - r.left, r.bottom - r.top
        except Exception:
            pass
    try:
        return (
            widget.winfo_vrootx(),
            widget.winfo_vrooty(),
            widget.winfo_vrootwidth(),
            widget.winfo_vrootheight(),
        )
    except tk.TclError:
        return 0, 0, widget.winfo_screenwidth(), widget.winfo_screenheight()


def _fit_toplevel_geometry(
    win: tk.Misc,
    width: int,
    height: int,
    *,
    min_width: int,
    min_height: int,
    bottom_gap: int = 64,
) -> str:
    """Geometry string that keeps a new top-level inside the usable desktop."""
    left, top, work_w, work_h = _work_area(win)
    w = max(min_width, min(width, max(min_width, work_w - 32)))
    h = max(min_height, min(height, max(min_height, work_h - bottom_gap - 32)))
    x = left + max(16, (work_w - w) // 2)
    y = top + 20
    return f"{w}x{h}+{x}+{y}"


def _clamp_toplevel_to_work_area(
    win: tk.Misc,
    *,
    min_width: int,
    min_height: int,
    bottom_gap: int = 64,
) -> None:
    """Keep an existing top-level from extending into the taskbar/work-area edge."""
    try:
        win.update_idletasks()
        left, top, work_w, work_h = _work_area(win)
        max_w = max(min_width, work_w - 32)
        max_h = max(min_height, work_h - bottom_gap - 32)
        w = max(min_width, min(win.winfo_width() or min_width, max_w))
        h = max(min_height, min(win.winfo_height() or min_height, max_h))
        x = win.winfo_x()
        y = win.winfo_y()
        x = max(left + 16, min(x, left + work_w - w - 16))
        y = max(top + 16, min(y, top + work_h - bottom_gap - h))
        win.geometry(f"{w}x{h}+{x}+{y}")
        win.update_idletasks()
    except tk.TclError:
        pass


def _set_ui_button_width(button, width: int) -> None:
    """Apply a pixel-ish width hint across CTk and ttk buttons."""
    try:
        if _CTK_AVAILABLE:
            button.configure(width=width)
        else:
            button.configure(width=max(1, round(width / 9)))
    except tk.TclError:
        pass


def _ui_frame(parent, card: bool = False, fg: Optional[str] = None):
    """A container.  ``card=True`` gives a bordered, rounded surface."""
    if _CTK_AVAILABLE:
        if card:
            return ctk.CTkFrame(parent, corner_radius=12,
                                fg_color=fg or _UI["window"],
                                border_width=1, border_color=_UI["border"])
        return ctk.CTkFrame(parent, fg_color=fg or "transparent")
    return tk.Frame(parent, bg=fg or _UI["window"])


def _ui_label(parent, text: str = "", size: int = 13, weight: str = "normal",
              muted: bool = False, anchor: Optional[str] = None,
              textvariable=None):
    color = _UI["muted"] if muted else _UI["text"]
    if _CTK_AVAILABLE:
        lbl = ctk.CTkLabel(parent, text=text, font=_ui_font(size, weight),
                           text_color=color, fg_color="transparent",
                           textvariable=textvariable)
        if anchor:
            lbl.configure(anchor=anchor)
        return lbl
    return tk.Label(parent, text=text, bg=_UI["window"], fg=color,
                    font=("TkDefaultFont", size,
                          "bold" if weight == "bold" else "normal"),
                    anchor=anchor or "center", textvariable=textvariable)


def _ui_button(parent, text: str, command=None, primary: bool = False,
               width: Optional[int] = None):
    if _CTK_AVAILABLE:
        kw = dict(
            text=text, command=command, corner_radius=8, height=34,
            font=_ui_font(13, "bold" if primary else "normal"),
        )
        if primary:
            kw.update(fg_color=_UI["accent"], hover_color=_UI["accent_dim"],
                      text_color="#ffffff")
        else:
            kw.update(fg_color=_UI["surface"], hover_color=_UI["surface_alt"],
                      text_color=_UI["text"], border_width=1,
                      border_color=_UI["border"])
        if width:
            kw["width"] = width
        return ctk.CTkButton(parent, **kw)
    btn = ttk.Button(parent, text=text, command=command)
    if width:
        _set_ui_button_width(btn, width)
    return btn


def _style_ui_button(button, primary: bool = False) -> None:
    """Restyle an existing shared button as primary or secondary."""
    try:
        if _CTK_AVAILABLE:
            if primary:
                button.configure(
                    fg_color=_UI["accent"], hover_color=_UI["accent_dim"],
                    text_color="#ffffff", border_width=0,
                    font=_ui_font(13, "bold"),
                )
            else:
                button.configure(
                    fg_color=_UI["surface"], hover_color=_UI["surface_alt"],
                    text_color=_UI["text"], border_width=1,
                    border_color=_UI["border"], font=_ui_font(13, "normal"),
                )
    except Exception:
        pass


def _history_button(app, parent_frame):
    """The "History ▾" button case windows share: drops the app-wide list of
    the last 15 viewed cases (see ``CourtListenerGUI.record_case_view``).
    ``app`` may be None (a window opened without an app handle) — no button."""
    if app is None or not hasattr(app, "post_history_menu"):
        return None
    btn = _ui_button(parent_frame, "History ▾", width=100)
    btn.configure(command=lambda: app.post_history_menu(btn))
    return btn


def _install_history_menubar(app, win: tk.Misc):
    """Attach the shared History and Window menus to case views."""
    if app is None or not hasattr(app, "populate_history_menu"):
        return None
    menubar = tk.Menu(win)
    history_menu = tk.Menu(menubar, tearoff=0)
    try:
        history_menu.configure(
            postcommand=lambda m=history_menu: app.populate_history_menu(m)
        )
    except tk.TclError:
        pass
    app.populate_history_menu(history_menu)
    menubar.add_cascade(label="History", menu=history_menu)
    if hasattr(app, "populate_window_menu"):
        window_menu = tk.Menu(menubar, tearoff=0)
        try:
            window_menu.configure(
                postcommand=lambda m=window_menu, w=win:
                    app.populate_window_menu(m, w)
            )
        except tk.TclError:
            pass
        app.populate_window_menu(window_menu, win)
        menubar.add_cascade(label="Window", menu=window_menu)
    try:
        win.config(menu=menubar)
    except tk.TclError:
        return None
    return menubar


def _install_window_menubar(app, win: tk.Misc):
    """Attach the shared Window menu to a non-case document view."""
    if app is None or not hasattr(app, "populate_window_menu"):
        return None
    menubar = tk.Menu(win)
    window_menu = tk.Menu(menubar, tearoff=0)
    try:
        window_menu.configure(
            postcommand=lambda m=window_menu, w=win:
                app.populate_window_menu(m, w)
        )
    except tk.TclError:
        pass
    app.populate_window_menu(window_menu, win)
    menubar.add_cascade(label="Window", menu=window_menu)
    try:
        win.config(menu=menubar)
    except tk.TclError:
        return None
    return menubar


class _CaseTabPage(ttk.Frame):
    """A notebook page that supplies the small Toplevel API case viewers use.

    Opinion viewers build the same widget tree regardless of whether their
    host is a real OS window or one page in the app-wide tabbed case window.
    Geometry, title, menu, and window-level key bindings are delegated to the
    notebook's Toplevel; ordinary widget operations remain local to the page.
    """

    def __init__(self, manager: "_CaseTabsWindow") -> None:
        super().__init__(manager.notebook)
        self._manager = manager
        self._case_title = "Opinion"
        self._case_menu = None
        manager.add_page(self)

    def title(self, value=None):
        if value is None:
            return self._case_title
        self._case_title = str(value)
        self._manager.refresh_page(self)
        return None

    def geometry(self, spec=None):
        if spec is None:
            return self._manager.win.geometry()
        self._manager.apply_geometry(str(spec))
        return None

    def minsize(self, width=None, height=None):
        if width is None or height is None:
            return self._manager.win.minsize()
        self._manager.win.minsize(width, height)
        return None

    def config(self, cnf=None, **kw):
        opts = {}
        if isinstance(cnf, dict):
            opts.update(cnf)
        elif cnf is not None:
            return super().config(cnf, **kw)
        opts.update(kw)
        if "menu" in opts:
            self._case_menu = opts.pop("menu")
            self._manager.refresh_page(self)
        if opts:
            return super().config(**opts)
        return None

    configure = config

    def bind(self, sequence=None, func=None, add=None):
        # Toplevel bindings are present in every descendant's bindtags.  A
        # Frame is not, so route keyboard shortcuts through the shared window
        # and dispatch them only while this page is selected.
        if sequence and (
            str(sequence).startswith("<Control-")
            or str(sequence).startswith("<Command-")
            or str(sequence) in ("<F3>", "<Shift-F3>", "<Escape>")
        ):
            def active_only(event, page=self, callback=func):
                if page._manager.active_page() is page and callback is not None:
                    return callback(event)
                return None
            return self._manager.win.bind(sequence, active_only, add="+")
        return super().bind(sequence, func, add)

    def protocol(self, name=None, func=None):
        if name == "WM_DELETE_WINDOW" and func is not None:
            # The shared OS window owns its own close-all protocol.  Individual
            # pages keep this only as metadata; their cleanup belongs on a
            # <Destroy> binding so Close Tab and Pop Out run it too.
            self._wm_delete_callback = func
            return None
        return self._manager.win.protocol(name, func)

    def transient(self, master=None):
        return self._manager.win.transient(master)

    def winfo_x(self):
        return self._manager.win.winfo_x()

    def winfo_y(self):
        return self._manager.win.winfo_y()

    def destroy(self) -> None:
        manager = getattr(self, "_manager", None)
        if manager is not None:
            manager.remove_page(self)
        try:
            super().destroy()
        except tk.TclError:
            pass


class _CaseTabsWindow:
    """One OS window containing all opinion views as notebook pages."""

    def __init__(self, app, parent: tk.Misc) -> None:
        self.app = app
        self.win = _ui_toplevel(parent)
        self.win.title("Cases")
        self.win.geometry(
            _fit_toplevel_geometry(
                self.win, 980, 780, min_width=560, min_height=360
            )
        )
        self.win.minsize(560, 360)
        self.notebook = ttk.Notebook(self.win)
        self.notebook.pack(fill="both", expand=True)
        self._pages: list[_CaseTabPage] = []
        self._geometry_set = False
        self._closing = False
        self._long_press_after = None
        self._long_press_xy = (0, 0)
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)
        self.notebook.bind("<Button-3>", self._show_tab_context_menu)
        self.notebook.bind("<Button-2>", self._show_tab_context_menu)
        self.notebook.bind("<Control-Button-1>", self._show_tab_context_menu)
        self.notebook.bind("<ButtonPress-1>", self._start_tab_long_press, add="+")
        self.notebook.bind(
            "<ButtonRelease-1>", self._cancel_tab_long_press, add="+"
        )
        self.notebook.bind("<B1-Motion>", self._track_tab_long_press, add="+")
        self.win.bind("<Control-Tab>", lambda _e: self._cycle_tab(1))
        self.win.bind(
            "<Control-Shift-Tab>", lambda _e: self._cycle_tab(-1)
        )
        try:
            self.win.bind(
                "<Control-ISO_Left_Tab>", lambda _e: self._cycle_tab(-1)
            )
        except tk.TclError:
            pass  # Windows Tk has no ISO_Left_Tab keysym.
        self.win.protocol("WM_DELETE_WINDOW", self.close)
        self.win.bind("<Destroy>", self._on_destroy, add="+")

    def add_page(self, page: _CaseTabPage) -> None:
        self._pages.append(page)
        self.notebook.add(page, text="Opinion")
        self.notebook.select(page)
        self.refresh_page(page)

    def remove_page(self, page: _CaseTabPage) -> None:
        if page not in self._pages:
            return
        self._pages.remove(page)
        try:
            self.notebook.forget(page)
        except tk.TclError:
            pass
        if not self._pages and not self._closing:
            self.close()
        else:
            self._sync_active()

    def active_page(self) -> "Optional[_CaseTabPage]":
        try:
            selected = self.notebook.select()
        except tk.TclError:
            return None
        for page in self._pages:
            if str(page) == selected:
                return page
        return None

    def _page_at(self, x: int, y: int) -> "Optional[_CaseTabPage]":
        try:
            index = self.notebook.index(f"@{x},{y}")
            tab_id = self.notebook.tabs()[index]
        except (tk.TclError, IndexError):
            return None
        return next((page for page in self._pages if str(page) == tab_id), None)

    def _cycle_tab(self, delta: int) -> str:
        tabs = self.notebook.tabs()
        if len(tabs) < 2:
            return "break"
        try:
            current = self.notebook.index(self.notebook.select())
            self.notebook.select(tabs[(current + delta) % len(tabs)])
        except tk.TclError:
            pass
        return "break"

    def _show_tab_context_menu(self, event=None, page=None):
        self._cancel_tab_long_press()
        if page is None and event is not None:
            page = self._page_at(event.x, event.y)
        if page is None:
            return None
        try:
            self.notebook.select(page)
        except tk.TclError:
            return None
        menu = tk.Menu(self.win, tearoff=0)
        menu.add_command(label="Close Tab", command=page.destroy)
        can_pop = bool(
            hasattr(self.app, "can_pop_out_view")
            and self.app.can_pop_out_view(page)
        )
        menu.add_command(
            label="Pop Out into New Window",
            state="normal" if can_pop else "disabled",
            command=lambda p=page: self.app.pop_out_view(p),
        )
        try:
            if event is not None:
                x_root, y_root = event.x_root, event.y_root
            else:
                x_root = page.winfo_rootx() + 24
                y_root = self.notebook.winfo_rooty() + 24
            menu.tk_popup(x_root, y_root)
        finally:
            try:
                menu.grab_release()
            except tk.TclError:
                pass
        return "break"

    def _start_tab_long_press(self, event) -> None:
        self._cancel_tab_long_press()
        page = self._page_at(event.x, event.y)
        if page is None:
            return
        self._long_press_xy = (event.x_root, event.y_root)

        def show(page=page) -> None:
            self._long_press_after = None
            event_like = type("_TabMenuEvent", (), {
                "x_root": self._long_press_xy[0],
                "y_root": self._long_press_xy[1],
            })()
            self._show_tab_context_menu(event_like, page)

        self._long_press_after = self.notebook.after(650, show)

    def _track_tab_long_press(self, event) -> None:
        x0, y0 = self._long_press_xy
        if abs(event.x_root - x0) > 8 or abs(event.y_root - y0) > 8:
            self._cancel_tab_long_press()

    def _cancel_tab_long_press(self, _event=None) -> None:
        if self._long_press_after is None:
            return
        try:
            self.notebook.after_cancel(self._long_press_after)
        except tk.TclError:
            pass
        self._long_press_after = None

    @staticmethod
    def _tab_label(title: str) -> str:
        label = re.sub(r"\s+", " ", title or "").strip() or "Opinion"
        return label if len(label) <= 52 else label[:49] + "..."

    def refresh_page(self, page: _CaseTabPage) -> None:
        if page not in self._pages:
            return
        try:
            self.notebook.tab(page, text=self._tab_label(page._case_title))
        except tk.TclError:
            return
        if self.active_page() is page:
            self._sync_active()

    def apply_geometry(self, spec: str) -> None:
        # Each viewer chooses a good initial size.  Honor the first page's
        # request; later tabs should not resize the shared window underneath
        # the reader as they are opened in the background.
        if self._geometry_set:
            return
        self._geometry_set = True
        try:
            self.win.geometry(spec)
        except tk.TclError:
            pass

    def _on_tab_changed(self, _event=None) -> None:
        self._sync_active()

    def _sync_active(self) -> None:
        page = self.active_page()
        if page is None:
            return
        try:
            self.win.title(page._case_title)
            if page._case_menu is not None:
                self.win.config(menu=page._case_menu)
            else:
                self.win.config(menu="")
        except tk.TclError:
            pass

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        for page in list(self._pages):
            try:
                page.destroy()
            except tk.TclError:
                pass
        self._pages.clear()
        try:
            self.win.destroy()
        except tk.TclError:
            pass

    def _on_destroy(self, event) -> None:
        if event.widget is self.win and hasattr(
            self.app, "_case_tabs_window_destroyed"
        ):
            self.app._case_tabs_window_destroyed(self)


def _case_view_host(parent: tk.Misc, app):
    """Return a standalone Toplevel or a page in the shared case notebook."""
    if app is not None and hasattr(app, "new_case_view_host"):
        return app.new_case_view_host(parent)
    return _ui_toplevel(parent)


def _secondary_view_host(parent: tk.Misc, app):
    """Host any non-modal document/tool view in the shared notebook mode."""
    if app is not None and hasattr(app, "new_secondary_view_host"):
        return app.new_secondary_view_host(parent)
    return _ui_toplevel(parent)


def _ui_checkbox(parent, text: str, variable, command=None):
    if _CTK_AVAILABLE:
        return ctk.CTkCheckBox(parent, text=text, variable=variable,
                               command=command, font=_ui_font(12),
                               text_color=_UI["text"], fg_color=_UI["accent"],
                               hover_color=_UI["accent_dim"],
                               checkbox_width=20, checkbox_height=20,
                               corner_radius=5)
    return ttk.Checkbutton(parent, text=text, variable=variable,
                           command=command)


class _HoverTip:
    """A lightweight hover tooltip: a moment after the pointer settles on
    ``widget`` it pops up ``text_getter()`` in a small yellow window near the
    cursor, and hides it on leave.  ``text_getter`` is read at show time, so a
    live status message (whose text changes) always shows its current value.
    Nothing pops up when the text is empty."""

    def __init__(self, widget, text_getter, *, delay: int = 450,
                 wraplength: int = 520) -> None:
        self._widget = widget
        self._get = text_getter
        self._delay = delay
        self._wrap = wraplength
        self._after: Optional[str] = None
        self._tip: Optional[tk.Toplevel] = None
        widget.bind("<Enter>", self._schedule, add="+")
        widget.bind("<Leave>", self._leave, add="+")
        widget.bind("<Destroy>", self._leave, add="+")

    def _schedule(self, _e=None) -> None:
        self._cancel()
        try:
            self._after = self._widget.after(self._delay, self._show)
        except tk.TclError:
            self._after = None

    def _cancel(self) -> None:
        if self._after is not None:
            try:
                self._widget.after_cancel(self._after)
            except tk.TclError:
                pass
            self._after = None

    def _leave(self, _e=None) -> None:
        self._cancel()
        self._hide()

    def _show(self) -> None:
        self._after = None
        try:
            text = (self._get() or "").strip()
        except Exception:
            text = ""
        if not text:
            return
        self._hide()
        try:
            x, y = self._widget.winfo_pointerxy()
            tip = tk.Toplevel(self._widget)
            tip.wm_overrideredirect(True)
            try:
                tip.attributes("-topmost", True)
            except tk.TclError:
                pass
            tk.Label(
                tip, text=text, justify="left", wraplength=self._wrap,
                background="#fffbe6", foreground="#000000",
                relief="solid", borderwidth=1, padx=8, pady=5,
            ).pack()
            tip.wm_geometry(f"+{x + 14}+{y + 18}")
            self._tip = tip
        except tk.TclError:
            self._tip = None

    def _hide(self) -> None:
        if self._tip is not None:
            try:
                self._tip.destroy()
            except tk.TclError:
                pass
            self._tip = None


def _install_status_label(parent, textvariable, *, padx=(10, 0)):
    """The status message that sits among a case window's bottom-bar buttons.

    It is packed so the *buttons* always win the horizontal space: a tiny
    requested width means Tk lays the buttons out first, and ``fill``/``expand``
    only lets the message spread into whatever room is left over.  When the
    window is too narrow to show the whole message it is simply clipped — a
    hover then pops the full text up in a tooltip (per the whole-message
    mouse-over)."""
    lbl = _ui_label(parent, muted=True, anchor="w", textvariable=textvariable)
    # width=1 (1 char for the ttk label, 1 px for CTk) keeps the label's own
    # size request negligible so no button is ever pushed off the bar.
    try:
        lbl.configure(width=1)
    except tk.TclError:
        pass
    if _CTK_AVAILABLE and isinstance(lbl, ctk.CTkLabel):
        # A CTkLabel is a frame whose size request tracks the text label
        # gridded inside it, so the width=1 above caps nothing and a long
        # message still squeezes buttons off the bar (worst in the case
        # window, where the view/Print buttons are re-packed after this
        # label and so lose the space contest first).  Freezing propagation
        # makes the 1×28 request stick; the message then only spreads into
        # leftover space and clips at the bar's edge.
        lbl.grid_propagate(False)
    lbl.pack(side="left", fill="x", expand=True, padx=padx)
    _HoverTip(lbl, textvariable.get)
    return lbl


def _ui_entry(parent, textvariable=None, show: Optional[str] = None):
    if _CTK_AVAILABLE:
        kw = dict(textvariable=textvariable, corner_radius=8, height=34,
                  border_color=_UI["border"], fg_color=_UI["window"],
                  text_color=_UI["text"], font=_ui_font(13))
        if show is not None:
            kw["show"] = show
        return ctk.CTkEntry(parent, **kw)
    return ttk.Entry(parent, textvariable=textvariable,
                     **({"show": show} if show is not None else {}))


_MODERN_TTK_READY = False


def _ensure_modern_ttk_styles(widget: tk.Misc) -> None:
    """Define named ttk styles ("Modern.Treeview", "Modern.Vertical.TScrollbar")
    for the ttk widgets that stay ttk inside modernised windows (there is no
    CustomTkinter tree).  Named — not global — so the classic main window keeps
    its default ttk look."""
    global _MODERN_TTK_READY
    if not _CTK_AVAILABLE or _MODERN_TTK_READY:
        return
    try:
        style = ttk.Style(widget)
        style.configure(
            "Modern.Treeview", background=_UI["window"],
            fieldbackground=_UI["window"], foreground=_UI["text"],
            borderwidth=0, rowheight=28, font=("TkDefaultFont", 11),
        )
        style.map(
            "Modern.Treeview",
            background=[("selected", _UI["selection"])],
            foreground=[("selected", _UI["text"])],
        )
        style.configure(
            "Modern.Treeview.Heading", background=_UI["surface"],
            foreground=_UI["muted"], relief="flat",
            font=("TkDefaultFont", 10, "bold"), padding=(6, 4),
        )
        style.map("Modern.Treeview.Heading",
                  background=[("active", _UI["surface_alt"])])
        style.configure(
            "Modern.Vertical.TScrollbar", background=_UI["surface"],
            troughcolor=_UI["window"], bordercolor=_UI["window"],
            arrowcolor=_UI["muted"],
        )
        style.configure(
            "Modern.TCombobox", fieldbackground=_UI["surface"],
            background=_UI["surface"], bordercolor=_UI["border"],
            arrowcolor=_UI["muted"], foreground=_UI["text"], padding=4,
            relief="flat",
        )
        style.map("Modern.TCombobox",
                  fieldbackground=[("readonly", _UI["surface"])],
                  selectbackground=[("readonly", _UI["surface"])],
                  selectforeground=[("readonly", _UI["text"])])
        style.configure(
            "Modern.TEntry", fieldbackground=_UI["surface"],
            bordercolor=_UI["border"], foreground=_UI["text"], padding=5,
            relief="flat",
        )
        style.configure("Modern.TLabel", background=_UI["window"],
                        foreground=_UI["text"])
        style.configure("ModernMuted.TLabel", background=_UI["window"],
                        foreground=_UI["muted"])
    except tk.TclError:
        pass
    _MODERN_TTK_READY = True


from bluebook_names import (
    apply_caption_case_reference,
    abbreviate_case_name,
    caption_case_reference_tokens,
    collapse_personal_all_caps_run,
    courtlistener_case_name,
    cut_companion_cases,
    is_recognized_given_name,
    normal_case_caption,
    refine_caption_case,
    simplify_historical_entity_caption,
    strip_related_case_note,
)
from citation_overrides import (
    citation_identity_keys,
    clean_base_citation,
    find_override,
    format_edited_citation,
    update_overrides,
)
from cl_parse import parse_cl_html as _parse_cl_html
import courtlistener as cl_api
from courtlistener import CourtListenerClient, CourtListenerError
import constitution
import ecfr
import eng_rep
import eng_rep_pdf
import fed_cas
import fed_rules
import state_statutes
import statutes_at_large
import us_code
import pdfium_lock
import us_reports_pdf
import brief_reader
import oyez
import slip_opinion
from citations import (
    PINCITE_AFTER_RE as _PINCITE_AFTER_RE,
    TEXT_CITE_RE as _TEXT_CITE_RE,
    NOMINATIVE_PARALLEL_RE as _NOMINATIVE_PARALLEL_RE,
    US_NOMINATIVE_PARALLEL_RE as _US_NOMINATIVE_PARALLEL_RE,
    NOMINATIVE_CITE_RE as _NOMINATIVE_TEXT_CITE_RE,
    EARLY_FED_CITE_RE as _EARLY_FED_CITE_RE,
    early_fed_cite_text as _early_fed_cite_text,
    SHORT_CITE_RE as _SHORT_CITE_RE,
    ID_CITE_RE as _ID_CITE_RE,
    case_match_text as _case_match_text,
    norm_reporter as _norm_reporter,
    build_short_cite_index as _build_short_cite_index,
    cite_target_from_text as _cite_target_from_text,
    detect_links as detect_brief_links,
    iter_docket_cites as _iter_docket_cites,
    iter_recap_cites as _iter_recap_cites,
)
from court_catalog import (
    CATALOG as _COURT_CATALOG,
    CIRCUIT_COURTS as _CIRCUIT_COURTS,
    COURT_BLUEBOOK as _COURT_BLUEBOOK,
    DISTRICT_COURTS as _DISTRICT_COURTS,
    STATE_BLUEBOOK as _STATE_BLUEBOOK,
    STATE_COURTS as _STATE_COURTS,
    all_court_ids as _all_court_ids,
    bluebook_court_from_name as _bluebook_court_from_name,
    bluebook_federal_trial_court as _bluebook_federal_trial_court,
)

_CONFIG_PATH = Path.home() / ".config" / "courtlistener" / "config.json"


def _load_config() -> dict:
    """Return the saved app config, or an empty dict if it is missing/broken."""
    try:
        data = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_config(data: dict) -> None:
    """Persist the app config.  Failures are non-fatal."""
    try:
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Brief cache: PDFs opened in the brief viewer are kept for 30 days from the
# last time they were opened (each open restarts the clock) and listed in the
# Brief ▸ Recent Briefs menu for one-click reopening.
# ---------------------------------------------------------------------------

_BRIEF_CACHE_DIR = Path.home() / ".config" / "courtlistener" / "brief_cache"
_BRIEF_CACHE_INDEX = _BRIEF_CACHE_DIR / "briefs.json"
_BRIEF_CACHE_DAYS = 30
_brief_cache_lock = threading.Lock()


def _brief_cache_load() -> list[dict]:
    try:
        data = json.loads(_BRIEF_CACHE_INDEX.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _brief_cache_save(entries: list[dict]) -> None:
    try:
        _BRIEF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _BRIEF_CACHE_INDEX.write_text(
            json.dumps(entries, indent=1), encoding="utf-8")
    except Exception as exc:
        print(f"[brief-cache] save failed: {exc}")


def _brief_cache_prune(entries: list[dict]) -> list[dict]:
    """Drop briefs not opened in the last 30 days, deleting their files."""
    cutoff = time.time() - _BRIEF_CACHE_DAYS * 86400
    kept: list[dict] = []
    for e in entries:
        if (e.get("last_opened") or 0) >= cutoff and e.get("sha1"):
            kept.append(e)
        else:
            try:
                (_BRIEF_CACHE_DIR / f"{e.get('sha1', '')}.pdf").unlink()
            except OSError:
                pass
    return kept


def _brief_cache_record(data: bytes, name: str, mode: str) -> None:
    """Save a PDF opened in the brief viewer into the cache (each open
    restarts its 30-day clock and moves it to the top of Recent Briefs).
    ``mode`` remembers which viewer to reopen it in: "linked" (the on-page
    citation-link PDF view) or "text" (the brief text reader)."""
    import hashlib

    try:
        sha1 = hashlib.sha1(data).hexdigest()
        with _brief_cache_lock:
            _BRIEF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            path = _BRIEF_CACHE_DIR / f"{sha1}.pdf"
            if not path.is_file() or path.stat().st_size != len(data):
                tmp = path.with_suffix(f".{os.getpid()}.tmp")
                tmp.write_bytes(data)
                tmp.replace(path)
            entries = [e for e in _brief_cache_load()
                       if e.get("sha1") != sha1]
            entries.insert(0, {
                "sha1": sha1,
                "name": name,
                "mode": mode,
                "last_opened": time.time(),
            })
            _brief_cache_save(_brief_cache_prune(entries))
    except Exception as exc:
        print(f"[brief-cache] store failed: {exc}")


def _brief_cache_entries() -> list[dict]:
    """The cached briefs, newest-opened first, expired ones pruned."""
    with _brief_cache_lock:
        entries = _brief_cache_load()
        kept = _brief_cache_prune(entries)
        if len(kept) != len(entries):
            _brief_cache_save(kept)
    kept.sort(key=lambda e: e.get("last_opened") or 0, reverse=True)
    return kept


def _brief_cache_read(entry: dict) -> Optional[bytes]:
    try:
        data = (_BRIEF_CACHE_DIR / f"{entry.get('sha1', '')}.pdf").read_bytes()
        return data if data.startswith(b"%PDF") else None
    except Exception:
        return None


def _json_ready(value):
    """Convert nested values to something json can persist."""
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _load_saved_token() -> str:
    """Return the token saved in the config file, or '' if none."""
    return _load_config().get("api_token", "")


def _save_token(token: str) -> None:
    """Persist *token* to the config file."""
    data = _load_config()
    data["api_token"] = token
    _save_config(data)


def _default_spotlight_hotkey() -> str:
    return "<ctrl>+<space>" if sys.platform == "darwin" else "<ctrl>+<space>"


def _load_saved_spotlight_hotkey() -> str:
    key = _load_config().get("spotlight_hotkey")
    return key if isinstance(key, str) and key.strip() else _default_spotlight_hotkey()


def _save_spotlight_hotkey(hotkey: str) -> None:
    data = _load_config()
    data["spotlight_hotkey"] = hotkey
    _save_config(data)


def _display_hotkey(hotkey: str) -> str:
    names = {
        "ctrl": "Ctrl", "cmd": "Cmd", "alt": "Alt", "shift": "Shift",
        "space": "Space", "enter": "Enter", "esc": "Esc", "tab": "Tab",
        "backspace": "Backspace", "delete": "Delete", "up": "Up",
        "down": "Down", "left": "Left", "right": "Right",
        "home": "Home", "end": "End", "page_up": "Page Up",
        "page_down": "Page Down",
    }
    parts: list[str] = []
    for raw in re.findall(r"<[^>]+>|[^+]+", hotkey or ""):
        token = raw.strip().strip("<>").lower()
        if not token:
            continue
        if re.fullmatch(r"f\d{1,2}", token):
            parts.append(token.upper())
        elif len(token) == 1:
            parts.append(token.upper())
        else:
            parts.append(names.get(token, token.replace("_", " ").title()))
    return "+".join(parts) or _display_hotkey(_default_spotlight_hotkey())


# Persistent session for third-party hosts (LOC, GovInfo, static.case.law).
# Uses a full browser-like header set; government CDNs reset connections when
# they see Python's default User-Agent or missing Accept/Sec-Fetch headers.
_anon_session = _requests.Session()
_anon_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "application/pdf,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
})

# URL routing for official US Reports PDFs:
#   vols 1-501  → LOC CDN per-opinion PDFs preferred; GovInfo only as backup
#                 when LOC lacks the volume/page (vol 1 has no GovInfo copy)
#   vols 502-583 → GovInfo (link service, then direct per-opinion PDF)
#                 preferred, with the LOC CDN as backup through vol 542
#   vols 1-542  → LOC CDN per-opinion PDFs (volume and page both 3-digit
#                 zero-padded) — the preferred source at/below _LOC_PREFERRED_MAX
#                 and the GovInfo backup above it
#   vols 584+   → us_reports_pdf carves the opinion out of the Court's own
#                 bound-volume / preliminary-print PDF, downloaded from
#                 supremecourt.gov into the "US Reports" folder on first use
#                 and reused from there after
_LOC_CUTOFF = 542
# At/below this volume the LOC (Library of Congress) scan is preferred over
# GPO's GovInfo edition; GovInfo is used only when LOC hasn't the volume/page.
_LOC_PREFERRED_MAX = 501
_GOVINFO_MAX = 583
_US_CITE_RE = re.compile(r"(\d+)\s+U\.S\.\s+(\d+)")

# The Supreme Court Reporter ("93 S. Ct. 705", or "S.Ct." closed up as Google
# Scholar prints it).  Only SCOTUS opinions carry this reporter, so a match is a
# reliable "this is a Supreme Court case" signal even when court_id is blank.
_SCT_CITE_RE = re.compile(r"\d+\s+S\.\s?Ct\.\s+\d+")

# Regex to parse a standard legal citation: "volume reporter page"
# Examples: "410 F.2d 1234", "12 F. Supp. 2d 567", "100 Cal. 400"
_CITE_PARSE_RE = re.compile(r"^(\d+)\s+(.+)\s+(\d+)")

# An "Id., at N" cite links to the case last cited only when N is plausibly a
# page of that reporter — within this many pages of its start.  A small record
# page ("Id., at 45" pointing into the trial record, not the reporter) falls
# outside the window and is left as plain text.
_ID_PIN_WINDOW = 100


def _cite_start_page(cite: str) -> Optional[int]:
    """The reporter start page of a citation ("3 Dall. 386" → 386), ignoring any
    "@pin" suffix; None when it doesn't parse."""
    base = re.sub(r"<[^>]+>", "", (cite or "").split("@", 1)[0]).strip()
    m = _CITE_PARSE_RE.match(base)
    try:
        return int(m.group(3)) if m else None
    except (TypeError, ValueError):
        return None


def _id_pin_in_range(base_cite: str, pin) -> bool:
    """True when an "Id., at *pin*" page falls within ``_ID_PIN_WINDOW`` pages of
    *base_cite*'s start page — i.e. a page of that reporter, not a record page."""
    start = _cite_start_page(base_cite)
    try:
        n = int(pin)
    except (TypeError, ValueError):
        return False
    return start is not None and start <= n <= start + _ID_PIN_WINDOW


_CLUSTER_ID_RE = re.compile(r"/clusters/(\d+)/?")
_COURT_ID_RE = re.compile(r"/courts/([^/]+)/?")


def _extract_cluster_id(url: str) -> Optional[int]:
    """Parse a cluster ID out of a CourtListener clusters URL."""
    m = _CLUSTER_ID_RE.search(str(url))
    return int(m.group(1)) if m else None


def _extract_court_id(url: str) -> str:
    """Parse a court slug out of a CourtListener courts URL (e.g. 'scotus', 'ca9')."""
    m = _COURT_ID_RE.search(str(url))
    return m.group(1) if m else ""


def _cluster_citations_to_strings(citations) -> list[str]:
    """Convert cluster-endpoint citations (dicts or strings) to plain strings."""
    result: list[str] = []
    for c in (citations or []):
        if isinstance(c, dict):
            vol = c.get("volume", "")
            rep = c.get("reporter", "")
            page = c.get("page", "")
            if vol and rep and page:
                result.append(f"{vol} {rep} {page}")
        elif isinstance(c, str) and c.strip():
            result.append(c.strip())
    return result


# Bluebook rule 6.1(a): close up adjacent single capitals, but set a single
# capital off from a longer abbreviation with a space.  Ordinal series
# designators ("2d", "3d", "4th") count as single capitals for this purpose.
# Sources such as Google Scholar emit reporters closed up ("S.Ct.",
# "L.Ed.2d", "F.Supp.2d"); these helpers re-space them to the proper
# Bluebook form ("S. Ct.", "L. Ed. 2d", "F. Supp. 2d") while leaving cites
# that are already correct — including all-single-capital reporters like
# "U.S." and "N.Y.S.2d" — untouched.
_REPORTER_UNIT_RE = re.compile(
    r"[A-Z]\.|"                     # single capital + period: F. S. N. Y.
    r"\d+(?:st|nd|rd|th|d)|"        # ordinal series designator: 2d 3d 4th
    r"[A-Za-z][A-Za-z'’]*\.?"       # longer word, optional period: Supp. Ct. So. App'x
)
_SINGLE_CAP_RE = re.compile(r"[A-Z]\.")


def _reporter_unit_is_tight(unit: str) -> bool:
    """A single capital ("F.") or an ordinal ("2d") closes up with its
    neighbour; longer abbreviations ("Supp.", "Ct.") take a space."""
    return bool(_SINGLE_CAP_RE.fullmatch(unit)) or unit[:1].isdigit()


def _respace_reporter(reporter: str) -> str:
    """Re-space a reporter abbreviation per Bluebook rule 6.1(a)."""
    units = _REPORTER_UNIT_RE.findall(reporter)
    if not units:
        return reporter.strip()
    out = units[0]
    for prev, cur in zip(units, units[1:]):
        tight = _reporter_unit_is_tight(prev) and _reporter_unit_is_tight(cur)
        out += ("" if tight else " ") + cur
    return out


def _respace_reporter_in_cite(cite: str) -> str:
    """Re-space the reporter inside a "volume reporter page" citation,
    leaving the string unchanged if it isn't a standard reporter cite."""
    m = _CITE_PARSE_RE.match(cite or "")
    if not m:
        return cite
    vol, reporter, page = m.group(1), m.group(2).strip(), m.group(3)
    return f"{vol} {_respace_reporter(reporter)} {page}"


# Priority-ordered patterns for picking the best citation for display,
# filenames, and Google Scholar searches.
# Order: U.S. Reports > S.Ct. > Federal Reporter (newest first) >
#        Federal Supplement > state/other reporters > anything non-Lexis.
_CITE_PRIORITY = [
    re.compile(r" U\.S\. "),
    re.compile(r" S\. Ct\. "),
    re.compile(r" F\.4th "),
    re.compile(r" F\.3d "),
    re.compile(r" F\.2d "),
    re.compile(r" F\. \d"),          # "F. " immediately before a digit (not F. Supp.)
    re.compile(r" F\. Supp\. 3d "),
    re.compile(r" F\. Supp\. 2d "),
    re.compile(r" F\. Supp\. "),
    re.compile(r" B\.R\. "),
]

_NOISE_CITE_RE = re.compile(r"lexis|westlaw|\bwl\b", re.IGNORECASE)

# Annotation / specialty / looseleaf series dropped from the title's parallel
# cites — the title lists only standard reporters (U.S./S. Ct./L. Ed., the
# official state reporters, and the regional reporters).
_NONSTANDARD_CITE_RE = re.compile(
    r"\bA\.\s?L\.\s?R\."           # American Law Reports (A.L.R., A.L.R.2d, …)
    r"|\bL\.\s?R\.\s?A\."          # Lawyers' Reports Annotated
    r"|\bU\.\s?S\.\s?L\.\s?W\."    # United States Law Week
    r"|\bOhio\s+Op\."             # Ohio Opinions (unofficial)
    r"|\bMedia\s+L\."            # Media Law Reporter
    r"|\((?:BNA|CCH|P-?H)\)",      # looseleaf services — (BNA), (CCH), (P-H)
    re.IGNORECASE,
)

# Reporter order for the window-title citation: the major national reporters
# first (Bluebook order), everything else after, in the order it was seen.
_TITLE_CITE_RANK = [
    re.compile(r" U\.S\. "),
    re.compile(r" S\. ?Ct\. "),
    re.compile(r" L\. ?Ed\."),
    re.compile(r" F\.4th "),
    re.compile(r" F\.3d "),
    re.compile(r" F\.2d "),
    re.compile(r" F\. \d"),
    re.compile(r" F\. Supp\."),
    re.compile(r" B\.R\. "),
]


def _pick_citation(citations) -> str:
    """
    Return the most useful citation from *citations* for display,
    filenames, and Google Scholar searches.

    Strips HTML tags, discards Lexis/Westlaw cites, then walks
    ``_CITE_PRIORITY`` to find the best reporter.  Falls back to the
    first non-noise cite, or the raw first entry if everything is noise.
    """
    if not citations:
        return ""
    if isinstance(citations, str):
        citations = [citations]

    clean = [re.sub(r"<[^>]+>", "", c).strip() for c in citations]
    non_noise = [c for c in clean if c and not _NOISE_CITE_RE.search(c)]

    pool = non_noise if non_noise else clean
    for pat in _CITE_PRIORITY:
        hit = next((c for c in pool if pat.search(c)), None)
        if hit:
            return hit

    return pool[0] if pool else ""


def _is_paginable_cite(c: str) -> bool:
    """True for a print reporter citation whose pages a star pagination can
    follow.  Excludes neutral/electronic cites — a year-volume cite (e.g.
    ``2011 N.J. LEXIS 87``) or a LEXIS/Westlaw reporter — whose numbers are
    unrelated to a print reporter's pages, so they're never matched against the
    star pagination."""
    m = _CITE_PARSE_RE.match(c or "")
    if not m:
        return False
    vol, rep = m.group(1), m.group(2)
    if re.fullmatch(r"(?:1[6-9]|20)\d{2}", vol):  # year-volume = neutral cite
        return False
    return not re.search(r"\b(?:LEXIS|WL|Westlaw)\b", rep, re.IGNORECASE)


def _scholar_parallel_cites(client, item: dict, exclude: str) -> list[str]:
    """A CourtListener case's parallel *standard-reporter* citations — the
    same set the window title shows (print reporters only: no Lexis/Westlaw
    or neutral cites, no annotation/looseleaf series) — excluding *exclude*
    (the cite that already failed on Google Scholar).  Used to retry Scholar
    under an alternate cite when the clicked one found nothing."""
    def key(c: str) -> str:
        return re.sub(r"[^a-z0-9]", "", (c or "").lower())

    raw = item.get("citation", [])
    cites = list(raw) if isinstance(raw, list) else [raw] if raw else []
    cluster_id = item.get("cluster_id") or item.get("id")
    if client is not None and cluster_id:
        try:
            rec = client.get_cluster(int(cluster_id), fields="citations")
            cites += _cluster_citations_to_strings(rec.get("citations"))
        except Exception as exc:
            print(f"[scholar-alt] cluster citations fetch failed: {exc}")

    seen = {key(exclude)}
    out: list[str] = []
    for c in cites:
        c = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", str(c))).strip()
        if (not c or not _is_paginable_cite(c)
                or _NOISE_CITE_RE.search(c)
                or _NONSTANDARD_CITE_RE.search(c)):
            continue
        k = key(c)
        if k in seen:
            continue
        seen.add(k)
        out.append(c)
    return out[:4]



# Strip reporter series designators ("2d", "4th") and volume/page digits
# before comparing reporter words against a court abbreviation.
_REPORTER_SERIES_RE = re.compile(r"\b\d*(?:2d|3d|4th|5th|6th)\b\.?|\b\d+\b")

_SCOTUS_REPORTERS = {"U.S.", "S. Ct.", "S.Ct.", "L. Ed.", "L. Ed. 2d", "L.Ed.", "L.Ed.2d"}


def _court_for_paren(citation: str, court_id: str, fallback: str = "") -> str:
    """
    Court abbreviation for a Bluebook date parenthetical, omitting or
    trimming whatever the reporter title already conveys (rule 10.4):

      60 Fed. Cl. 600        → ()          reporter names the court
      306 Md. 556            → ()          official state reporter, highest court
      100 Cal. App. 4th 454  → ()          official reporter names the court
      75 Cal. Rptr. 2d 1     → (Ct. App.)  reporter conveys the state only
      12 N.Y.S.2d 345        → (App. Div.) reporter conveys the state only
      510 A.2d 562           → (Md.)       regional reporter conveys nothing
    """
    court_id = (court_id or "").strip().lower()
    m = _CITE_PARSE_RE.match(citation or "")
    reporter = m.group(2).strip() if m else ""
    if "scotus" in court_id or reporter in _SCOTUS_REPORTERS:
        return ""
    abbr = _COURT_BLUEBOOK.get(court_id, "")
    if not abbr:
        # An id outside the catalog: bluebook the court *name* CourtListener
        # supplied ("Court of Appeals of Ohio" → "Ohio Ct. App.",
        # "United States Bankruptcy Court, S.D. Texas" → "Bankr. S.D.
        # Tex.") rather than printing it raw; keep the raw fallback only
        # when the name isn't recognizable.  A fallback that is just the
        # court id ("circtsdny") is never printed — "(circtsdny 1904)" is
        # worse than no court at all.
        name = (fallback or "").strip()
        abbr = (_bluebook_court_from_name(name)
                or _bluebook_federal_trial_court(name)
                or name)
        if abbr and (abbr == court_id
                     or re.fullmatch(r"[a-z0-9]{2,}", abbr)):
            return ""
    if not abbr or not reporter:
        return abbr
    rep_tokens = [t for t in _REPORTER_SERIES_RE.sub(" ", reporter).split() if t]
    ct_tokens = abbr.split()
    meaningful = [t for t in ct_tokens if t != "Ct."]
    if meaningful and all(t in rep_tokens for t in meaningful):
        return ""
    if (
        rep_tokens
        and len(ct_tokens) > 1
        and rep_tokens[0].replace(".", "").lower().startswith(
            ct_tokens[0].replace(".", "").lower()
        )
    ):
        return " ".join(ct_tokens[1:])
    return abbr


def _build_default_filename(item: dict) -> str:
    """
    Return a sanitized default filename (without extension) for saving an opinion.

    Format: ``Case Name, Reporter Cite (Court YEAR)``
    The court abbreviation follows Bluebook rule 10.4 — omitted for SCOTUS
    and whenever the reporter already conveys it (e.g. ``60 Fed. Cl. 600``).
    Falls back gracefully when citation or date are missing.
    """
    # Case name, abbreviated per Bluebook rule 10.2.2 (table T6/T10)
    case_name = abbreviate_case_name(re.sub(
        r"<[^>]+>", "",
        item.get("caseName") or item.get("case_name") or "opinion"
    ).strip())

    # Best citation (U.S. Reports > S.Ct. > Federal Reporters > others)
    citation_str = _pick_citation(item.get("citation", []))

    # Year from date filed
    date_filed = item.get("dateFiled") or item.get("date_filed") or ""
    year = date_filed[:4] if len(date_filed) >= 4 else ""

    # Court abbreviation — omitted when SCOTUS or conveyed by the reporter
    court_id = str(item.get("court_id") or item.get("court") or "").strip().lower()
    court_abbr = _court_for_paren(
        citation_str, court_id, str(item.get("court") or court_id).strip()
    )

    # Build the parenthetical: (Court YEAR) or (YEAR) for SCOTUS
    if court_abbr and year:
        paren = f"({court_abbr} {year})"
    elif year:
        paren = f"({year})"
    elif court_abbr:
        paren = f"({court_abbr})"
    else:
        paren = ""

    # Assemble parts, skipping empty ones.
    # Join case name + citation with a comma, then append the parenthetical
    # with a space only (no comma before it).
    main_parts = [p for p in [case_name, citation_str] if p]
    raw_name = ", ".join(main_parts)
    if paren:
        raw_name = f"{raw_name} {paren}" if raw_name else paren

    # Sanitize: keep alphanumeric, spaces, and common filename-safe punctuation
    safe = "".join(
        c if c.isalnum() or c in " .,()-_'&" else "_"
        for c in raw_name
    )[:120].strip()
    return safe


def _named_temp_pdf_path(stem: str) -> str:
    """A path in the system temp directory named ``<stem>.pdf``, so a PDF handed
    to the OS print dialog already carries its Bluebook file name — if the user
    then "Save As" from the viewer, the suggested name is already right (instead
    of a random ``tmpXXXX.pdf``).

    When that name is already taken, a number is appended (" (2)", " (3)", …);
    if no free variant can be found, the base name is reused (overwriting the
    existing file)."""
    stem = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", (stem or "").strip()).strip()
    stem = stem[:120].strip(" .") or "opinion"
    tmpdir = tempfile.gettempdir()
    base = os.path.join(tmpdir, f"{stem}.pdf")
    if not os.path.exists(base):
        return base
    for n in range(2, 1000):
        cand = os.path.join(tmpdir, f"{stem} ({n}).pdf")
        if not os.path.exists(cand):
            return cand
    return base  # couldn't differentiate — overwrite the existing file


def _us_reports_loc_url(citation: str) -> Optional[str]:
    """
    Return the LOC CDN PDF URL for a US Reports citation, or None if the
    volume falls outside the LOC collection (vols 1-542 only).
    """
    m = _US_CITE_RE.search(citation)
    if not m:
        return None
    vol, page = int(m.group(1)), int(m.group(2))
    if vol > _LOC_CUTOFF:
        return None
    return (
        f"https://cdn.loc.gov/service/ll/usrep/"
        f"usrep{vol:03d}/usrep{vol:03d}{page:03d}/usrep{vol:03d}{page:03d}.pdf"
    )


def _us_reports_govinfo_url(citation: str) -> Optional[tuple[str, str]]:
    """
    Return (link_url, direct_pdf_url) for a US Reports citation, or None if
    the volume is outside the GovInfo range (vols 2-582).

    GovInfo holds US Reports starting from vol 2, so this also serves as a
    fallback for vols 1-542 when the LOC CDN is unavailable.

    link_url:       https://www.govinfo.gov/link/usreports/{vol}/{page}
    direct_pdf_url: https://www.govinfo.gov/content/pkg/USREPORTS-{vol}/pdf/USREPORTS-{vol}-{page}.pdf
    """
    m = _US_CITE_RE.search(citation)
    if not m:
        return None
    vol, page = int(m.group(1)), int(m.group(2))
    if vol > _GOVINFO_MAX:
        return None
    link_url = f"https://www.govinfo.gov/link/usreports/{vol}/{page}"
    direct_url = f"https://www.govinfo.gov/content/pkg/USREPORTS-{vol}/pdf/USREPORTS-{vol}-{page}.pdf"
    return link_url, direct_url


def _slugify_reporter(reporter: str) -> str:
    """
    Convert a reporter abbreviation to the slug used by static.case.law.

    The Caselaw Access Project slugify rules:
      1. Lowercase
      2. Spaces → hyphens
      3. Remove all characters that are not alphanumeric or hyphens
      4. Collapse consecutive hyphens; strip leading/trailing hyphens

    Examples:
      "F.2d"        → "f2d"
      "F.3d"        → "f3d"
      "F. Supp."    → "f-supp"
      "F. Supp. 2d" → "f-supp-2d"
      "F. App'x"    → "f-appx"
      "Cal."        → "cal"
      "N.E.2d"      → "ne2d"
    """
    s = reporter.lower()
    s = s.replace(" ", "-")
    s = re.sub(r"[^a-z0-9-]", "", s)
    s = re.sub(r"-+", "-", s)
    s = s.strip("-")
    # Old reporter names → the slug static.case.law actually uses.  The Federal
    # Reporter is "F." today, so an old "Fed. Rep." cite must look there.
    return _CASE_LAW_REPORTER_ALIASES.get(s, s)


# Old/long reporter names → the slug static.case.law uses for the modern form.
_CASE_LAW_REPORTER_ALIASES = {
    "fed-rep": "f",        # Federal Reporter (old "Fed. Rep." → "F.")
    "fed-rep-2d": "f2d",
    "fed-rep-3d": "f3d",
    # Federal Appendix spelled without the canonical spacing/apostrophe slugs to
    # several forms; static.case.law uses "f-appx".
    "fappx": "f-appx",     # "F.App'x", "F.Appx."
    "fedappx": "f-appx",   # "Fed.App'x", "Fed.Appx."
    "fed-appx": "f-appx",  # "Fed. App'x", "Fed. Appx."
}


def _static_case_law_url(citation: str) -> Optional[str]:
    """
    Return the PDF URL candidate on static.case.law for a citation string
    such as '410 F.2d 1234', or None if the citation cannot be parsed.

    URL pattern:
      https://static.case.law/{reporter-slug}/{volume}/case-pdfs/{page:04d}-01.pdf
    """
    citation = re.sub(r"<[^>]+>", "", citation).strip()
    m = _CITE_PARSE_RE.match(citation)
    if not m:
        return None
    vol, reporter, page = m.group(1), m.group(2).strip(), m.group(3)
    slug = _slugify_reporter(reporter)
    if not slug:
        return None
    return f"https://static.case.law/{slug}/{vol}/case-pdfs/{int(page):04d}-01.pdf"


@dataclass(frozen=True)
class _CaseLawPdfChoice:
    cite: str
    url: str
    label: str = ""


@dataclass(frozen=True)
class _CaseLawPageOpinion:
    """One CAP opinion sharing a reporter page with one or more others."""
    url: str
    json_url: str
    name: str


_CASE_LAW_NUMBERED_PDF_RE = re.compile(
    r"^(https://static\.case\.law/.*/case-pdfs/\d+)-(\d+)\.pdf$",
    re.IGNORECASE,
)


def _case_law_numbered_pdf_url(url: str, number: int) -> Optional[str]:
    """Sibling ``-NN.pdf`` URL for a static.case.law per-case PDF."""
    m = _CASE_LAW_NUMBERED_PDF_RE.match((url or "").strip())
    if not m or number < 1:
        return None
    width = max(2, len(m.group(2)))
    return f"{m.group(1)}-{number:0{width}d}.pdf"


def _case_law_json_for_pdf_url(url: str) -> Optional[str]:
    """The CAP metadata JSON published beside a per-case PDF URL."""
    if not _CASE_LAW_NUMBERED_PDF_RE.match((url or "").strip()):
        return None
    return re.sub(
        r"/case-pdfs/(\d+-\d+)\.pdf$", r"/cases/\1.json",
        url, flags=re.IGNORECASE,
    )


def _case_law_page_opinions(url: str) -> list[_CaseLawPageOpinion]:
    """Discover all CAP opinions whose PDF begins on the same reporter page.

    CAP names the first ``NNNN-01.pdf`` and, rarely, puts another opinion that
    begins on that page at ``-02.pdf`` (then ``-03.pdf``, and so on).  A second
    PDF is probed first; only when it exists are all sibling JSON files fetched
    for their authoritative case names.  An empty list means the URL is not an
    ``-01`` CAP PDF or no second opinion exists.
    """
    m = _CASE_LAW_NUMBERED_PDF_RE.match((url or "").strip())
    if not m or int(m.group(2)) != 1:
        return []

    def exists(candidate: str) -> bool:
        try:
            resp = _anon_session.head(
                candidate, timeout=10, allow_redirects=True)
            return resp.status_code == 200
        except Exception as exc:
            print(f"[case.law] sibling check failed for {candidate}: {exc}")
            return False

    second = _case_law_numbered_pdf_url(url, 2)
    if not second or not exists(second):
        return []

    pdf_urls = [url, second]
    for number in range(3, 100):
        candidate = _case_law_numbered_pdf_url(url, number)
        if not candidate or not exists(candidate):
            break
        pdf_urls.append(candidate)

    out: list[_CaseLawPageOpinion] = []
    for pdf_url in pdf_urls:
        json_url = _case_law_json_for_pdf_url(pdf_url) or ""
        name = ""
        if json_url:
            try:
                resp = _anon_session.get(json_url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        name = re.sub(
                            r"\s+", " ", str(
                                data.get("name_abbreviation")
                                or data.get("name") or ""
                            ),
                        ).strip()
            except Exception as exc:
                print(f"[case.law] metadata fetch failed {json_url}: {exc}")
        out.append(_CaseLawPageOpinion(pdf_url, json_url, name))
    return out


def _case_law_opinion_name(opinion: _CaseLawPageOpinion) -> str:
    """Readable, Bluebook-abbreviated label for a CAP page choice."""
    if not opinion.name:
        return ""
    try:
        return abbreviate_case_name(normal_case_caption(opinion.name))
    except Exception:
        return opinion.name


def _match_case_law_page_opinion(
    opinions: list[_CaseLawPageOpinion], expected_name: str,
) -> Optional[_CaseLawPageOpinion]:
    """Best unambiguous CAP page opinion for a CL/Scholar case name."""
    expected = re.sub(r"<[^>]+>", "", expected_name or "").strip()
    if not expected:
        return None
    rated: list[tuple[int, float, _CaseLawPageOpinion]] = []
    for opinion in opinions:
        if not opinion.name:
            continue
        tier = _match_tier(expected, opinion.name)
        score = _name_match_score(expected, opinion.name)
        if tier >= 0 and score >= _NAME_MATCH_MIN:
            rated.append((tier, score, opinion))
    if not rated:
        return None
    rated.sort(key=lambda row: (row[0], row[1]), reverse=True)
    best = rated[0]
    # A two-sided source caption should match both CAP parties.  Falling back
    # to a one-party coincidence is precisely when the user should choose.
    if len(_name_parties(expected)) == 2 and best[0] < 2:
        return None
    if (len(rated) > 1 and rated[1][0] == best[0]
            and abs(rated[1][1] - best[1]) < 0.01):
        return None
    return best[2]


def _case_law_pdf_choices_for_cites(
    cites: list[str], expected_name: str = "",
) -> list[_CaseLawPdfChoice]:
    """Every static.case.law PDF that exists for the supplied parallel cites.

    State opinions are often printed in both an official state reporter and a
    regional reporter.  CAP stores those scans under each reporter, so keep
    probing after the first hit and let the reader choose among them.
    """
    choices: list[_CaseLawPdfChoice] = []
    seen_urls: set[str] = set()
    for raw in cites:
        cite = re.sub(r"<[^>]+>", "", str(raw or "")).strip()
        if (not cite or _NOISE_CITE_RE.search(cite)
                or _NONSTANDARD_CITE_RE.search(cite)):
            continue
        url = _static_case_law_url(cite)
        if not url or url in seen_urls:
            continue
        print(f"[case.law] checking static.case.law: {url}")
        try:
            head = _anon_session.head(url, timeout=10, allow_redirects=True)
        except Exception as exc:
            print(f"[case.law] HEAD check failed for {url}: {exc}")
            continue
        if head.status_code == 200:
            seen_urls.add(url)
            chosen_url = url
            page_opinions = _case_law_page_opinions(url)
            if page_opinions and expected_name:
                matched = _match_case_law_page_opinion(
                    page_opinions, expected_name)
                if matched is not None:
                    chosen_url = matched.url
                    print(
                        f"[case.law] matched {expected_name!r} to "
                        f"{_case_law_opinion_name(matched) or matched.url!r}"
                    )
                else:
                    print(
                        f"[case.law] could not match {expected_name!r} among "
                        f"{len(page_opinions)} opinions on the reporter page"
                    )
                    # Preserve every sibling as a separate View PDF menu item.
                    # The source name could not safely choose, so the user must.
                    for i, opinion in enumerate(page_opinions, 1):
                        name = _case_law_opinion_name(opinion)
                        label = f"{cite} â€” {name}" if name else (
                            f"{cite} â€” Opinion {i}"
                        )
                        choices.append(_CaseLawPdfChoice(
                            cite=cite, url=opinion.url, label=label,
                        ))
                    continue
            choices.append(_CaseLawPdfChoice(cite=cite, url=chosen_url))
        else:
            print(f"[case.law] static.case.law {head.status_code} for {cite!r}")
    return choices


# --- Early-SCOTUS "nominative" reporters -------------------------------------
# Before 1875 the U.S. Reports were cited by the Reporter of Decisions' name
# ("3 Dall. 386", "1 Cranch 137").  Google Scholar prints these old SCOTUS cites
# in nominative form, which the standard citation parser doesn't recognize and
# static.case.law doesn't slug — yet CourtListener resolves them and case.law
# files the opinion under the modern "U.S." volume.  Each reporter maps to the
# offset from its volume to the U.S.-Reports volume (the page is unchanged):
# Dallas 1-4 → U.S. 1-4, Cranch 1-9 → U.S. 5-13, Wheaton 1-12 → U.S. 14-25,
# Peters 1-16 → U.S. 26-41, Howard 1-24 → U.S. 42-65, Black 1-2 → U.S. 66-67,
# Wallace 1-23 → U.S. 68-90.
_NOMINATIVE_US_OFFSET = {
    "dall": 0, "dallas": 0, "cranch": 4, "wheat": 13, "wheaton": 13,
    "pet": 25, "peters": 25, "how": 41, "howard": 41, "black": 65,
    "wall": 67, "wallace": 67, "otto": 90,
}
_NOMINATIVE_CANON = {
    "dall": "Dall.", "dallas": "Dall.", "cranch": "Cranch", "wheat": "Wheat.",
    "wheaton": "Wheat.", "pet": "Pet.", "peters": "Pet.", "how": "How.",
    "howard": "How.", "black": "Black", "wall": "Wall.", "wallace": "Wall.",
    "otto": "Otto",
}
# Case-sensitive on purpose: a reporter abbreviation is capitalized in a real
# cite, so the digits-around requirement plus the capital keeps common words
# ("how", "black", "wall") in prose from being mistaken for a citation.
_NOMINATIVE_CITE_RE = re.compile(
    r"\b(\d{1,2})\s+(Dall|Dallas|Cranch|Wheat|Wheaton|Pet|Peters|How|Howard|"
    r"Black|Wall|Wallace|Otto)\.?\s+(\d{1,4})\b"
)
_NOMINATIVE_DISPLAY_RE = re.compile(
    r"^\s*(\d+)\s+U\.S\.\s+\((\d+)\s+"
    r"(Dall|Dallas|Cranch|Wheat|Wheaton|Pet|Peters|How|Howard|"
    r"Black|Wall|Wallace|Otto)\.?\)\s+(\d+)\s*$",
    re.IGNORECASE,
)

_WI_NEUTRAL_CITE_RE = re.compile(r"^\d{4}\s+WI(?:\s+App)?\s+\d+$")
_WI_OFFICIAL_CITE_RE = re.compile(r"^\d+\s+Wis\.\s*(?:2d\s+)?\d+$")
_WI_REGIONAL_CITE_RE = re.compile(r"^\d+\s+N\.W\.(?:2d\s+)?\d+$")
_REGIONAL_REPORTER_RE = re.compile(
    r"\s(?:A\.(?:2d|3d)?|P\.(?:2d|3d)?|N\.E\.(?:2d|3d)?|"
    r"N\.W\.(?:2d)?|S\.E\.(?:2d)?|S\.W\.(?:2d|3d)?|"
    r"So\.(?:\s*[23]d)?)\s",
    re.IGNORECASE,
)


def _nominative_display_cite(us_cite: str, citations) -> str:
    """Bluebook's dual modern/nominative form for early U.S. Reports.

    ``3 U.S. 199`` plus ``3 Dall. 199`` becomes
    ``3 U.S. (3 Dall.) 199``.  The relationship is verified through the
    reporter-volume offset table so an unrelated parallel cannot be inserted.
    """
    um = re.fullmatch(r"(\d+)\s+U\.S\.\s+(\d+)", us_cite or "")
    if not um:
        return ""
    for raw in citations or []:
        text = re.sub(r"\s+", " ", str(raw or "")).strip()
        nm = _NOMINATIVE_CITE_RE.fullmatch(text)
        if not nm or _us_reports_cite(text) != us_cite:
            continue
        reporter = _NOMINATIVE_CANON.get(nm.group(2).lower(), nm.group(2))
        return f"{um.group(1)} U.S. ({nm.group(1)} {reporter}) {um.group(2)}"
    return ""


def _nominative_display_component_keys(display_cite: str) -> set[str]:
    """Normalized component cites represented by a combined nominative cite.

    ``5 U.S. (1 Cranch) 299`` already conveys both ``5 U.S. 299`` and
    ``1 Cranch 299``.  Returning their identity keys lets the case-window title
    suppress those redundant standalone parallels while retaining them when a
    combined display cite could not be built.
    """
    m = _NOMINATIVE_DISPLAY_RE.fullmatch(display_cite or "")
    if not m:
        return set()
    reporter = _NOMINATIVE_CANON.get(m.group(3).lower(), m.group(3))
    components = (
        f"{m.group(1)} U.S. {m.group(4)}",
        f"{m.group(2)} {reporter} {m.group(4)}",
    )
    return {
        re.sub(r"[^a-z0-9]", "", component.lower())
        for component in components
    }


def _wisconsin_display_cite(citations) -> str:
    """Wisconsin SCR 80.02 initial-citation order, when available."""
    clean = [
        re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", str(c or ""))).strip()
        for c in citations or []
    ]
    neutral = next((c for c in clean if _WI_NEUTRAL_CITE_RE.fullmatch(c)), "")
    if not neutral:
        return ""
    official = next((c for c in clean if _WI_OFFICIAL_CITE_RE.fullmatch(c)), "")
    regional = next((c for c in clean if _WI_REGIONAL_CITE_RE.fullmatch(c)), "")
    return ", ".join(c for c in (neutral, official, regional) if c)


def _us_reports_cite(cite: str) -> str:
    """A nominative early-SCOTUS citation in its modern "U.S." form
    ("3 Dall. 386" → "3 U.S. 386", "1 Cranch 137" → "5 U.S. 137"), which
    CourtListener and static.case.law index under; "" when *cite* names no
    nominative reporter."""
    m = _NOMINATIVE_CITE_RE.search(cite or "")
    if not m:
        return ""
    off = _NOMINATIVE_US_OFFSET.get(m.group(2).lower())
    return f"{int(m.group(1)) + off} U.S. {m.group(3)}" if off is not None else ""


def _citation_search_variants(query: str) -> tuple[str, ...]:
    """The query as entered, plus its U.S.-Reports equivalent when it
    contains an early-SCOTUS nominative citation.

    Search services do not agree on which side of a dual citation they index:
    a user may type ``8 Wall. 168`` while CourtListener or Google Scholar only
    recognizes ``75 U.S. 168``.  Preserve surrounding case-name and pincite
    text so a full query remains just as specific, and retain the typed form as
    a fallback for sources that do index the old reporter.
    """
    query = re.sub(r"\s+", " ", query or "").strip()
    if not query:
        return ()
    variants = [query]
    m = _NOMINATIVE_CITE_RE.search(query)
    if m:
        us_cite = _us_reports_cite(m.group(0))
        if us_cite:
            expanded = query[:m.start()] + us_cite + query[m.end():]
            if expanded not in variants:
                variants.append(expanded)
    return tuple(variants)


def _link_cite(text: str, short_cite_index) -> tuple[str, str]:
    """(reporter cite, pincite) for a cited-case hyperlink's text, recognizing
    the old nominative SCOTUS reporters the standard parser misses ("Calder v.
    Bull, 3 Dall. 386, 388" → ("3 Dall. 386", "388")) so a Scholar link Google
    can't open can still be located on CourtListener or static.case.law."""
    cite, pin = _cite_target_from_text(text, short_cite_index)
    if cite:
        return cite, pin
    plain = re.sub(r"<[^>]+>", "", text or "")
    m = _NOMINATIVE_CITE_RE.search(plain)
    if m:
        rep = _NOMINATIVE_CANON.get(m.group(2).lower(), m.group(2))
        cite = f"{m.group(1)} {rep} {m.group(3)}"
        pm = re.match(r"[\s,]+(\d{1,5})\b", plain[m.end():])
        return cite, (pm.group(1) if pm else "")
    return "", ""


def _case_law_pdf_for_cite(cite: str) -> Optional[str]:
    """A static.case.law PDF URL that actually exists (HEAD 200) for *cite* —
    trying the citation as printed and then its modern U.S.-Reports form for an
    old nominative SCOTUS cite — or None when case.law has neither."""
    choices = _case_law_pdf_choices_for_cites([cite, _us_reports_cite(cite)])
    return choices[0].url if choices else None


def _case_law_json_url(citation: str) -> Optional[str]:
    """The per-case JSON URL on static.case.law for *citation* (e.g.
    ``https://static.case.law/f-appx/1/cases/0002-01.json`` for ``1 F. App'x
    2``), or None when the citation can't be parsed.  The JSON carries the
    opinion text and metadata — used for Federal Appendix cases, whose scans
    have no reliable text layer."""
    citation = re.sub(r"<[^>]+>", "", citation).strip()
    m = _CITE_PARSE_RE.match(citation)
    if not m:
        return None
    vol, reporter, page = m.group(1), m.group(2).strip(), m.group(3)
    slug = _slugify_reporter(reporter)
    if not slug:
        return None
    return f"https://static.case.law/{slug}/{vol}/cases/{int(page):04d}-01.json"


@lru_cache(maxsize=256)
def _case_law_metadata(cite: str) -> "Optional[dict]":
    """Authoritative per-case metadata for a reporter citation, cached for the
    life of the app.  A citation-keyed URL cannot drift to a merely similar
    case, so its capitalization is safe to prefer over OCR/title-casing guesses.

    A miss is simply a miss (CAP's earliest U.S. Reports scans are paginated
    differently from the citations they carry, so early Supreme Court cases
    404 here): the caller falls back to CourtListener like any other case
    CAP does not hold.
    """
    url = _case_law_json_url(cite)
    if not url:
        return None
    try:
        resp = _anon_session.get(url, timeout=10)
    except Exception as exc:
        print(f"[case.law-json] metadata fetch failed {url}: {exc}")
        return None
    if resp.status_code != 200:
        print(f"[case.law-json] {resp.status_code} for {cite!r}")
        return None
    try:
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception as exc:
        print(f"[case.law-json] bad JSON for {cite!r}: {exc}")
        return None


# CAP's ``name_abbreviation`` is curated for the official reports but
# machine-generated for the vendor reporters, where it can silently drop
# words — the S. Ct. file for NIFLA (138 S. Ct. 2361) reads "Nat'l Inst.
# Advocates & Life Advocates v. Becerra".  Case *names* are therefore
# harvested only from official-reporter files; vendor files remain fine
# for factual fields (decision_date).
_CAP_VENDOR_REPORTER_RE = re.compile(
    r"\b(?:S\.\s?Ct\.|L\.\s?Ed\.|LEXIS|WL|U\.S\.L\.W\.)", re.IGNORECASE)


def _case_law_name_for_cites(cites) -> str:
    """First CAP caption usable as a capitalization reference.

    An opened opinion's case name remains authoritative; callers may copy only
    casing for identical words from this value, never substitute the caption.
    """
    seen: set[str] = set()
    attempted = 0
    for raw in cites:
        cite = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", str(raw or ""))).strip()
        key = re.sub(r"\s+", "", cite).lower()
        if not cite or key in seen:
            continue
        seen.add(key)
        if _CAP_VENDOR_REPORTER_RE.search(cite):
            continue
        if not _case_law_json_url(cite):
            continue
        attempted += 1
        data = _case_law_metadata(cite)
        if not data:
            continue
        name = re.sub(r"\s+", " ", str(
            data.get("name_abbreviation") or data.get("name") or ""
        )).strip()
        if name:
            return name
        if attempted >= 6:
            break
    return ""


@dataclass(frozen=True)
class _CaseLawTextRecord:
    """Parsed opinion text and display metadata from one exact CAP JSON file."""
    text: str
    citation: str
    item: dict
    json_url: str = ""


def _case_law_text_record(
    data, fallback_cite: str = "", json_url: str = "",
) -> "Optional[_CaseLawTextRecord]":
    """Turn a static.case.law case JSON object into a text-view record."""
    if not isinstance(data, dict):
        return None

    name = re.sub(r"\s+", " ", str(
        data.get("name_abbreviation") or data.get("name") or ""
    )).strip()
    citation_rows = data.get("citations") or []
    cites = [
        re.sub(r"\s+", " ", str(row.get("cite") or "")).strip()
        for row in citation_rows if isinstance(row, dict) and row.get("cite")
    ]
    official = next((
        re.sub(r"\s+", " ", str(row.get("cite") or "")).strip()
        for row in citation_rows
        if isinstance(row, dict) and row.get("type") == "official"
        and row.get("cite")
    ), "")
    cite_str = official or (cites[0] if cites else "") or fallback_cite
    court_data = data.get("court") or {}
    if not isinstance(court_data, dict):
        court_data = {}
    court = str(
        court_data.get("name_abbreviation")
        or court_data.get("name") or ""
    ).strip()
    court_id = str(
        court_data.get("slug")
        or court_data.get("id") or ""
    ).strip()
    date = str(data.get("decision_date") or "")
    year = date[:4] if len(date) >= 4 else ""
    paren = " ".join(p for p in (court, year) if p)
    stem = ", ".join(p for p in (name, cite_str) if p) or cite_str
    if paren:
        stem = f"{stem} ({paren})" if stem else f"({paren})"

    body = data.get("casebody") or {}
    # Some CAP exports nest the opinion under casebody["data"].
    if isinstance(body, dict) and isinstance(body.get("data"), dict):
        body = body["data"]
    if not isinstance(body, dict):
        return None
    chunks: list[str] = []
    head = str(body.get("head_matter") or "").strip()
    if head:
        chunks.append(head)
    for op in body.get("opinions") or []:
        if not isinstance(op, dict):
            continue
        text = str(op.get("text") or "").strip()
        if text:
            author = str(op.get("author") or "").strip()
            chunks.append(
                f"{author}\n{text}"
                if author and not text.startswith(author) else text
            )
    text = "\n\n".join(chunks).strip()
    if not text:
        return None
    item = {
        "caseName": name,
        "citation": cites or ([cite_str] if cite_str else []),
        "court": court,
        "court_id": court_id,
        "dateFiled": date,
    }
    return _CaseLawTextRecord(text, stem, item, json_url)


def _case_law_text_for_pdf_url(
    pdf_url: str, fallback_cite: str = "",
) -> "Optional[_CaseLawTextRecord]":
    """Fetch the JSON corresponding to this exact numbered CAP case PDF."""
    json_url = _case_law_json_for_pdf_url(pdf_url)
    if not json_url:
        return None
    try:
        resp = _anon_session.get(json_url, timeout=15)
        if resp.status_code != 200:
            return None
        return _case_law_text_record(resp.json(), fallback_cite, json_url)
    except Exception as exc:
        print(f"[case.law-text] JSON fetch failed for {json_url}: {exc}")
        return None


def _case_law_json_case(cite: str) -> "Optional[tuple[str, str]]":
    """Fetch and parse static.case.law's per-case JSON for *cite*, returning
    ``(opinion_text, bluebook_citation)`` or None.

    The Bluebook citation is built from the JSON's own authoritative fields —
    the abbreviated case name, the official reporter cite, and the deciding
    court and year — never from the brief's prose.  The text is the head-matter
    (parties, docket, panel) followed by every opinion body."""
    data = _case_law_metadata(cite)
    if not data:
        return None

    record = _case_law_text_record(data, cite)
    return (record.text, record.citation) if record is not None else None


def _link_name(text: str) -> str:
    """The case name in a cited-case hyperlink's text ("Calder v. Bull, 3 Dall.
    386, 388 (1798)" → "Calder v. Bull"), or "" — used to locate the case on
    CourtListener by name when its citation can't be parsed (so a blocked
    Scholar link still falls back).  Only a two-party caption or an "In re" /
    "Ex parte" form is returned, never a bare prose fragment."""
    t = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", text or "")).strip()
    m = re.search(r",?\s+\d{1,4}\s+[A-Za-z]", t)  # cut at the reporter citation
    if m:
        t = t[:m.start()]
    t = t.strip(" ,;.")
    if re.search(r"\bvs?\.?\b", t, re.IGNORECASE) or re.match(
        r"(?i)(?:in\s+re|ex\s+parte)\b", t
    ):
        return t
    return ""


def _gather_all_citations(client, item: dict) -> list[str]:
    """Every citation known for a case: the search-result cite(s) plus the
    cluster record's parallel cites (de-duplicated, HTML-stripped).

    Early Supreme Court results frequently carry only a nominative-reporter
    cite (e.g. "19 How. 393"); the parallel "U.S." cite that locates the
    official PDF lives on the cluster.  Likewise Federal Reporter cases may
    expose only one of several parallel cites.  Trying them all — rather than
    just the first — is what lets the PDF resolver succeed for these."""
    out: list[str] = []
    seen: set[str] = set()

    def add(c) -> None:
        c = re.sub(r"<[^>]+>", "", str(c)).strip()
        if c and c not in seen:
            seen.add(c)
            out.append(c)

    raw = item.get("citation", [])
    for c in (raw if isinstance(raw, list) else [raw] if raw else []):
        add(c)
    cluster_id = item.get("cluster_id") or item.get("id")
    if cluster_id:
        try:
            cr = client.get_cluster(int(cluster_id), fields="citations")
            for c in (cr.get("citations") or []):
                if isinstance(c, str):
                    add(c)
                elif isinstance(c, dict):
                    v, r, p = c.get("volume"), c.get("reporter"), c.get("page")
                    if v and r and p:
                        add(f"{v} {r} {p}")
        except Exception as exc:
            print(f"[resolve] cluster citation fetch failed: {exc}")
    return out


# Federal Appendix reporter: "F. App'x", "F.App'x", "Fed. Appx.", etc.
# (straight or typographic apostrophe).  These cases are scans — Google Scholar
# rarely has the text — so the app opens them straight on the official PDF.
_FED_APPX_RE = re.compile(r"F(?:ed)?\.?\s*App['’]?x\.?", re.IGNORECASE)


def _item_is_fed_appx(item: dict) -> bool:
    """True if any citation on a search-result item is to the Federal Appendix."""
    return _fed_appx_cite(item) is not None


def _fed_appx_cite(item: dict) -> Optional[str]:
    """The Federal Appendix citation on an item (HTML-stripped), or None."""
    raw = (item or {}).get("citation")
    cites = raw if isinstance(raw, list) else [raw] if raw else []
    for c in cites:
        if _FED_APPX_RE.search(str(c)):
            return re.sub(r"<[^>]+>", "", str(c)).strip()
    return None


# Text that can sit between a Scholar-linked case name and its citation while
# still being part of the same reference: parallel reporter cites, commas, an
# "at" pin, a trailing year — citation scaffolding, never prose.
_CITE_CONNECTIVE_RE = re.compile(
    r"^[\s,;.]*"
    r"(?:(?:and\s+)?\(?\d{1,4}\)?\s+[A-Za-z][A-Za-z.'’ ]{0,20}?\s+\d{1,5}[a-z]?"
    r"(?:\s*,\s*\d{1,5}(?:\s*[-–]\s*\d{1,5})?)?[\s,;.]*)*"
    r"(?:at\s+\d{1,5}[\s,;.]*)?"
    r"\s*$"
)
_CITE_YEAR_RE = re.compile(r"\s*\(\d{4}[a-z]?\)")


def _recap_cite_key(cite: str) -> str:
    """Stable lookup key for a WL/LEXIS citation."""
    return re.sub(r"[^a-z0-9]", "", (cite or "").lower())


def _recap_spec_index(text: str) -> dict[str, str]:
    """WL/LEXIS cite → merged RECAP spec for a whole opinion.

    The brief detector carries a docket printed in the first full citation to
    later short-form occurrences.  Building this once per opinion preserves
    that behavior even though the opinion reader renders one block at a time.
    """
    out: dict[str, str] = {}
    for start, end, spec in _iter_recap_cites(text or ""):
        if spec:
            out[_recap_cite_key(text[start:end])] = spec
    return out


def _recap_citation_ranges(
    text: str, spec_index: Optional[dict[str, str]] = None,
) -> list[tuple[int, int, tuple[str, str]]]:
    """RECAP link ranges in one rendered block, using whole-opinion context."""
    out: list[tuple[int, int, tuple[str, str]]] = []
    index = spec_index or {}
    for start, end, spec in _iter_recap_cites(text or ""):
        spec = spec or index.get(_recap_cite_key(text[start:end]))
        if spec:
            out.append((start, end, ("recap", spec)))
    for start, end, spec in _iter_docket_cites(text or ""):
        out.append((start, end, ("recap", spec)))
    return out


def _special_citation_ranges(
    spans, recap_spec_index: Optional[dict[str, str]] = None,
) -> "list[tuple[int, int, tuple[str, str]]]":
    """Character ranges in a block's concatenated span text that should render
    as a single one of *our* links instead of Google Scholar's, paired with the
    action to use:

      * English Reports cites in our index → ("engrep", spec) — the CommonLII scan
      * Federal Appendix cites             → ("cite", cite)   — the case.law PDF
      * unpublished federal cases          → ("recap", spec)  — RECAP/docket

    Each such cite is extended forward over a trailing "(year)" and backward over
    any parallel citations to a Scholar-hyperlinked case name, so the whole
    reference (case name, parallel cites, the cite) becomes our link and no
    Scholar link survives inside it.  Sorted, non-overlapping (start, end, action)."""
    text = "".join(s.text for s in spans)
    if not text:
        return []
    # Maximal runs of consecutive spans that share one Scholar hyperlink — these
    # mark the case name (Scholar wraps the name, sometimes split across spans
    # by its own italics, in a single <a> to the cited case).
    linked: list[tuple[int, int]] = []
    pos = 0
    run_start = None
    run_link = ""
    for s in spans:
        lk = s.link or ""
        if not (lk and lk == run_link):
            if run_link:
                linked.append((run_start, pos))
            run_link = lk
            run_start = pos if lk else None
        pos += len(s.text)
    if run_link:
        linked.append((run_start, pos))

    # The cites we link ourselves, as (start, end, action).
    targets: list[tuple[int, int, tuple[str, str]]] = []
    targets.extend(_recap_citation_ranges(text, recap_spec_index))
    for m in eng_rep.ER_CITE_RE.finditer(text):
        spec = eng_rep.cite_spec(m)
        if eng_rep.resolve(spec):  # only cases we actually have
            targets.append((m.start(), m.end(), ("engrep", spec)))
    for s, e, spec, _cases in eng_rep.iter_nominate_cites(text):
        targets.append((s, e, ("engrep", spec)))  # gated on the index already
    if _FED_APPX_RE.search(text):  # cheap guard before the full reporter scan
        for m in _TEXT_CITE_RE.finditer(text):
            cite = re.sub(r"\s+", " ", m.group(0)).strip()
            if _FED_APPX_RE.search(cite) and _static_case_law_url(cite):
                targets.append((m.start(), m.end(), ("cite", cite)))

    out: list[tuple[int, int, tuple[str, str]]] = []
    for es, ee, action in targets:
        ym = _CITE_YEAR_RE.match(text, ee)
        if ym:
            ee = ym.end()
        start = es
        for ls, le in linked:
            if ls <= es < le:                 # cite sits inside the linked name
                start = min(start, ls)
                ee = max(ee, le)
            elif le <= es and _CITE_CONNECTIVE_RE.match(text[le:es]):
                start = min(start, ls)        # linked name precedes, cites between
        out.append((start, ee, action))
    out.sort(key=lambda r: (r[0], r[1]))
    merged: list[tuple[int, int, tuple[str, str]]] = []
    for s, e, action in out:
        if merged and s <= merged[-1][1]:
            ps, pe, pa = merged[-1]
            merged[-1] = (ps, max(pe, e), pa)
        else:
            merged.append((s, e, action))
    return merged


def _item_from_cluster(cluster: dict) -> dict:
    """A search-result-shaped item from a citation-lookup OpinionCluster,
    carrying just the fields the result rows and PDF resolver need."""
    cites: list[str] = []
    for c in cluster.get("citations") or []:
        if isinstance(c, str):
            cites.append(c)
        elif isinstance(c, dict):
            v, r, p = c.get("volume"), c.get("reporter"), c.get("page")
            if v and r and p:
                cites.append(f"{v} {r} {p}")
    short_name = cluster.get("case_name") or ""
    full_name = cluster.get("case_name_full") or ""
    item: dict = {
        "cluster_id": cluster.get("id"),
        "caseName": short_name or full_name,
        "case_name": short_name,
        "case_name_full": full_name,
        "citation": cites,
        "dateFiled": cluster.get("date_filed") or "",
    }
    court = cluster.get("court_id") or cluster.get("court") or ""
    if isinstance(court, str) and court and "/" not in court:
        item["court_id"] = court
    return item


def _cl_item_for_citation(client, cite: str, name: str = "") -> Optional[dict]:
    """Resolve a reporter citation to the CourtListener cluster that actually
    bears it, as a search-result-shaped item (or ``None``).

    CourtListener's full-text search ranks by relevance and very often returns
    the wrong case first for a bare citation — ``citation:(410 U.S. 113)``
    surfaces a case that merely *cites* Roe, not Roe itself.  So resolve through
    the citation-lookup endpoint (exact), and fall back to full-text search only
    if that finds nothing — and even then accept a result only when its own
    citations include the requested one, rather than blindly taking the first.
    Returning ``None`` (so the caller reports "not found") is preferable to
    opening the wrong case.

    A citation can be *ambiguous*: CourtListener answers "1 Cranch 299" with
    status 300 and two clusters, because the D.C. Circuit's Cranch reports are
    normalized to the same form — Stuart v. Laird (5 U.S. 299, 1803) and
    Wiggins v. Wiggins (1806) both "bear" it.  Rather than skipping those (and
    falling into the even less careful full-text search), the candidates are
    scored: the cluster also bearing the citation's modern U.S.-Reports
    parallel wins, then a match on the case ``name`` the caller knows, then
    the Supreme Court for a nominative SCOTUS cite."""
    cite = (cite or "").split("@", 1)[0].strip()
    if not cite:
        return None
    lookup_cites = _citation_search_variants(cite)
    want_keys = {
        re.sub(r"\s+", "", variant).lower()
        for variant in lookup_cites
    }
    alt = _us_reports_cite(cite)
    altkey = re.sub(r"\s+", "", alt).lower() if alt else ""

    def norm_cites(raw) -> set[str]:
        out: set[str] = set()
        for c in raw or []:
            if isinstance(c, dict):
                v, r, p = c.get("volume"), c.get("reporter"), c.get("page")
                if v and r and p:
                    out.add(re.sub(r"\s+", "", f"{v}{r}{p}").lower())
            else:
                out.add(re.sub(r"\s+", "", re.sub(r"<[^>]+>", "", str(c))).lower())
        return out

    def score(case_name: str, cites: set[str], court: str) -> int:
        s = 0
        if altkey and altkey in cites:
            s += 4
        if name and _name_match_score(name, case_name or "") >= _NAME_MATCH_MIN:
            s += 2
        if altkey and (court or "").lower() == _SCOTUS_COURT_ID:
            s += 1
        return s

    # 1) Resolution via the citation-lookup endpoint (exact; 300 = ambiguous).
    # Try the form the user entered first.  A Cranch citation can also denote a
    # D.C. Circuit reporter, so consulting its U.S.-Reports counterpart is a
    # fallback rather than an unconditional preference.
    for lookup_cite in lookup_cites:
        try:
            clusters = []
            for entry in client.lookup_citation(lookup_cite):
                if entry.get("status") in (200, 300):
                    clusters.extend(entry.get("clusters") or [])
            if clusters:
                best = max(
                    clusters,
                    key=lambda cl: score(
                        cl.get("case_name") or cl.get("case_name_full") or "",
                        norm_cites(cl.get("citations")),
                        str(cl.get("court_id") or cl.get("court") or ""),
                    ),
                )
                item = _item_from_cluster(best)
                if item.get("cluster_id"):
                    return item
        except Exception as exc:
            print(f"[cl-cite] citation-lookup failed for {lookup_cite!r}: {exc}")

    # 2) Fall back to full-text search, trusting only a real citation match
    # (and preferring the same disambiguation signals when several match).
    for lookup_cite in lookup_cites:
        matched = []
        seen_items: set[str] = set()
        for q in (f"citation:({lookup_cite})", f'"{lookup_cite}"'):
            try:
                results = client.search(q, type="o", page_size=5).get("results") or []
            except Exception:
                continue
            for item in results:
                if not (want_keys & norm_cites(item.get("citation"))):
                    continue
                key = str(item.get("cluster_id") or item.get("id") or id(item))
                if key not in seen_items:
                    seen_items.add(key)
                    matched.append(item)
        if matched:
            return max(
                matched,
                key=lambda it: score(
                    re.sub(r"<[^>]+>", "",
                           it.get("caseName") or it.get("case_name") or ""),
                    norm_cites(it.get("citation")),
                    str(it.get("court_id") or ""),
                ),
            )
    return None


def _us_reports_cite_via_courtlistener(
    client, cites: list[str]
) -> Optional[str]:
    """The U.S. Reports parallel cite ("410 U.S. 113") for a SCOTUS opinion that
    only carries a Supreme Court Reporter ("S. Ct.") cite, looked up on
    CourtListener; ``None`` when a U.S. cite is already present, there's no
    S. Ct. cite to resolve, or CourtListener knows no U.S. parallel.

    Google Scholar frequently prints only the S. Ct. cite for a Supreme Court
    case ("93 S. Ct. 705", no "410 U.S. 113"), which leaves the official-PDF
    paths — all keyed on the U.S. Reports cite — with nothing to work from.
    CourtListener's citation-lookup resolves the S. Ct. cite to the cluster that
    bears it (via :func:`_cl_item_for_citation`, which only accepts a cluster
    whose own citations include the one asked for), and that cluster's parallel
    citations carry the U.S. cite.  Returning it lets the caller pull the
    official opinion scan behind the "View PDF" button."""
    if client is None:
        return None
    # A U.S. Reports cite is already in hand — the PDF paths can use it directly.
    if any(_US_CITE_RE.search(c) for c in cites):
        return None
    sct = next((c for c in cites if _SCT_CITE_RE.search(c)), None)
    if not sct:
        return None
    try:
        item = _cl_item_for_citation(client, sct)
    except Exception as exc:
        print(f"[resolve] S. Ct. -> U.S. lookup failed for {sct!r}: {exc}")
        return None
    for c in (item or {}).get("citation") or []:
        m = _US_CITE_RE.search(re.sub(r"<[^>]+>", "", str(c)))
        if m:
            return f"{m.group(1)} U.S. {m.group(2)}"
    return None


def _cl_item_for_name(client, name: str) -> Optional[dict]:
    """Resolve a case *name* to the best-matching CourtListener cluster as a
    search-result-shaped item (or None).  The fallback for locating a cited case
    when its citation can't be parsed or resolved — e.g. a Scholar hyperlink
    whose text is just the case name, followed when Google Scholar is blocked.
    Uses the same name ranker as the quick search, so only a strong match
    (two-party, or a distinctive party) is returned."""
    name = re.sub(r"<[^>]+>", "", name or "").strip()
    if not name:
        return None
    try:
        ranked = _cl_name_ranked_search(client, name)
    except Exception as exc:
        print(f"[cl-name] lookup failed for {name!r}: {exc}")
        return None
    return ranked[0][1] if ranked else None


# ======================================================================
# Spotlight name-ranked case search
# ======================================================================
# CourtListener's full-text search ranks by relevance and gives the case
# *name* no special weight over the body text, so a quick search for a case
# by name (e.g. "Pennoyer v. Neff") can bury the case itself under opinions
# that merely discuss it.  When the query is a name rather than a reporter
# citation, these helpers instead restrict the search to the caseName field,
# rank hits by how closely their names match the query, and surface the most
# authoritative close matches first — ordering by citation count (how often a
# case is cited), which stands in for court level as the authority signal, and
# adding a dedicated Supreme Court pass so a recent, as-yet-uncited SCOTUS
# decision isn't buried by that citation-count ordering.

_SCOTUS_COURT_ID = "scotus"

# A close name match must clear this score (see _name_match_score): a query
# whose single party fully matches a candidate party scores exactly 0.5, so
# "match one or both of the parties" is the floor for "reasonably close".
_NAME_MATCH_MIN = 0.5

# Articles, conjunctions, the "v.", and corporate/procedural boilerplate are
# dropped before comparing names so they don't dominate the token overlap.
_NAME_STOPWORDS = {
    "the", "of", "and", "a", "an", "in", "on", "for", "re", "ex", "parte",
    "matter", "v", "vs", "et", "al", "co", "cos", "corp", "inc", "ltd",
    "llc", "llp", "lp", "lllp", "plc", "company", "companies",
    "incorporated", "corporation", "no", "nos",
}

_NAME_PARTY_SPLIT_RE = re.compile(r"\s+v(?:s)?\.?\s+", re.IGNORECASE)

# A party that is *only* the United States abbreviation, in any spelling
# ("US", "U.S.", "U. S.", "USA", "U.S.A.").  Such a side stands for the United
# States as a party, so it is mapped to the spelled-out tokens and "U.S. v.
# Texas" matches "United States v. Texas".  Matched against the whole side, so
# an embedded "Chevron U.S.A. Inc." is left alone.
_US_PARTY_RE = re.compile(r"^\s*u\.?\s*s\.?\s*a?\.?\s*$", re.IGNORECASE)


def _name_tokens(name: str) -> list[str]:
    """Significant lowercased word tokens of a case (or party) name, with
    HTML, punctuation, articles, bare numbers and one-letter tokens removed."""
    name = re.sub(r"<[^>]+>", " ", name or "")
    name = re.sub(r"[^\w\s]", " ", name.lower())
    return [
        t for t in name.split()
        if len(t) > 1 and not t.isdigit() and t not in _NAME_STOPWORDS
    ]


def _name_parties(name: str) -> list[set[str]]:
    """Token sets for each side of a case name ("A v. B" → [{a}, {b}]).  A side
    that is just the United States abbreviation becomes {united, states} (see
    :data:`_US_PARTY_RE`)."""
    out: list[set[str]] = []
    for side in _NAME_PARTY_SPLIT_RE.split(name or "", maxsplit=1):
        toks = {"united", "states"} if _US_PARTY_RE.match(side) else set(_name_tokens(side))
        if toks:
            out.append(toks)
    return out


def _token_close(a: str, b: str) -> bool:
    """Two name tokens match when equal, one is a prefix of the other, or
    they are near-identical spellings (handles plurals and minor variants)."""
    if a == b:
        return True
    if len(a) >= 4 and len(b) >= 4 and (a.startswith(b) or b.startswith(a)):
        return True
    if len(a) >= 5 and len(b) >= 5:
        return difflib.SequenceMatcher(None, a, b).ratio() >= 0.85
    return False


def _is_acronym_of(acro: set[str], words: set[str]) -> bool:
    """True if a one-token party *acro* is the initialism of multi-word party
    *words*: "nrdc" ↔ {natural, resources, defense, council}, "fec" ↔ {federal,
    election, commission}, "cfpb" ↔ {consumer, financial, protection, bureau}.

    Lets an agency acronym match its spelled-out name.  The initials are
    compared as a sorted multiset (party word order is already lost to the set),
    so it also matches when the words are listed in another order; the exact
    letter-count match keeps an ordinary short word from matching a party with a
    different number of words."""
    if len(acro) != 1 or not (2 <= len(words) <= 6):
        return False
    (a,) = acro
    return 2 <= len(a) <= 6 and sorted(a) == sorted(w[0] for w in words)


def _party_overlap(query_party: set[str], cand_party: set[str]) -> float:
    """Fraction of a query party's tokens found (token-close) in a candidate
    party — how completely that side of the query name is present."""
    if not query_party:
        return 0.0
    # An agency-style acronym and its spelled-out name are a full match.
    if (_is_acronym_of(query_party, cand_party)
            or _is_acronym_of(cand_party, query_party)):
        return 1.0
    hit = sum(1 for t in query_party
              if any(_token_close(t, c) for c in cand_party))
    return hit / len(query_party)


# Frequent party names — every state, "United States", and the generic
# government plaintiffs — that are too common to identify a case on their own.
# A match on such a name *alone* (one side of the query) is the weakest kind of
# hit and is shown only when nothing better turns up; as one side of a genuine
# two-party match it still counts, so "U.S. v. Texas" matches "United States v.
# Texas".  Keyed by the party's token set so "New York" → {new, york} is
# recognized while a distinctive name that merely contains a state word
# ("Texas Instruments" → {texas, instruments}) is not.
_COMMON_PARTY_NAMES: set[frozenset[str]] = {
    frozenset(_name_tokens(_n)) for _n in (
        [_state for _state, _courts in _STATE_COURTS]
        + ["United States", "United States of America",
           "People", "State", "Commonwealth"]
    )
}
_COMMON_PARTY_NAMES.discard(frozenset())


def _is_common_party(party: set[str]) -> bool:
    """True when *party* is a frequent name (a state, "United States", or a
    generic government plaintiff) — see :data:`_COMMON_PARTY_NAMES`."""
    return frozenset(party) in _COMMON_PARTY_NAMES


# A real party name is short.  A consolidated caption merges dozens of parties
# into one side ("Foltz, Consumer Action, United Policyholders, Texas Watch … v.
# State Farm, …"); such a giant blob contains so many tokens it matches almost
# any query, so a side longer than this is not treated as a clean party.
_MAX_CLEAN_PARTY = 8

# A reverse-caption match (parties on swapped sides — tier 2) is normally
# outranked by the case as typed (tier 3) and dropped when a tier-3 match
# exists.  But a reverse-caption case cited at least this often is a major
# precedent in its own right and is shown alongside the tier-3 match.
_REVERSED_PARTY_MIN_CITES = 1000


def _name_match_score(query: str, candidate: str) -> float:
    """Closeness (0..1) of a candidate case name to the query name, scored on
    how well the query's parties match the candidate's.  Matching one party
    well scores ~0.5; matching both scores near 1.0."""
    q_parties = _name_parties(query)
    c_parties = _name_parties(candidate)
    if not q_parties or not c_parties:
        # One-sided name ("In re Gault"), or an unparseable caption: compare
        # the whole token sets directly.
        q = set(_name_tokens(query))
        c = set(_name_tokens(candidate))
        return _party_overlap(q, c) if q else 0.0
    per_side = [max(_party_overlap(qp, cp) for cp in c_parties)
                for qp in q_parties]
    avg = sum(per_side) / len(per_side)
    # Reward a both-sides hit so an exact "A v. B" outranks a one-party match.
    bonus = 0.15 if sum(1 for s in per_side if s >= 0.6) >= 2 else 0.0
    # Require at least one party to match solidly, so two weak half-matches
    # don't masquerade as a close hit.
    if max(per_side) < 0.6:
        return 0.0
    return min(1.0, avg + bonus)


def _match_tier(query: str, candidate: str) -> int:
    """How strongly *candidate*'s name matches *query*, as a coarse priority
    tier (higher wins); only the best tier present is shown — see
    :func:`_filter_to_best_tier`:

      3 — both parties match on the *same* sides as the query ("Roe v. Wade" →
          "Roe v. Wade"): the case as captioned; frequent names count here, so
          "U.S. v. Texas" matches "United States v. Texas".
      2 — both parties match but on *swapped* sides ("Roe v. Wade" → "Wade v.
          Roe"): usually a related or reverse-caption case.
      1 — exactly one party matches and it is distinctive.
      0 — the only matching party is a frequent name (a state, "United States",
          a generic government plaintiff), which alone is too weak to mean much
          — shown only when nothing better was found.

    Returns -1 when the names don't match (the 0.6 per-side floor mirrors
    :func:`_name_match_score`)."""
    q_parties = _name_parties(query)
    c_parties = _name_parties(candidate)
    if not q_parties or not c_parties:
        # One-sided name ("In re Gault") or unparseable caption: a whole-set
        # overlap stands in, treated as a distinctive one-party hit.
        q = set(_name_tokens(query))
        c = set(_name_tokens(candidate))
        return 1 if q and _party_overlap(q, c) >= 0.6 else -1

    def m(qp: set[str], cp: set[str]) -> bool:
        return len(cp) <= _MAX_CLEAN_PARTY and _party_overlap(qp, cp) >= 0.6

    # Two-party match: the parties must land on *different* candidate sides
    # (requiring opposite sides stops both matching inside one long name —
    # "United States Dist. Court for W. Texas" is not "U.S. v. Texas").  Same
    # orientation as the query (tier 3) beats the swapped caption (tier 2).
    if len(q_parties) == 2 and len(c_parties) == 2:
        (qa, qb), (ca, cb) = q_parties, c_parties
        if m(qa, ca) and m(qb, cb):
            return 3
        if m(qa, cb) and m(qb, ca):
            return 2

    matched = [qp for qp in q_parties if any(m(qp, cp) for cp in c_parties)]
    if not matched:
        return -1
    # One side matched (or both, but on the same candidate party): distinctive
    # unless every matched party is a frequent name.
    return 0 if all(_is_common_party(qp) for qp in matched) else 1


def _filter_to_best_tier(query: str,
                         tagged: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    """Across all sources, keep only the best match tier present (see
    :func:`_match_tier`): the case as captioned if found, else the swapped
    caption, else distinctive one-party matches, else frequent-name ones.
    Applied to the combined results so a strong hit found by *any* pass
    suppresses the weaker fillers of *every* pass — otherwise the Supreme Court
    pass, searching by relevance, pads a clean query with one-sided state-name
    matches ("Smith v. Arizona" for "Miranda v. Arizona").

    One exception: when the best tier is the as-captioned match (3), a
    swapped-caption match (2) that is itself heavily cited
    (:data:`_REVERSED_PARTY_MIN_CITES`) is kept alongside it — a major
    precedent shouldn't vanish just because its caption runs the other way."""
    rated = [
        (_match_tier(query, re.sub(
            r"<[^>]+>", "",
            it.get("caseName") or it.get("case_name") or "").strip()),
         bucket, it)
        for bucket, it in tagged
    ]
    best = max((t for t, _b, _it in rated), default=-1)
    out: list[tuple[str, dict]] = []
    for t, bucket, it in rated:
        if t == best:
            out.append((bucket, it))
        elif (best == 3 and t == 2
              and (it.get("citeCount") or 0) >= _REVERSED_PARTY_MIN_CITES):
            # Re-tag the heavily-cited swapped-caption match into its own bucket
            # so the as-captioned matches don't crowd it out of the display.
            out.append(("reversed", it))
    return out


def _case_signature(name: str, cite: str, year: str, court: str = "",
                    *, include_name: bool = True) -> dict:
    """Structured identity of a case for spotlight de-duplication:

    * ``cites``  — exact reporter citations as (series, volume, page) tuples;
    * ``series`` — the reporter series those citations belong to;
    * ``nk``     — a name key (order-insensitive party tokens + the decision
      year), or ``None`` when the name is suppressed (a reverse-caption match);
    * ``court``  — the normalized court id, or "" when the source doesn't say.

    Reporter spelling is normalized ("410 U.S. 113" and "410 US 113" collapse),
    and a citation string carrying parallel cites ("59 N.W.2d 336, 157 Neb.
    226") contributes all of them.  :func:`_same_case` compares two signatures."""
    cites: set[tuple[str, str, str]] = set()
    series: set[str] = set()
    for part in re.split(r"[;,]", re.sub(r"<[^>]+>", "", cite or "")):
        m = _CITE_PARSE_RE.match(part.strip())
        if m:
            rep = re.sub(r"[^a-z0-9]", "", m.group(2).lower())
            if rep:
                cites.add((rep, m.group(1), m.group(3)))
                series.add(rep)
    nk = None
    if include_name:
        toks = _name_tokens(name)
        if toks:
            nk = "n:" + " ".join(sorted(set(toks)))
            yr = re.sub(r"\D", "", year or "")
            if yr:
                nk += ":" + yr
    return {
        "nk": nk, "cites": cites, "series": series,
        "court": re.sub(r"[^a-z0-9]", "", (court or "").lower()),
    }


def _same_case(a: dict, b: dict) -> bool:
    """Whether two case signatures (see :func:`_case_signature`) denote one
    case.  An exact shared reporter citation settles it outright.  Otherwise the
    names-with-year must match *and* the two must not actively disagree: two
    known-but-different courts, or two distinct citations within the same
    reporter series, mark genuinely different cases (a common caption like
    "McGuire v. McGuire" recurs across jurisdictions and even years).  A court
    known to one side but not the other is not a disagreement — the cases may
    still be one, so name-and-year still merges them.  Parallel citations in
    different series ("59 N.W.2d 336" and "157 Neb. 226") do not disagree, so a
    case reported two ways still collapses to one row."""
    if a["cites"] & b["cites"]:
        return True
    if a["nk"] and a["nk"] == b["nk"]:
        if a["court"] and b["court"] and a["court"] != b["court"]:
            return False
        if a["series"] & b["series"]:
            return False
        return True
    return False


# How many *distinct* cases each spotlight source/court-tier may display.  A
# source over-fetches a couple of spare candidates beyond its cap so that, when
# one of its results duplicates a case already shown by another source, the
# next-best result takes the slot and the source still shows its full quota.
_BUCKET_CAPS: dict[str, int] = {
    "scholar": 4,     # Google Scholar (best of the first results page)
    "opiniondb": 3,   # already-saved Google Scholar opinions
    "exact": 3,       # strict AND match of two distinctive parties
    "ranked": 4,      # CourtListener name matches, ranked by citation count
    "reversed": 2,    # heavily-cited swapped-caption ("Texas v. United States")
    "scotus": 3,      # dedicated Supreme Court pass (catches recent/uncited)
    "juris": 3,       # a single jurisdiction named in the query
    "cl": 3,          # CourtListener citation-lookup results
    "engrep": 1,      # English Reports
}


def _prefer_source(new_bucket: str, shown_bucket: str) -> bool:
    """Whether a *new* result should take over an already-shown duplicate from
    *shown_bucket*.  The Google Scholar opinion page is the app's first-choice
    source, so a Scholar hit replaces a CourtListener row for the same case
    rather than being dropped; every other collision keeps the row already
    shown (first arrival wins)."""
    scholar_sources = {"scholar", "opiniondb"}
    return (new_bucket in scholar_sources
            and shown_bucket not in scholar_sources)


def _is_scotus_order_item(item: dict) -> bool:
    """True for a SCOTUS "order" entry — a docket/order with no real opinion —
    which the main search routes out of the primary results and the spotlight
    likewise drops.  A genuine opinion is recognized by either having been
    cited by other cases (the top-level ``citeCount``) or citing cases itself
    (its main opinion's outbound ``cites``); an order does neither.

    The inbound ``citeCount`` check is essential: CourtListener leaves the
    outbound ``cites`` array empty in *search* payloads even for foundational
    opinions (Marbury v. Madison, cited ~6000 times, comes back with
    ``cites == []``), so keying on outbound cites alone wrongly discards them —
    which made name searches for such cases return nothing at all."""
    court_val = str(item.get("court_id") or item.get("court") or "")
    if "scotus" not in court_val.lower():
        return False
    if (item.get("citeCount") or 0) > 0:
        return False
    opinions = item.get("opinions") or []
    main_op = max(opinions, key=lambda o: len(o.get("cites") or []),
                  default=None)
    cites_count = len(main_op.get("cites") or []) if main_op else 0
    return cites_count <= 2


# --- Jurisdiction hint parsing ----------------------------------------------
# A query may pin the court itself — "Doe v. Roe (7th Cir. 2009)" — in which
# case the search is aimed straight at that jurisdiction.  Only a court hint
# set off from the name (a trailing parenthetical, a trailing clause after a
# comma, or a bare trailing "Nth Cir.") is recognized, so an ordinary party
# name is never mistaken for a jurisdiction.

_CIRCUIT_ORDINAL_IDS = {
    "1st": "ca1", "2d": "ca2", "2nd": "ca2", "3d": "ca3", "3rd": "ca3",
    "4th": "ca4", "5th": "ca5", "6th": "ca6", "7th": "ca7", "8th": "ca8",
    "9th": "ca9", "10th": "ca10", "11th": "ca11",
    "first": "ca1", "second": "ca2", "third": "ca3", "fourth": "ca4",
    "fifth": "ca5", "sixth": "ca6", "seventh": "ca7", "eighth": "ca8",
    "ninth": "ca9", "tenth": "ca10", "eleventh": "ca11",
}

# Reverse lookup of a federal district court's Bluebook abbreviation, with
# periods/spaces removed ("S.D.N.Y." → "sdny", "N.D. Cal." → "ndcal").
_DISTRICT_ABBR_IDS = {
    abbr.replace(".", "").replace(" ", "").lower(): cid
    for cid, abbr in _DISTRICT_COURTS.items()
}

# State name → that state's court list, and every state court's Bluebook
# abbreviation (all levels) → its CourtListener court id, for resolving a
# state court hint ("Cal." → cal, "Cal. Ct. App." → calctapp).
_STATE_NAME_COURTS = {state.lower(): courts for state, courts in _STATE_COURTS}
_STATE_COURT_ABBR_IDS = {
    abbr.replace(".", "").replace(" ", "").lower(): cid
    for _state, courts in _STATE_COURTS for cid, abbr, _label in courts
}

_CIRCUIT_HINT_RE = re.compile(
    r"(?P<ord>\d{1,2}(?:st|nd|rd|d|th)|first|second|third|fourth|fifth|"
    r"sixth|seventh|eighth|ninth|tenth|eleventh)\s+cir(?:cuit)?\.?",
    re.IGNORECASE,
)
_DC_CIRCUIT_HINT_RE = re.compile(r"\bd\.?\s*c\.?\s+cir(?:cuit)?\.?", re.IGNORECASE)
_FED_CIRCUIT_HINT_RE = re.compile(r"\bfed(?:eral)?\.?\s+cir(?:cuit)?\.?",
                                  re.IGNORECASE)
_SCOTUS_HINT_RE = re.compile(
    r"\b(?:scotus|u\.?\s*s\.?\s+supreme\s+court|united\s+states\s+supreme\s+"
    r"court|supreme\s+court\s+of\s+the\s+united\s+states)\b",
    re.IGNORECASE,
)
_YEAR_TAIL_RE = re.compile(r"[\s,]*(?:19|20)\d{2}\s*$")


def _classify_court_hint(hint: str) -> Optional[tuple[str, str]]:
    """Resolve a court-hint string ("7th Cir.", "S.D.N.Y.", "Cal.") to
    (space-separated court ids, label), or None when it names no known court."""
    h = _YEAR_TAIL_RE.sub("", (hint or "").strip()).strip(" ,.;()[]")
    if not h:
        return None
    low = h.lower()

    if _SCOTUS_HINT_RE.search(low):
        return _SCOTUS_COURT_ID, "U.S. Supreme Court"
    m = _CIRCUIT_HINT_RE.search(low)
    if m:
        cid = _CIRCUIT_ORDINAL_IDS.get(m.group("ord").lower())
        if cid:
            return cid, _CIRCUIT_COURTS.get(cid, cid)
    if _DC_CIRCUIT_HINT_RE.search(low):
        return "cadc", _CIRCUIT_COURTS["cadc"]
    if _FED_CIRCUIT_HINT_RE.search(low):
        return "cafc", _CIRCUIT_COURTS["cafc"]

    # Federal district court, by its Bluebook abbreviation.
    key = low.replace(".", "").replace(" ", "")
    cid = _DISTRICT_ABBR_IDS.get(key)
    if cid:
        return cid, _DISTRICT_COURTS[cid]

    # State court by its Bluebook abbreviation, any level ("Cal." → cal,
    # "Cal. Ct. App." → calctapp, "Tex. App." → texapp).
    cid = _STATE_COURT_ABBR_IDS.get(key)
    if cid:
        return cid, _COURT_BLUEBOOK.get(cid, cid)

    # State named in full ("California", "California Court of Appeal"):
    # identify the state, then classify the specific court it names.
    for state_low, courts in _STATE_NAME_COURTS.items():
        if low == state_low or low.startswith(state_low + " "):
            cid = _classify_state_court(low, courts)
            return cid, _COURT_BLUEBOOK.get(cid, cid)
    return None


def _detect_jurisdiction(query: str) -> Optional[tuple[str, str, str]]:
    """If *query* pins a court — "Doe v. Roe (7th Cir. 2009)", "Smith, 9th
    Cir." — return (court_ids, name_without_hint, label).  None otherwise."""
    q = (query or "").strip()

    # 1. A trailing parenthetical: "... (7th Cir. 2009)".
    m = re.search(r"[(\[]([^)\]]*)[)\]]\s*$", q)
    if m:
        hit = _classify_court_hint(m.group(1))
        if hit:
            name = q[:m.start()].strip(" ,;-–—")
            return hit[0], name, hit[1]

    # 2. A trailing clause after the last comma: "..., 9th Cir.".
    if "," in q:
        head, tail = q.rsplit(",", 1)
        if tail.strip() and len(tail.split()) <= 4:
            hit = _classify_court_hint(tail)
            if hit:
                return hit[0], head.strip(" ,;-–—"), hit[1]

    # 3. A bare trailing circuit, with no delimiter: "... 7th Cir.".
    for rx in (_CIRCUIT_HINT_RE, _DC_CIRCUIT_HINT_RE, _FED_CIRCUIT_HINT_RE):
        m = rx.search(q)
        if m and _YEAR_TAIL_RE.sub("", q[m.end():]).strip() == "":
            hit = _classify_court_hint(q[m.start():])
            if hit:
                return hit[0], q[:m.start()].strip(" ,;-–—"), hit[1]
    return None


# --- Name-restricted CourtListener search -----------------------------------

def _cl_casename_query(name: str, *, strict: bool = False) -> str:
    """A flexible ``caseName`` query that retrieves a candidate when a
    *distinctive* party of an "A v. B" name is present.

    CourtListener's ``caseName`` field is AND-by-default, so the obvious
    ``caseName:(chevron nrdc)`` finds *nothing* for "Chevron v. NRDC": the
    stored caption is "Chevron U.S.A. Inc. v. Natural Resources Defense
    Council, Inc.", which contains "chevron" but not the abbreviation "nrdc".
    Instead each distinctive party's tokens are AND'd within their own
    ``caseName`` group and the groups are OR'd, so an opinion matching one side
    is still retrieved; ranking then sorts the genuinely relevant ones up.

    A frequent party (a state, "United States" — see
    :data:`_COMMON_PARTY_NAMES`) is deliberately left out of the OR: it is a
    party in an unmanageable number of cases ("United States v. …" alone is
    hundreds of thousands), so OR-ing it would crowd the actual case off the
    page.  Its presence is confirmed afterwards by :func:`_match_tier`.  Only
    when *every* party is a frequent name ("U.S. v. Texas") are all parties
    AND'd together — their combination is specific enough to find the case.

    With ``strict``, the distinctive parties are AND'd into a single group
    rather than OR'd — a precise query that pins the exact two-party case even
    when it is too lightly cited to surface among the broad OR results, and
    that leans on CourtListener's own acronym expansion ("FEC v. Cruz" →
    ``caseName:(cruz fec)`` finds "Ted Cruz for Senate v. Federal Election
    Commission")."""
    parties = _name_parties(name)
    distinctive = [p for p in parties if not _is_common_party(p)]
    if distinctive and not strict:
        return " OR ".join(
            f"caseName:({' '.join(sorted(p))})" for p in distinctive
        )
    pool = distinctive or parties
    if pool:
        return f"caseName:({' '.join(sorted(set().union(*pool)))})"
    toks = _name_tokens(name)
    return f"caseName:({' '.join(toks)})" if toks else (name or "").strip()


def _cl_name_search(client, name: str, court_ids: Optional[str], *,
                    page_size: int = 20, limit: int = 3, spare: int = 0,
                    drop_scotus_orders: bool = False,
                    order_by_citecount: bool = False,
                    strict: bool = False) -> list[dict]:
    """Search a (set of) court(s) for a case *name*, restricting the query to
    the caseName field (matching either party — see :func:`_cl_casename_query`),
    then return the items whose names are reasonably close to *name*, best
    first.  Up to ``limit + spare`` are returned: the caller displays ``limit``
    of them and keeps the spares to replace any that turn out to duplicate a
    case already shown by another source.

    ``order_by_citecount`` controls which candidates are *fetched* (the API
    returns one page): when set, the most-cited matches are retrieved, which is
    essential for the either-party search — the canonical case (e.g. Chevron)
    often matches only one party and would otherwise fall outside the page,
    while its huge citation count pulls it in.  When unset (the Supreme Court
    pass), the API's relevance order is used so a recent, as-yet-uncited
    decision is still fetched.  ``strict`` AND's the distinctive parties into
    one precise query (see :func:`_cl_casename_query`) to pin a lightly-cited
    exact case."""
    q = _cl_casename_query(name, strict=strict)
    if not q:
        return []
    extra = {"order_by": "citeCount desc"} if order_by_citecount else None
    try:
        data = client.search(q, type="o", court=court_ids or None,
                             page_size=page_size, extra=extra)
        results = data.get("results") or []
    except Exception as exc:
        print(f"[cl-name] search failed for court={court_ids!r}: {exc}")
        return []
    if drop_scotus_orders:
        results = [it for it in results if not _is_scotus_order_item(it)]

    scored: list[tuple[int, float, int, dict]] = []
    for it in results:
        cand = re.sub(
            r"<[^>]+>", "",
            it.get("caseName") or it.get("case_name") or "",
        ).strip()
        score = _name_match_score(name, cand)
        if score >= _NAME_MATCH_MIN:
            scored.append((_match_tier(name, cand),
                           score, it.get("citeCount") or 0, it))
    # Sort by match tier first (as-captioned over swapped over one-party over
    # frequent-name — see _match_tier), so a stronger match is never crowded out
    # of the page by a more-cited but weaker one; then by closeness, then by
    # citation count (the authority signal that stands in for walking the court
    # hierarchy, so "Brown v. Board of Education", cited thousands of times,
    # outranks a one-off "Board of Education v. Brown").  The caller's
    # _filter_to_best_tier then drops the lower tiers once every source's
    # results are pooled.
    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    kept = scored[:limit + spare]
    # Carry through any heavily-cited swapped-caption (tier 2) match the page cap
    # cut, so a major reverse-caption precedent reaches _filter_to_best_tier even
    # when the as-captioned matches fill the page (e.g. six "United States v. …
    # Texas" cases ahead of "Texas v. United States").
    kept += [t for t in scored[limit + spare:]
             if t[0] == 2 and t[2] >= _REVERSED_PARTY_MIN_CITES]
    return [it for _tier, _score, _cites, it in kept]


def _cl_name_ranked_search(client, query: str) -> list[tuple[str, dict]]:
    """Name-ranked CourtListener results for a quick-search *query* that is a
    case name (not a reporter citation), as ``(bucket, item)`` pairs whose
    bucket carries each result's display cap (see ``_BUCKET_CAPS``).

    When the query pins a jurisdiction ("... (7th Cir. 2009)"), the best name
    matches in that court are returned.  Otherwise these passes run in parallel:

    * ``exact`` — only when the query names two distinctive parties: an AND of
      both, which pins the exact case even when it is too lightly cited to
      surface in the broad passes ("FEC v. Cruz", "NRDC v. EPA").
    * ``ranked`` — across all courts, retrieving the most-cited name matches
      and ordering them by closeness then citation count.  Citation count is
      the authority signal that replaces walking the court hierarchy, and it
      lets a case that matches only one party (e.g. Chevron) still surface.
    * ``scotus`` — the Supreme Court alone, by relevance, so a recent,
      as-yet-uncited SCOTUS decision (which the citation-count ordering would
      bury) is still shown.

    The groups are concatenated exact-, then ranked-, then scotus-first, and
    cross-source de-duplication keeps each later pass to the cases the earlier
    ones didn't already surface.  Each pass over-fetches a couple of spares so
    duplicates dropped during display can be replaced.  Finally the pooled
    results are reduced to their best match tier (see
    :func:`_filter_to_best_tier`), so one-party fillers are dropped whenever a
    genuine two-party match was found by any pass."""
    juris = _detect_jurisdiction(query)
    if juris:
        court_ids, name, _label = juris
        items = _cl_name_search(
            client, name or query, court_ids, limit=3, spare=2,
            drop_scotus_orders=(court_ids == _SCOTUS_COURT_ID),
        )
        return _filter_to_best_tier(name or query,
                                    [("juris", it) for it in items])

    # (bucket, court ids, how many to show, drop SCOTUS orders, citeCount, strict)
    passes = [
        ("ranked", None, 4, False, True, False),
        ("scotus", _SCOTUS_COURT_ID, 3, True, False, False),
    ]
    # A two-distinctive-party query also gets a strict AND pass that pins the
    # exact case when it is too lightly cited for the broad passes to reach.
    if len([p for p in _name_parties(query) if not _is_common_party(p)]) >= 2:
        passes.insert(0, ("exact", None, 3, False, True, True))
    groups: list[list[tuple[str, dict]]] = [[] for _ in passes]

    def run_pass(i: int) -> None:
        bucket, court_ids, lim, drop, by_cites, strict = passes[i]
        groups[i] = [
            (bucket, it) for it in _cl_name_search(
                client, query, court_ids, limit=lim, spare=2,
                drop_scotus_orders=drop, order_by_citecount=by_cites,
                strict=strict,
            )
        ]

    # Run the passes in parallel, then concatenate exact-/ranked-first.
    with ThreadPoolExecutor(max_workers=len(passes)) as ex:
        for _ in as_completed([ex.submit(run_pass, i) for i in range(len(passes))]):
            pass
    out: list[tuple[str, dict]] = []
    for group in groups:
        out.extend(group)
    return _filter_to_best_tier(query, out)


_STATE_HIGH_COURT_IDS = {
    courts[0][0] for _state, courts in _STATE_COURTS if courts
}


def _spotlight_court_priority(court_id: str) -> int:
    """Coarse authority order for saved opinions lacking a citation count.

    CourtListener's broad name pass uses citation count as its authority
    signal and adds a dedicated Supreme Court pass.  The local opinion store
    has no citation counts, so court level is the stable equivalent tie-break:
    SCOTUS, then federal circuits/state high courts, then other known courts.
    Name tier and name closeness still control before this value.
    """
    cid = (court_id or "").strip().lower()
    if cid == _SCOTUS_COURT_ID:
        return 4
    if cid in _CIRCUIT_COURTS or cid in _STATE_HIGH_COURT_IDS:
        return 3
    if cid in _DISTRICT_COURTS or cid in _COURT_BLUEBOOK:
        return 2
    return 1 if cid else 0


def _bluebook_saved_opinion_name(db, hit: dict) -> str:
    """Put a saved opinion's caption through the app's Bluebook name ladder.

    New database records are already abbreviated, but older/imported JSONL
    rows need not be.  When the full record is available its opinion prose is
    also used to repair ambiguous all-caps/title-case names before abbreviation.
    """
    raw = re.sub(r"<[^>]+>", "", str(hit.get("name") or "")).strip()
    if not raw:
        return ""
    body = ""
    sid = str(hit.get("scholar_id") or "").strip()
    if sid:
        try:
            rec = db.get_by_scholar_id(sid) or {}
            body = str(rec.get("text") or "")
        except Exception:
            body = ""
    try:
        name = refine_caption_case(normal_case_caption(raw), body)
        name = simplify_historical_entity_caption(name, body)
        return abbreviate_case_name(name)
    except Exception:
        return raw


def _opinion_db_spotlight_results(db, query: str,
                                  limit: int = 3) -> list[dict]:
    """Up to ``limit`` name/court-ranked rows from an already-open opinion DB.

    This deliberately accepts a database object rather than opening one: the
    Spotlight caller uses it only when the application's lazy database load has
    already completed.  Name searches reuse the CourtListener match tiers and
    jurisdiction-hint parser; reporter citations keep the database's exact
    citation lookup order and merely receive the court tie-break.
    """
    q = re.sub(r"\s+", " ", query or "").strip()
    if not q or db is None:
        return []

    juris = _detect_jurisdiction(q)
    name_query = juris[1] if juris and juris[1] else q
    allowed_courts = set(juris[0].split()) if juris else set()
    is_cite = bool(_LINE_CITE_RE.search(q))
    try:
        if is_cite:
            candidates = db.find(q)
        else:
            candidates = db.search_names(name_query, max(60, limit * 20))
    except Exception as exc:
        print(f"[spotlight-db] search failed for {q!r}: {exc}")
        return []

    scored: list[tuple[int, float, int, str, dict]] = []
    for hit in candidates:
        court_id = str(hit.get("court") or "").strip().lower()
        if allowed_courts and court_id not in allowed_courts:
            continue
        cand_name = str(hit.get("name") or "")
        if is_cite:
            tier, score = 3, 1.0
        else:
            tier = _match_tier(name_query, cand_name)
            score = _name_match_score(name_query, cand_name)
            if tier < 0 or score < _NAME_MATCH_MIN:
                continue
        scored.append((
            tier, score, _spotlight_court_priority(court_id),
            str(hit.get("year") or ""), hit,
        ))

    if not is_cite and scored:
        best_tier = max(row[0] for row in scored)
        scored = [row for row in scored if row[0] == best_tier]
    scored.sort(key=lambda row: (row[0], row[1], row[2], row[3]),
                reverse=True)

    out: list[dict] = []
    for _tier, _score, _court, _year, hit in scored[:limit]:
        saved = dict(hit)
        saved["name"] = _bluebook_saved_opinion_name(db, saved)
        out.append(saved)
    return out


_OPINION_TYPE_LABELS: dict[str, str] = {
    "010combined": "Opinion",
    "015unamimous": "Unanimous Opinion",
    "020lead": "Lead Opinion",
    "025plurality": "Plurality Opinion",
    "030concurrence": "Concurrence",
    "035concurrenceinpart": "Concurrence in Part",
    "040dissent": "Dissent",
    "050addendum": "Addendum",
    "060remittitur": "Remittitur",
    "070rehearing": "Rehearing",
    "080onthemerits": "On the Merits",
    "090onmotiontoamend": "On Motion to Amend",
}


def _strip_html(html: str) -> str:
    """Strip HTML tags, converting block-level tags to newlines first."""
    text = re.sub(
        r"<(br|/p|/div|/h[1-6]|/li|/tr|/blockquote)\b[^>]*>",
        "\n", html, flags=re.IGNORECASE,
    )
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# CL opinion-type codes → OpinionPart kind
_CL_TYPE_KIND: dict[str, str] = {
    "010combined": "majority",
    "015unamimous": "majority",
    "020lead": "majority",
    "025plurality": "majority",
    "030concurrence": "concurrence",
    "035concurrenceinpart": "concurrence",
    "040dissent": "dissent",
    "050addendum": "majority",
    "060remittitur": "majority",
    "070rehearing": "majority",
    "080onthemerits": "majority",
    "090onmotiontoamend": "majority",
}


#: CAP headmatter elements that duplicate metadata the header already shows.
_HEADMATTER_DROP = {"parties", "docketnumber", "court", "decisiondate",
                    "otherdate", "citation"}
#: Section headings for the elements that are shown, in CAP's own order.
_HEADMATTER_HEADINGS = {
    "headnotes": "Headnotes", "syllabus": "Syllabus", "summary": "Summary",
    "attorneys": "Counsel", "seealso": "See Also", "history": "History",
    "disposition": "Disposition", "correction": "Correction",
}
_HEADMATTER_EL_RE = re.compile(
    r"<(parties|docketnumber|court|decisiondate|otherdate|citation|"
    r"headnotes|syllabus|summary|attorneys|seealso|history|disposition|"
    r"correction)\b[^>]*>(.*?)</\1>",
    re.IGNORECASE | re.DOTALL,
)


def _headmatter_blocks(headmatter: str) -> "tuple[list, list]":
    """(blocks, footnotes) rendering a cluster's CAP ``headmatter`` — the
    material printed before the opinion (headnotes, the arguments of counsel,
    procedural summaries).  Elements duplicating the header metadata (parties,
    docket number, dates) are dropped; the rest keep their printed order, each
    section introduced by a small heading.  Reporter page markers inside the
    headmatter are removed: the argument pages would otherwise collide with
    the opinion's own star pagination in the page map."""
    try:
        from google_scholar import Block, Span
    except ImportError:
        return [], []
    blocks: list = []
    footnotes: list = []
    prev_tag = ""
    for m in _HEADMATTER_EL_RE.finditer(headmatter or ""):
        tag = m.group(1).lower()
        if tag in _HEADMATTER_DROP:
            continue
        try:
            parsed, fns = _parse_cl_html(m.group(2), fn_prefix="hm_")
        except Exception:
            parsed, fns = [], []
        # Drop star-pagination marker spans (see docstring).
        cleaned = []
        for b in parsed:
            spans = [s for s in b.spans if not s.pagenum]
            if any(s.text.strip() for s in spans):
                cleaned.append(Block(kind=b.kind, spans=spans))
        if not cleaned:
            continue
        if tag != prev_tag:
            heading = _HEADMATTER_HEADINGS.get(tag, tag.title())
            blocks.append(Block(kind="heading",
                                spans=[Span(text=heading, bold=True)]))
            prev_tag = tag
        blocks.extend(cleaned)
        footnotes.extend(fns)
    return blocks, footnotes


def _pick_combined_opinion(opinions: list[dict]) -> Optional[dict]:
    """The CourtListener "combined" opinion — the whole case as one
    document — when the cluster has one.  Prefer a combined record carrying
    reporter page markers; a lone unpaginated combined record is still useful
    because its plain text may be the cluster's only body.  The caller checks
    completeness against typed sub-opinions before allowing it to replace
    them."""
    def starred(op: dict) -> bool:
        return "star-pagination" in (
            op.get("html_with_citations") or op.get("html") or ""
        )
    combined = [op for op in opinions if "combined" in (op.get("type") or "")]
    for op in combined:
        if starred(op):
            return op
    # A number of lawbox/slip-opinion clusters expose only one combined
    # record, without star-pagination markup.  It is still the only body and
    # its plain_text can be segmented; the completeness gate below prevents
    # it from displacing any typed separate writings.
    if len(combined) == 1:
        return combined[0]
    hits = [op for op in opinions if starred(op)]
    return hits[0] if len(hits) == 1 else None


_STRUCTURAL_CL_TYPES = {
    "015unamimous", "020lead", "025plurality", "030concurrence",
    "035concurrenceinpart", "040dissent",
}


def _combined_parts_cover_typed(opinions: list[dict], parts: list) -> bool:
    """Whether a parsed combined document preserves CourtListener's typed
    writings.

    A combined, star-paginated document is useful for reporter pages, but it
    is not necessarily complete.  Some clusters contain a truncated combined
    record alongside correctly typed lead/concurrence/dissent sub-opinions.
    Never let that convenience record erase a writing the API explicitly
    identifies.
    """
    expected: dict[str, int] = {"majority": 0, "concurrence": 0, "dissent": 0}
    for op in opinions:
        type_code = op.get("type") or ""
        if type_code not in _STRUCTURAL_CL_TYPES:
            continue
        if not any((op.get(k) or "").strip() for k in (
            "html_with_citations", "html", "plain_text",
        )):
            continue
        expected[_CL_TYPE_KIND.get(type_code, "majority")] += 1

    if not any(expected.values()):
        return True
    actual = {"majority": 0, "concurrence": 0, "dissent": 0}
    for part in parts:
        if getattr(part, "kind", "") in actual:
            actual[part.kind] += 1
    return all(actual[kind] >= count for kind, count in expected.items())


def _assemble_case_parts(
    client, item: dict
) -> "tuple[list[OpinionPart], list[Block], str, dict]":
    """Fetch a case from CourtListener and build structured OpinionParts.

    Returns (parts, all_blocks, plain_text, cluster_metadata).
    """
    try:
        from google_scholar import (
            Block, OpinionPart, Span, blocks_to_text,
            link_footnotes_by_marker, parse_opinion_blocks, segment_blocks,
        )
    except ImportError:
        return [], [], "", {}

    cluster_id = item.get("cluster_id") or item.get("id")
    cluster = client.get_cluster(
        int(cluster_id),
        fields="case_name,citations,judges,attorneys,syllabus,headnotes,"
               "headmatter,sub_opinions,date_filed,docket",
    )

    # The Bluebook date parenthetical needs the court, but citation-lookup
    # items don't carry it (the court lives on the docket).  Fill it in when
    # missing so e.g. "85 F. 271" cites as "(6th Cir. 1898)".
    if not str(item.get("court_id") or item.get("court") or "").strip():
        docket_url = cluster.get("docket")
        if docket_url:
            try:
                dk = client._get_url(docket_url, {"fields": "court_id"})
                if dk.get("court_id"):
                    item["court_id"] = dk["court_id"]
            except Exception as exc:
                print(f"[cl-parts] docket court lookup failed: {exc}")

    # --- Build header part from metadata ---
    header_blocks: list[Block] = []
    case_name = re.sub(
        r"<[^>]+>", "",
        cluster.get("case_name") or item.get("caseName")
        or item.get("case_name") or "",
    ).strip()
    if case_name:
        header_blocks.append(Block(kind="center", spans=[
            Span(text=case_name, bold=True),
        ]))

    citations = cluster.get("citations") or []
    cite_parts: list[str] = []
    for c in citations:
        if isinstance(c, dict):
            vol = c.get("volume", "")
            reporter = c.get("reporter", "")
            page = c.get("page", "")
            if vol and reporter and page:
                cite_parts.append(f"{vol} {reporter} {page}")
        elif isinstance(c, str) and c.strip():
            cite_parts.append(c.strip())
    if cite_parts:
        header_blocks.append(Block(kind="center", spans=[
            Span(text=", ".join(cite_parts)),
        ]))

    for field_name, label in [
        ("judges", "Judges"),
        ("attorneys", "Attorneys"),
    ]:
        val = (cluster.get(field_name) or "").strip()
        if val:
            val = _strip_html(val)
        if val:
            header_blocks.append(Block(kind="para", spans=[
                Span(text=f"{label}: ", bold=True),
                Span(text=val),
            ]))

    # The CAP ``headmatter`` — everything printed before the opinion
    # (headnotes, the arguments of counsel, procedural summaries) — becomes
    # its own part.  It embeds the syllabus/headnotes, so those separate
    # fields are only rendered when there is no headmatter to show.
    hm_blocks, hm_footnotes = _headmatter_blocks(
        (cluster.get("headmatter") or "").strip()
    )
    if not hm_blocks:
        for field_name, label in [("syllabus", "Syllabus"),
                                  ("headnotes", "Headnotes")]:
            val = (cluster.get(field_name) or "").strip()
            if val:
                parsed, _fn = _parse_cl_html(val)  # these carry no footnotes
                if parsed:
                    header_blocks.append(Block(kind="heading", spans=[
                        Span(text=label, bold=True),
                    ]))
                    header_blocks.extend(parsed)

    parts: list[OpinionPart] = []
    if header_blocks:
        parts.append(OpinionPart(label="Header", kind="header", blocks=header_blocks))
    hm_part: "Optional[OpinionPart]" = None
    if hm_blocks:
        hm_part = OpinionPart(label="Headmatter (headnotes & arguments)",
                              kind="header", blocks=hm_blocks,
                              footnotes=hm_footnotes)
        parts.append(hm_part)
        header_blocks = header_blocks + hm_blocks

    # --- Sub-opinions ---
    sub_urls = cluster.get("sub_opinions") or []
    opinions: list[dict] = []
    for url in sub_urls:
        try:
            op = client._get_url(
                url,
                {"fields": "ordering_key,type,author_str,per_curiam,"
                           "html_with_citations,html,plain_text"},
            )
            opinions.append(op)
        except Exception as exc:
            print(f"[cl-parts] failed to fetch sub-opinion {url}: {exc}")

    opinions.sort(key=lambda o: (
        o.get("ordering_key") is None, o.get("ordering_key") or 0,
    ))

    # When CourtListener carries a star-paginated "combined" opinion — the whole
    # case as one nicely-formatted document, the way Google Scholar serves it —
    # show only that, parsed through the Scholar pipeline so reporter page
    # numbers and pin cites work and the opinion splits into its parts.  This
    # avoids rendering the same text twice (the separate sub-opinions and then
    # the combined version) and reads like a Scholar opinion.
    combined = _pick_combined_opinion(opinions)
    if combined is not None:
        html_text = (
            combined.get("html_with_citations") or combined.get("html") or ""
        )
        cblocks = []
        cfootnotes = []
        try:
            cblocks = parse_opinion_blocks(html_text)
            # ``<pre class="inline">`` CourtListener records are one or two
            # enormous DOM blocks to the Scholar parser.  The CL parser knows
            # that blank lines inside those pre chunks are real paragraphs,
            # which restores the bylines needed by Prado, Duncan, and similar
            # modern slip opinions.
            if len(cblocks) <= 2 and len(html_text) > 4000:
                alt_blocks, cfootnotes = _parse_cl_html(
                    html_text, fn_prefix="combined_"
                )
                if len(alt_blocks) > len(cblocks):
                    cblocks = alt_blocks
                if (combined.get("plain_text") or "").strip():
                    # Some lawbox records expose their real paragraph breaks
                    # only in ``plain_text``; both HTML parsers see a pair of
                    # giant centered/pre blocks.  Recover those paragraphs so
                    # authorship headings remain classifiable.
                    plain = (combined.get("plain_text") or "").strip()
                    paras = [p for p in re.split(r"\n\s*\n", plain) if p.strip()]
                    if len(paras) <= 2:
                        # Lawbox plain text commonly uses one physical line
                        # per paragraph and no blank separators.
                        paras = plain.splitlines()
                    plain_blocks = [
                        Block(kind="para", spans=[Span(text=para.strip())])
                        for para in paras
                        if para.strip()
                    ]
                    if len(plain_blocks) > len(cblocks):
                        cblocks = plain_blocks
            cparts = segment_blocks(cblocks)
            if cfootnotes and cparts:
                cparts[-1].footnotes.extend(cfootnotes)
            link_footnotes_by_marker(cparts)  # make [N] footnotes clickable
        except Exception as exc:
            print(f"[cl-parts] combined-opinion parse failed: {exc}")
            cparts = []
        if cparts and _combined_parts_cover_typed(opinions, cparts):
            # The combined text is the whole printed case *from the opinion
            # on*; the CAP headmatter (headnotes, arguments of counsel) still
            # belongs before the opinion — after the caption header when the
            # combined text carries one.
            if hm_part is not None:
                at = 1 if cparts and cparts[0].kind == "header" else 0
                cparts = cparts[:at] + [hm_part] + cparts[at:]
            body = [b for p in cparts for b in p.blocks]
            try:
                plain = blocks_to_text(body)
            except Exception:
                plain = ""
            return cparts, body, plain, cluster
        if cblocks:
            # Even when the combined body is incomplete, its printed header
            # can contain an official reporter omitted from CourtListener's
            # cluster metadata.  Fuse only short centered/heading fields into
            # the metadata header; keep the typed sub-opinions as the body.
            seen_header = {
                re.sub(r"\s+", " ", b.text()).strip().lower()
                for b in header_blocks
            }
            for b in cblocks[:12]:
                text = re.sub(r"\s+", " ", b.text()).strip()
                key = text.lower()
                if (b.kind in ("center", "heading") and 0 < len(text) <= 300
                        and key not in seen_header):
                    header_blocks.append(b)
                    if parts and parts[0].kind == "header":
                        parts[0].blocks.append(b)
                    seen_header.add(key)

    all_blocks: list[Block] = list(header_blocks)

    for idx, op in enumerate(opinions):
        type_code = op.get("type") or ""
        if "combined" in type_code:
            # A combined record was already considered above.  If it failed
            # the completeness check, the typed sub-opinions are the fallback;
            # appending the same combined body here would duplicate the case.
            continue
        label = _OPINION_TYPE_LABELS.get(type_code, type_code or "Opinion")
        kind = _CL_TYPE_KIND.get(type_code, "majority")

        # Add author info to label
        author = (op.get("author_str") or "").strip()
        if op.get("per_curiam") and not author:
            author = "Per Curiam"
        if author:
            label = f"{label} ({author})"

        html_text = (
            op.get("html_with_citations")
            or op.get("html")
            or ""
        )
        op_footnotes: list[Block] = []
        if html_text:
            # Namespace footnote ids per opinion so a case's several opinions
            # (each numbering from 1) don't collide in the viewer.
            op_blocks, op_footnotes = _parse_cl_html(html_text, fn_prefix=f"op{idx}_")
        else:
            plain = (op.get("plain_text") or "").strip()
            if plain:
                try:
                    from google_scholar import educate_quotes
                    plain = educate_quotes(plain)
                except ImportError:
                    pass
                op_blocks = [
                    Block(kind="para", spans=[Span(text=para.strip())])
                    for para in re.split(r"\n{2,}", plain) if para.strip()
                ]
            else:
                op_blocks = []

        if op_blocks:
            parts.append(OpinionPart(label=label, kind=kind, blocks=op_blocks,
                                     footnotes=op_footnotes))
            all_blocks.extend(op_blocks)

    try:
        plain_text = blocks_to_text(all_blocks)
    except Exception:
        plain_text = ""

    return parts, all_blocks, plain_text, cluster


def _assemble_case_text(client, item: dict) -> str:
    """
    Build a plain-text representation of a case from CourtListener.

    Layout:
      Case name
      Citations
      (blank line)
      Judges: …        ← only if the cluster has data
      Attorneys: …     ← only if the cluster has data
      Syllabus: …      ← only if the cluster has data
      Headnotes: …     ← only if the cluster has data
      (blank line)
      --- Opinion type ---
      <opinion text>
      … repeated for each sub-opinion, sorted by ordering_key …
    """
    lines: list[str] = []

    cluster_id = item.get("cluster_id") or item.get("id")
    print(f"[text] fetching cluster {cluster_id}")
    cluster = client.get_cluster(
        int(cluster_id),
        fields="case_name,citations,judges,attorneys,syllabus,headnotes,sub_opinions",
    )

    # --- Header ---
    case_name = re.sub(
        r"<[^>]+>", "",
        cluster.get("case_name") or item.get("caseName") or item.get("case_name") or "",
    ).strip()
    lines.append(case_name)

    citations = cluster.get("citations") or []
    cite_parts: list[str] = []
    for c in citations:
        if isinstance(c, dict):
            vol = c.get("volume", "")
            reporter = c.get("reporter", "")
            page = c.get("page", "")
            if vol and reporter and page:
                cite_parts.append(f"{vol} {reporter} {page}")
        elif isinstance(c, str) and c.strip():
            cite_parts.append(c.strip())
    if cite_parts:
        lines.append(", ".join(cite_parts))
    lines.append("")

    # --- Metadata sections ---
    for field, label in [
        ("judges", "Judges"),
        ("attorneys", "Attorneys"),
        ("syllabus", "Syllabus"),
        ("headnotes", "Headnotes"),
    ]:
        val = (cluster.get(field) or "").strip()
        if val:
            val = _strip_html(val)
        if val:
            lines.append(f"{label}: {val}")
            lines.append("")

    # --- Sub-opinions ---
    sub_urls = cluster.get("sub_opinions") or []
    opinions: list[dict] = []
    for url in sub_urls:
        try:
            op = client._get_url(
                url,
                {"fields": "ordering_key,type,html_with_citations,html,plain_text"},
            )
            opinions.append(op)
        except Exception as exc:
            print(f"[text] failed to fetch sub-opinion {url}: {exc}")

    # Sort by ordering_key ascending; None sorts last
    opinions.sort(key=lambda o: (o.get("ordering_key") is None, o.get("ordering_key") or 0))

    for op in opinions:
        type_code = op.get("type") or ""
        label = _OPINION_TYPE_LABELS.get(type_code, type_code or "Opinion")
        lines.append(f"--- {label} ---")
        lines.append("")
        text = (
            op.get("html_with_citations")
            or op.get("html")
            or op.get("plain_text")
            or ""
        )
        if text:
            lines.append(_strip_html(text))
        lines.append("")

    return "\n".join(lines)


try:
    from google_scholar import (
        GoogleScholarFetcher,
        bears_citation as _scholar_bears_citation,
        blocks_to_text,
        educate_quotes,
        fix_title_comma,
        parse_opinion_blocks,
        segment_blocks,
        text_similarity,
    )

    _SCHOLAR_AVAILABLE = True
except ImportError:
    _SCHOLAR_AVAILABLE = False

    def educate_quotes(text: str) -> str:  # graceful degradation
        return text

    def fix_title_comma(text: str) -> str:  # graceful degradation
        return text

@dataclass(frozen=True)
class _CasePdfTextSource:
    """A text representation discovered for a PDF-first case."""
    kind: str
    button_label: str
    source_label: str
    source_url: str
    text: str
    item: dict
    parts: list
    blocks: list
    case_law_record: "Optional[_CaseLawTextRecord]" = None


def _case_pdf_text_source(
    pdf_url: str, title: str, client=None,
) -> "Optional[_CasePdfTextSource]":
    """Prefer CourtListener text for a CAP PDF, then its exact CAP JSON text."""
    parsed = _parse_citation_line(title or "")
    fallback_cite = parsed[1] if parsed else ""
    fallback_name = parsed[0] if parsed else ""
    record = _case_law_text_for_pdf_url(pdf_url, fallback_cite)
    if record is not None:
        name = str(record.item.get("caseName") or "").strip() or fallback_name
        cites = _cluster_citations_to_strings(record.item.get("citation", []))
    else:
        name = fallback_name
        cites = [fallback_cite] if fallback_cite else []

    if client is not None:
        try:
            target = None
            for cite in cites:
                target = _cl_item_for_citation(client, cite, name=name)
                if target:
                    break
            if target:
                # CAP often has better reporter metadata than a sparse CL
                # search result.  Preserve it as a fallback for the title.
                target = dict(target)
                if record is not None:
                    target.setdefault("caseName", record.item.get("caseName", ""))
                    target.setdefault("citation", record.item.get("citation", []))
                    target.setdefault("dateFiled", record.item.get("dateFiled", ""))
                    target.setdefault("court", record.item.get("court", ""))
                    target.setdefault("court_id", record.item.get("court_id", ""))
                parts, blocks, plain, _cluster = _assemble_case_parts(
                    client, target
                )
                text = plain or _assemble_case_text(client, target)
                if (text or "").strip():
                    source_url = str(
                        target.get("absolute_url")
                        or target.get("cluster_absolute_url") or ""
                    ).strip()
                    if source_url.startswith("/"):
                        source_url = "https://www.courtlistener.com" + source_url
                    return _CasePdfTextSource(
                        "courtlistener", "CourtListener Text",
                        "CourtListener", source_url, text, target,
                        parts or [], blocks or [], record,
                    )
        except Exception as exc:
            print(f"[pdf-text] CourtListener discovery failed: {exc}")

    if record is None:
        return None
    return _CasePdfTextSource(
        "case_law", "static.case.law Text", "static.case.law",
        record.json_url, record.text, record.item, [], [], record,
    )


try:
    from pynput import keyboard as _pynput_keyboard

    _HOTKEY_AVAILABLE = True
except ImportError:
    _HOTKEY_AVAILABLE = False


def _stdin_is_tty() -> bool:
    """True when an interactive terminal is attached, so we can read the
    's' (show window) and 'q' (quit) commands the background process offers."""
    try:
        return bool(sys.stdin) and sys.stdin.isatty()
    except Exception:
        return False


def _mac_activate_app(allow_osascript: bool = True) -> bool:
    """Bring this Python process to the foreground on macOS.

    Tk's ``focus_force`` moves focus only between this app's own windows;
    when the spotlight hotkey fires while another application is frontmost,
    macOS keeps sending keystrokes to that application until ours is
    activated.  An app may always activate *itself*, so no extra privacy
    permission is needed for the AppKit path.  pynput's macOS backend
    already depends on PyObjC, so AppKit is normally importable whenever
    the global hotkey works; the osascript fallback (which may prompt for
    Automation permission once) covers the rest."""
    try:
        from AppKit import (
            NSApplicationActivateIgnoringOtherApps,
            NSRunningApplication,
        )
        NSRunningApplication.currentApplication().activateWithOptions_(
            NSApplicationActivateIgnoringOtherApps
        )
        return True
    except Exception:
        pass
    if not allow_osascript:
        return False
    try:
        subprocess.Popen(
            ["osascript", "-e",
             'tell application "System Events" to set frontmost of '
             f"the first process whose unix id is {os.getpid()} to true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return True
    except Exception:
        return False


class CourtListenerGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("CourtListener Case Law Search")
        self.root.geometry("1300x720")
        self.root.minsize(900, 500)

        self._client: Optional[CourtListenerClient] = None
        self._results: list[dict] = []
        self._scholar_results: list = []  # ScholarResult objects
        self._selected_courts: set[str] = set()  # empty = all courts
        self._search_thread: Optional[threading.Thread] = None
        self._scholar: Optional["GoogleScholarFetcher"] = None
        self._opinion_db = None          # opinion_db.OpinionDB (lazy)
        self._opinion_db_failed = False  # don't retry a broken DB every call
        self._opinion_db_loading = False
        self._opinion_db_thread: Optional[threading.Thread] = None
        self._opinion_db_lock = threading.RLock()

        self._preview_cache: dict[int, str] = {}  # result index → snippet text
        self._sort_state: dict[int, tuple[str, bool]] = {}  # tree id → (col, reverse)

        # Initialize token from env or saved config
        initial_token = os.environ.get("COURTLISTENER_TOKEN") or _load_saved_token()
        self._token_var = tk.StringVar(value=initial_token)

        self._quick_popup: Optional[tk.Toplevel] = None
        self._spotlight_toggle_at = 0.0
        self._hotkey_listener = None
        self._spotlight_hotkey = _load_saved_spotlight_hotkey()
        self._root_hidden = False

        # Opinion windows can remain separate (the default) or share one
        # notebook Toplevel.  Viewers register no-refetch reopen callbacks so
        # changing modes can migrate every case already on screen.
        config = _load_config()
        self._case_tabs_enabled = bool(config.get("case_tabs_enabled", False))
        self._case_tabs_var = tk.BooleanVar(
            master=self.root, value=self._case_tabs_enabled
        )
        self._case_tabs_window: Optional[_CaseTabsWindow] = None
        self._open_case_views: dict[int, dict] = {}
        self._force_standalone_view = False

        # Recently viewed cases, most recent first, for the "History ▾"
        # dropdown every case window carries: {"key", "label", "reopen"}.
        # Deduped by key (a re-view moves the case to the front), capped.
        self._case_history: list[dict] = self._load_case_history()

        self._build_ui()
        self._setup_global_hotkey()
        if sys.platform == "darwin":
            self._macos_keepalive()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close_window)
        # If we just restarted from an update, fold the pre-update opinions
        # backup back in (shortly, so the window comes up first).
        self.root.after(100, self._apply_pending_update_merge)

    # ------------------------------------------------------------------
    # Case-view history (the "History ▾" dropdown on case windows)
    # ------------------------------------------------------------------

    _CASE_HISTORY_MAX = 15

    def _load_case_history(self) -> list[dict]:
        saved = _load_config().get("case_history", [])
        if not isinstance(saved, list):
            return []
        entries: list[dict] = []
        for raw in saved[:self._CASE_HISTORY_MAX]:
            if not isinstance(raw, dict):
                continue
            key = str(raw.get("key") or "").strip()
            label = re.sub(r"\s+", " ", str(raw.get("label") or "")).strip()
            payload = raw.get("payload")
            opener = self._history_opener_from_payload(payload, label)
            if key and label and opener is not None:
                entries.append({
                    "key": key, "label": label, "reopen": opener,
                    "payload": payload,
                })
        return entries

    def _save_case_history(self) -> None:
        data = _load_config()
        saved: list[dict] = []
        for entry in self._case_history[:self._CASE_HISTORY_MAX]:
            payload = entry.get("payload")
            if not isinstance(payload, dict):
                continue
            saved.append({
                "key": entry.get("key", ""),
                "label": entry.get("label", ""),
                "payload": _json_ready(payload),
            })
        data["case_history"] = saved
        _save_config(data)

    def _history_opener_from_payload(self, payload, label: str):
        if not isinstance(payload, dict):
            return None
        kind = payload.get("type")
        if kind == "cl":
            item = dict(payload.get("item") or {})
            prefetch = bool(payload.get("prefetch_pdf", True))
            return lambda: self._open_history_cl(item, label, prefetch)
        if kind == "scholar":
            url = str(payload.get("url") or "").strip()
            if not url:
                return None
            item = dict(payload.get("item") or {})
            cite = str(payload.get("cite") or "").strip()
            prefetch = bool(payload.get("prefetch_pdf", True))
            return lambda: self._open_history_scholar(
                url, item, cite, label, prefetch
            )
        if kind == "pdf":
            url = str(payload.get("url") or "").strip()
            title = str(payload.get("title") or label or url).strip()
            if not url:
                return None
            return lambda: self._open_history_pdf(url, title)
        return None

    def _open_history_cl(
        self, item: dict, label: str, prefetch_pdf: bool = True,
    ) -> None:
        client = self._get_client() if self._token_var.get().strip() else None
        if client is None:
            self._status_var.set(
                "History needs a CourtListener API token for that case."
            )
            return
        self._status_var.set(f"Opening {label} from history...")

        def run() -> None:
            target = dict(item)
            if not (target.get("cluster_id") or target.get("id")):
                cite = _pick_citation(target.get("citation", []))
                if cite:
                    found = _cl_item_for_citation(client, cite)
                    if found:
                        target = found
            if not (target.get("cluster_id") or target.get("id")):
                self._post_root(
                    self._status_var.set,
                    "Could not reopen that history item from CourtListener.",
                )
                return
            self._assemble_and_open_cl(
                target, client, prefetch_pdf, lambda: None,
                search=False,
            )

        threading.Thread(target=run, daemon=True).start()

    def _open_history_scholar(
        self, url: str, item: dict, cite: str, label: str,
        prefetch_pdf: bool = True,
    ) -> None:
        fetcher = self._get_scholar()
        if fetcher is None:
            return
        self._status_var.set(f"Opening {label} from history...")

        def run() -> None:
            try:
                result = fetcher.fetch_by_url(url)
            except Exception as exc:
                print(f"[history] Scholar reopen failed for {url!r}: {exc}")
                result = None
            if result:
                r_url, html = result
                self._post_root(
                    self._open_scholar_window, r_url, html, item or None,
                    None, "opened from history", prefetch_pdf,
                )
            else:
                item_name = re.sub(
                    r"<[^>]+>", "",
                    str((item or {}).get("caseName")
                        or (item or {}).get("case_name") or ""),
                ).strip()
                self._post_root(
                    self._scholar_case_fallback, url, cite, "", prefetch_pdf,
                    item_name,
                )

        threading.Thread(target=run, daemon=True).start()

    def _open_history_pdf(self, url: str, title: str) -> None:
        self._status_var.set(f"Opening {title} from history...")
        _open_case_law_pdf(
            self.root, url, title, self._status_var.set, app=self,
        )

    def record_case_view(
        self, key: str, label: str, reopen, payload: Optional[dict] = None,
    ) -> None:
        """Remember a viewed case for the History dropdowns.  ``key``
        identifies the case (so a re-view moves it to the front instead of
        duplicating it); ``reopen`` is a no-argument callable that shows the
        case again the same way it was shown before."""
        label = re.sub(r"\s+", " ", label or "").strip() or key
        self._case_history = [e for e in self._case_history if e["key"] != key]
        entry = {"key": key, "label": label, "reopen": reopen}
        if payload is not None:
            entry["payload"] = _json_ready(payload)
        self._case_history.insert(0, entry)
        del self._case_history[self._CASE_HISTORY_MAX:]
        self._save_case_history()

    def retitle_case_view(self, key: str, label: str) -> None:
        """Update a history entry's label in place (e.g. once the Bluebook
        citation has been enriched with the court/year) without promoting it."""
        label = re.sub(r"\s+", " ", label or "").strip()
        if not label:
            return
        for e in self._case_history:
            if e["key"] == key:
                e["label"] = label
                self._save_case_history()
                break

    def populate_history_menu(self, menu: tk.Menu) -> None:
        """Fill *menu* with the current case-history entries."""
        try:
            menu.delete(0, "end")
        except tk.TclError:
            return
        if not self._case_history:
            menu.add_command(label="No cases viewed yet", state="disabled")
        for e in self._case_history:
            label = e["label"]
            if len(label) > 72:
                label = label[:69] + "..."
            menu.add_command(label=label, command=e["reopen"])

    def post_history_menu(self, widget: tk.Misc) -> None:
        """Drop the last-viewed-cases menu below *widget* (a History button)."""
        menu = tk.Menu(widget, tearoff=0)
        if not self._case_history:
            menu.add_command(label="No cases viewed yet", state="disabled")
        for e in self._case_history:
            label = e["label"]
            if len(label) > 72:
                label = label[:69] + "…"
            menu.add_command(label=label, command=e["reopen"])
        try:
            menu.tk_popup(widget.winfo_rootx(),
                          widget.winfo_rooty() + widget.winfo_height())
        finally:
            try:
                menu.grab_release()
            except tk.TclError:
                pass

    # ------------------------------------------------------------------
    # Opinion-window mode (separate OS windows or one tabbed window)
    # ------------------------------------------------------------------

    def populate_window_menu(self, menu: tk.Menu, view=None) -> None:
        """Fill the Window menu shown beside History on every opinion."""
        try:
            menu.delete(0, "end")
        except tk.TclError:
            return
        menu.add_checkbutton(
            label="Show All Windows in Tabbed View",
            variable=self._case_tabs_var,
            command=lambda: self.set_case_tabs_enabled(
                bool(self._case_tabs_var.get())
            ),
        )
        menu.add_separator()
        menu.add_command(
            label="Close Current Tab" if isinstance(view, _CaseTabPage)
            else "Close Window",
            command=(view.destroy if view is not None else lambda: None),
        )

    def new_case_view_host(self, parent: tk.Misc):
        """Compatibility name for opinion viewers."""
        return self.new_secondary_view_host(parent)

    def new_secondary_view_host(self, parent: tk.Misc):
        """Host a new non-modal view according to the current Window mode."""
        if self._force_standalone_view:
            # Pop Out may reopen through a short background resolver (notably
            # same-page CAP PDFs), so this is a one-shot consumed by the next
            # actual viewer host rather than a stack-scoped boolean.
            self._force_standalone_view = False
            return _ui_toplevel(parent)
        if not self._case_tabs_enabled:
            return _ui_toplevel(parent)
        manager = self._case_tabs_window
        if manager is None:
            manager = _CaseTabsWindow(self, self.root)
            self._case_tabs_window = manager
        return _CaseTabPage(manager)

    def register_case_window(
        self, owner, view, key: str, label: str, reopen,
    ) -> None:
        """Compatibility name for registered opinion views."""
        self.register_secondary_window(owner, view, key, label, reopen)

    def register_secondary_window(
        self, owner, view, key: str, label: str, reopen,
    ) -> None:
        """Track a view so mode changes and Pop Out can reconstruct it."""
        ident = id(view)
        self._open_case_views[ident] = {
            "owner": owner, "view": view, "key": key,
            "label": label, "reopen": reopen,
        }

        def gone(event, ident=ident, view=view) -> None:
            if event.widget is view:
                self._open_case_views.pop(ident, None)

        try:
            view.bind("<Destroy>", gone, add="+")
        except tk.TclError:
            pass
        if isinstance(view, _CaseTabPage):
            view._secondary_reopen = reopen
            view._secondary_owner = owner

    def _secondary_entry_for_view(self, view):
        return next((
            entry for entry in self._open_case_views.values()
            if entry.get("view") is view
        ), None)

    def can_pop_out_view(self, view) -> bool:
        return (
            isinstance(view, _CaseTabPage)
            and self._secondary_entry_for_view(view) is not None
        )

    def pop_out_view(self, view) -> None:
        """Recreate one tab as a standalone OS window, leaving mode enabled."""
        entry = self._secondary_entry_for_view(view)
        if entry is None or not isinstance(view, _CaseTabPage):
            return
        try:
            view.destroy()
        except tk.TclError:
            pass
        self._force_standalone_view = True
        try:
            entry["reopen"]()
        except Exception as exc:
            self._force_standalone_view = False
            print(f"[windows] could not pop out {entry['label']!r}: {exc}")

    @staticmethod
    def _case_view_is_live(view) -> bool:
        try:
            return bool(view.winfo_exists())
        except (AttributeError, tk.TclError):
            return False

    def set_case_tabs_enabled(self, enabled: bool) -> None:
        """Switch modes and migrate every currently open opinion."""
        enabled = bool(enabled)
        self._case_tabs_var.set(enabled)
        if enabled == self._case_tabs_enabled:
            return

        snapshots = [
            entry for entry in self._open_case_views.values()
            if self._case_view_is_live(entry.get("view"))
        ]
        self._case_tabs_enabled = enabled
        config = _load_config()
        config["case_tabs_enabled"] = enabled
        _save_config(config)

        # Set the new mode before closing anything; each snapshot's reopen
        # callback will therefore choose the destination host automatically.
        for entry in snapshots:
            try:
                entry["view"].destroy()
            except tk.TclError:
                pass
        self._open_case_views.clear()
        if not enabled and self._case_tabs_window is not None:
            self._case_tabs_window.close()
            self._case_tabs_window = None
        for entry in snapshots:
            try:
                entry["reopen"]()
            except Exception as exc:
                print(f"[windows] could not migrate {entry['label']!r}: {exc}")

    def _case_tabs_window_destroyed(self, manager: _CaseTabsWindow) -> None:
        if self._case_tabs_window is manager:
            self._case_tabs_window = None

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        style = ttk.Style()
        style.configure("Treeview", rowheight=28)

        # --- Menubar ---
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        # History first (far left) — the same recently-viewed-cases menu the
        # case windows carry, refreshed each time it opens.
        history_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="History", menu=history_menu)
        try:
            history_menu.configure(
                postcommand=lambda m=history_menu: self.populate_history_menu(m)
            )
        except tk.TclError:
            pass
        self.populate_history_menu(history_menu)
        settings_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Settings", menu=settings_menu)
        settings_menu.add_command(label="API Token…", command=self._show_settings_dialog)
        settings_menu.add_command(
            label="Spotlight Shortcut…",
            command=self._show_spotlight_shortcut_dialog,
        )
        settings_menu.add_separator()
        settings_menu.add_command(
            label="Check for Updates…", command=self._check_for_updates,
        )
        lookup_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Look Up", menu=lookup_menu)
        lookup_menu.add_command(
            label="U.S. Code / C.F.R. Section…", accelerator="Ctrl+L",
            command=self._show_statute_lookup,
        )
        lookup_menu.add_command(
            label="Open Citation List…",
            command=self._show_citation_list_dialog,
        )
        lookup_menu.add_command(
            label="Quick Look Up (case or statute)…", accelerator="Ctrl+S",
            command=self._show_quick_lookup,
        )
        lookup_menu.add_command(
            label="Search English Reports (case name)…",
            command=self._show_eng_rep_search,
        )
        brief_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Brief", menu=brief_menu)
        brief_menu.add_command(
            label="Open Brief (highlight citations)…", accelerator="Ctrl+B",
            command=self._open_brief,
        )
        brief_menu.add_command(
            label="Import PDF & Link Citations (on the page)…",
            command=self._open_linked_pdf,
        )
        # PDFs opened in the brief viewer stay cached for 30 days from their
        # last open; this submenu reopens them (restarting the clock).
        self._recent_briefs_menu = tk.Menu(
            brief_menu, tearoff=0,
            postcommand=self._fill_recent_briefs_menu,
        )
        brief_menu.add_cascade(
            label="Recent Briefs", menu=self._recent_briefs_menu,
        )
        db_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Database", menu=db_menu)
        db_menu.add_command(
            label="Find Opinion in Database…", command=self._show_db_find,
        )
        db_menu.add_separator()
        db_menu.add_command(
            label="Merge In Database File…", command=self._merge_db_file,
        )
        db_menu.add_command(
            label="Rebuild Search Index", command=self._rebuild_db_index,
        )
        self.root.bind("<Control-l>", lambda _e: self._show_statute_lookup())
        self.root.bind("<Control-s>", lambda _e: self._show_quick_lookup())
        self.root.bind("<Control-b>", lambda _e: self._open_brief())

        # --- Search frame ---
        search_frame = ttk.LabelFrame(self.root, text="Search", padding=6)
        search_frame.pack(fill="x", padx=10, pady=(10, 4))

        # Row 1: query + button
        row1 = ttk.Frame(search_frame)
        row1.pack(fill="x", pady=(0, 4))
        ttk.Label(row1, text="Query:").pack(side="left")
        self._query_var = tk.StringVar()
        self._query_entry = ttk.Entry(row1, textvariable=self._query_var)
        self._query_entry.pack(side="left", padx=6, fill="x", expand=True)
        self._query_entry.bind("<Return>", lambda _e: self._do_search())
        self._search_btn = ttk.Button(row1, text="Search", command=self._do_search)
        self._search_btn.pack(side="left", padx=(0, 4))

        # Row 2: filters
        row2 = ttk.Frame(search_frame)
        row2.pack(fill="x")

        self._courts_btn_var = tk.StringVar(value="Courts: All ▾")
        ttk.Button(
            row2,
            textvariable=self._courts_btn_var,
            command=self._show_court_picker,
        ).pack(side="left", padx=(0, 12))

        ttk.Label(row2, text="Filed from:").pack(side="left")
        self._date_from_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self._date_from_var, width=12).pack(
            side="left", padx=4
        )

        ttk.Label(row2, text="to:").pack(side="left")
        self._date_to_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self._date_to_var, width=12).pack(
            side="left", padx=4
        )

        ttk.Label(row2, text="  Max results:").pack(side="left")
        self._page_size_var = tk.IntVar(value=20)
        ttk.Spinbox(
            row2, from_=5, to=20, textvariable=self._page_size_var, width=5
        ).pack(side="left", padx=4)

        # --- Results area: left trees + right preview ---
        results_frame = ttk.LabelFrame(self.root, text="Results", padding=6)
        results_frame.pack(fill="both", expand=True, padx=10, pady=4)

        # --- Status bar + action buttons — packed first so they are always
        #     visible regardless of window height.
        bottom = ttk.Frame(results_frame)
        bottom.pack(side="bottom", fill="x", pady=(4, 0))

        self._download_btn = ttk.Button(
            bottom,
            text="Download PDF",
            command=self._download_selected,
            state="disabled",
        )
        self._download_btn.pack(side="right", padx=4)

        scholar_tip = "" if _SCHOLAR_AVAILABLE else " (needs beautifulsoup4)"
        self._scholar_btn = ttk.Button(
            bottom,
            text=f"Scholar Text{scholar_tip}",
            command=self._fetch_scholar_text,
            state="disabled",
        )
        self._scholar_btn.pack(side="right", padx=4)

        self._status_var = tk.StringVar(value="Enter a query and click Search.")
        ttk.Label(bottom, textvariable=self._status_var, anchor="w").pack(
            side="left", fill="x", expand=True
        )

        # --- Compact preview strip, spans the full width above the status bar
        preview_frame = ttk.LabelFrame(results_frame, text="Preview", padding=2)
        preview_frame.pack(side="bottom", fill="x", pady=(4, 0))
        self._preview_text = tk.Text(
            preview_frame,
            wrap="word",
            height=4,
            state="disabled",
            font=("TkDefaultFont", 9),
            relief="flat",
            background="#f5f5f5",
        )
        self._preview_text.pack(fill="x", expand=True)

        paned = ttk.PanedWindow(results_frame, orient="horizontal")
        paned.pack(fill="both", expand=True)

        # -- Left pane: CourtListener results (main tree + orders tree) --
        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=3)
        ttk.Label(
            left_frame,
            text="CourtListener",
            foreground="gray",
            font=("TkDefaultFont", 9, "italic"),
        ).pack(anchor="w")

        cols = ("case_name", "court", "date_filed", "citation", "status")

        main_tree_frame = ttk.Frame(left_frame)
        main_tree_frame.pack(fill="both", expand=True)
        self._tree = ttk.Treeview(
            main_tree_frame, columns=cols, show="headings", selectmode="browse"
        )
        self._configure_tree_columns(self._tree)
        vsb = ttk.Scrollbar(main_tree_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        # Double-click opens the Google Scholar text (falling back to the
        # CourtListener text, then a case.law PDF) — the reading view, not a
        # download.  "Download PDF" remains on the button below.
        self._tree.bind("<Double-1>", lambda _e: self._fetch_scholar_text())
        self._tree.bind("<<TreeviewSelect>>", lambda _e: self._on_row_select(self._tree))
        self._tree.bind("<Button-3>", lambda e: self._on_right_click(e, self._tree))

        # Orders / short-opinion section
        orders_sep = ttk.Frame(left_frame)
        orders_sep.pack(fill="x", pady=(4, 0))
        ttk.Separator(orders_sep, orient="horizontal").pack(fill="x")
        ttk.Label(
            orders_sep,
            text="Orders  (≤ 2 citations)",
            foreground="gray",
            font=("TkDefaultFont", 9, "italic"),
        ).pack(anchor="w", pady=(2, 0))

        orders_tree_frame = ttk.Frame(left_frame)
        orders_tree_frame.pack(fill="x")
        self._orders_tree = ttk.Treeview(
            orders_tree_frame, columns=cols, show="headings", selectmode="browse", height=4
        )
        self._configure_tree_columns(self._orders_tree)
        vsb2 = ttk.Scrollbar(orders_tree_frame, orient="vertical", command=self._orders_tree.yview)
        self._orders_tree.configure(yscrollcommand=vsb2.set)
        self._orders_tree.pack(side="left", fill="x", expand=True)
        vsb2.pack(side="right", fill="y")
        self._orders_tree.bind("<Double-1>", lambda _e: self._fetch_scholar_text())
        self._orders_tree.bind(
            "<<TreeviewSelect>>", lambda _e: self._on_row_select(self._orders_tree)
        )
        self._orders_tree.bind("<Button-3>", lambda e: self._on_right_click(e, self._orders_tree))

        # -- Right pane: Google Scholar results --
        scholar_pane = ttk.Frame(paned)
        paned.add(scholar_pane, weight=2)
        sch_header = ttk.Frame(scholar_pane)
        sch_header.pack(fill="x")
        ttk.Label(
            sch_header,
            text="Google Scholar",
            foreground="gray",
            font=("TkDefaultFont", 9, "italic"),
        ).pack(side="left")
        self._scholar_status_var = tk.StringVar(value="")
        ttk.Label(
            sch_header, textvariable=self._scholar_status_var, foreground="gray"
        ).pack(side="right")

        sch_tree_frame = ttk.Frame(scholar_pane)
        sch_tree_frame.pack(fill="both", expand=True)
        self._scholar_tree = ttk.Treeview(
            sch_tree_frame,
            columns=("case", "source"),
            show="headings",
            selectmode="browse",
        )
        self._scholar_tree.heading("case", text="Case")
        self._scholar_tree.heading("source", text="Court / Year")
        self._scholar_tree.column("case", width=250, minwidth=120)
        self._scholar_tree.column("source", width=140, minwidth=80)
        svsb = ttk.Scrollbar(
            sch_tree_frame, orient="vertical", command=self._scholar_tree.yview
        )
        self._scholar_tree.configure(yscrollcommand=svsb.set)
        self._scholar_tree.pack(side="left", fill="both", expand=True)
        svsb.pack(side="right", fill="y")
        self._scholar_tree.bind(
            "<<TreeviewSelect>>", lambda _e: self._on_scholar_row_select()
        )
        self._scholar_tree.bind(
            "<Double-1>", lambda _e: self._open_selected_scholar_result()
        )


    # ------------------------------------------------------------------
    # Window lifecycle — hide on close, keep hotkey alive
    # ------------------------------------------------------------------

    def _can_run_headless(self) -> bool:
        """True when the process can keep running without a visible window:
        either the global hotkey is live (Ctrl+Space opens search) or there's
        a terminal to read 's'/'q' from."""
        if _stdin_is_tty():
            return True
        return _HOTKEY_AVAILABLE and self._hotkey_listener is not None

    def _on_close_window(self) -> None:
        """Hide the main window instead of destroying it so the process keeps
        running in the background — the global hotkey stays live and the
        window can be reopened with 's' in the terminal.  Only quit outright
        when there's no way to bring it back."""
        if self._can_run_headless():
            self.root.withdraw()
            self._root_hidden = True
            self._print_background_help(closed=True)
        else:
            self.root.destroy()

    def _ensure_root_exists(self) -> None:
        """Make sure the root window is usable — show it if it was hidden."""
        if self._root_hidden:
            self.root.deiconify()
            self._root_hidden = False

    def _show_main_window(self) -> None:
        """Bring the full search window to the front (the 's' command and the
        quick-search 'open the main window' path both land here)."""
        try:
            self._ensure_root_exists()
            self.root.deiconify()
            self.root.lift()
            if sys.platform == "win32":
                self._win_force_foreground(self.root)
            self.root.focus_force()
            self._query_entry.focus_set()
        except tk.TclError:
            pass

    def _print_background_help(self, closed: bool = False) -> None:
        """Tell the user how to drive the background process — Ctrl+Space to
        search, 's' to open the full window, 'q' to quit."""
        hotkey = _display_hotkey(getattr(
            self, "_spotlight_hotkey", _default_spotlight_hotkey()
        ))
        intro = (
            "Window closed — GetCases is still running in the background."
            if closed
            else "GetCases is running in the background."
        )
        tips = []
        if _HOTKEY_AVAILABLE and self._hotkey_listener is not None:
            tips.append(f"Press {hotkey} anywhere to search.")
        if _stdin_is_tty():
            tips.append(
                "Type 's' + Enter to open the full search window, "
                "'q' + Enter to quit."
            )
        if tips:
            print("\n" + intro + "\n  " + "\n  ".join(tips))

    # ------------------------------------------------------------------
    # Global hotkey (Ctrl+Space / Cmd+Space) → quick search popup
    # ------------------------------------------------------------------

    def _macos_keepalive(self) -> None:
        """macOS: keep background work flowing when no window is visible.

        Opening a case from the spotlight destroys the popup — often the
        app's only visible window — and fetches the opinion on a worker
        thread.  With the app active but window-less, macOS App Nap
        throttles the process and the Aqua Tcl notifier can sit in its
        event wait without draining the timer/idle queues, so the fetched
        case window doesn't appear until some UI event (reopening the
        spotlight) wakes the loop.  Two measures, both cheap:

        * an NSActivity opts the process out of App Nap for as long as the
          app runs (PyObjC ships wherever pynput's hotkey works; the
          "allowing idle system sleep" level doesn't keep the Mac awake);
        * a low-frequency self-rescheduling after() tick keeps the Tcl
          event loop cycling, so callbacks posted by worker threads are
          dispatched promptly even with no window events arriving.
        """
        try:
            from Foundation import NSProcessInfo
            try:
                from Foundation import (
                    NSActivityUserInitiatedAllowingIdleSystemSleep as _opts,
                )
            except ImportError:
                _opts = 0x00FFFFFF  # UserInitiatedAllowingIdleSystemSleep
            # Keep a reference — ending the activity re-enables App Nap.
            self._macos_activity = (
                NSProcessInfo.processInfo().beginActivityWithOptions_reason_(
                    _opts, "CourtListener spotlight hotkey stays responsive"
                )
            )
        except Exception as exc:
            print(f"[macos] App Nap opt-out unavailable: {exc}")

        def tick() -> None:
            try:
                self.root.after(250, tick)
            except tk.TclError:
                pass  # app shutting down

        tick()

    def _setup_global_hotkey(self) -> None:
        if not _HOTKEY_AVAILABLE:
            return
        if self._hotkey_listener is not None:
            if sys.platform == "darwin":
                return
            try:
                self._hotkey_listener.stop()
            except Exception:
                pass
            self._hotkey_listener = None
        hotkey = getattr(
            self, "_spotlight_hotkey", _default_spotlight_hotkey()
        )
        try:
            self._hotkey_listener = _pynput_keyboard.GlobalHotKeys(
                {hotkey: self._on_global_hotkey}
            )
            self._hotkey_listener.daemon = True
            self._hotkey_listener.start()
        except Exception:
            self._hotkey_listener = None

    def _on_global_hotkey(self) -> None:
        self.root.after(0, self._toggle_quick_search_popup)

    def _spot_knockout_corners(self, popup: tk.Toplevel) -> None:
        """On Windows, punch the popup's square window corners out to
        transparent so the rounded CustomTkinter card reads cleanly over
        whatever is behind it.  Elsewhere this is a no-op — the corners simply
        show the card's own background colour."""
        if sys.platform != "win32":
            return
        key = "#010203"  # a colour the UI never uses, keyed out to transparent
        try:
            popup.configure(bg=key)
            popup.wm_attributes("-transparentcolor", key)
        except tk.TclError:
            pass

    def _close_quick_popup(self) -> None:
        """Take down the spotlight popup (idempotent).  The window is
        withdrawn before it is destroyed: on macOS, destroying the
        borderless ("plain"-style) popup outright can leave it painted on
        screen even though Tk has forgotten it — pressing the hotkey then
        looks like the spotlight "didn't close" (and the next press, which
        creates a fresh popup over the ghost, like it closed and instantly
        reopened).  Withdrawing unmaps it first, so every close path
        actually clears the screen."""
        popup, self._quick_popup = self._quick_popup, None
        if popup is None:
            return
        for step in (popup.withdraw, popup.destroy):
            try:
                step()
            except tk.TclError:
                pass

    def _mac_return_focus(self) -> None:
        """macOS: after the spotlight (or its not-found toast) closes with
        no other window showing, hide the app so the previous application
        becomes frontmost again.  The popup activated this app to type
        into; cancelling it would otherwise leave a window-less app
        holding the keyboard, and hiding also takes any window the Aqua
        compositor left behind off the screen.  No-op on other platforms,
        or while any of this app's windows is visible."""
        if sys.platform != "darwin":
            return
        stack = [self.root]
        while stack:
            w = stack.pop()
            try:
                if (isinstance(w, (tk.Tk, tk.Toplevel))
                        and w.state() == "normal"):
                    return
                stack.extend(w.winfo_children())
            except tk.TclError:
                continue
        try:
            from AppKit import NSApplication
            NSApplication.sharedApplication().hide_(None)
        except Exception:
            pass

    def _toggle_quick_search_popup(self) -> None:
        # One press = one toggle: a duplicate hotkey delivery (macOS event
        # taps can fire twice for one chord) would close the popup and
        # immediately reopen it, so a burst within the debounce window is
        # a single toggle.
        now = time.monotonic()
        if now - self._spotlight_toggle_at < 0.3:
            return
        self._spotlight_toggle_at = now
        if self._quick_popup is not None:
            self._close_quick_popup()
            self._mac_return_focus()
            return

        popup = tk.Toplevel(self.root)
        self._quick_popup = popup
        # Reset the consecutive-empty-Return counter and dropdown generation
        # each time a fresh popup opens.
        self._spotlight_empty_returns = 0
        self._spotlight_generation = 0
        self._spotlight_results_frame = None
        # Borderless spotlight window.  On macOS an overrideredirect window
        # can never become the *key* window under Aqua Tk — the insertion
        # cursor blinks but every keystroke still goes to the previous app —
        # so the borderless look comes from the (unsupported-but-stable)
        # "plain" Mac window style instead, which does accept key focus.
        if sys.platform == "darwin":
            try:
                popup.tk.call("::tk::unsupported::MacWindowStyle",
                              "style", popup._w, "plain", "none")
            except tk.TclError:
                popup.overrideredirect(True)
        else:
            popup.overrideredirect(True)
        popup.attributes("-topmost", True)

        entry_var = tk.StringVar()

        if _CTK_AVAILABLE:
            # Modern spotlight: a rounded, themed card with a large search
            # field.  `border` is the card into which the results dropdown packs
            # itself later, mirroring the plain-Tk layout below.
            _ensure_modern_theme()
            pw, ph = 600, 66
            popup.configure(bg=_UI["window"])
            self._spot_knockout_corners(popup)
            border = ctk.CTkFrame(
                popup, corner_radius=16, fg_color=_UI["window"],
                border_width=1, border_color=_UI["border"],
            )
            border.pack(fill="both", expand=True)
            bar = ctk.CTkFrame(border, corner_radius=12, fg_color=_UI["surface"])
            bar.pack(fill="x", padx=12, pady=12)
            ctk.CTkLabel(
                bar, text="⚖", font=_ui_font(20), text_color=_UI["muted"],
                width=26,
            ).pack(side="left", padx=(12, 0))
            entry = ctk.CTkEntry(
                bar, textvariable=entry_var, font=_ui_font(17),
                placeholder_text="Search cases, citations, statutes…",
                border_width=0, fg_color="transparent",
                text_color=_UI["text"], height=42,
            )
            entry.pack(side="left", fill="x", expand=True, padx=(4, 12), pady=6)
            focus_target = entry._entry
        else:
            pw, ph = 520, 48
            popup.configure(bg="#888888")
            border = tk.Frame(popup, bg="#888888")
            border.pack(fill="both", expand=True)
            inner = tk.Frame(border, bg="#ffffff", padx=10, pady=6)
            inner.pack(fill="both", expand=True, padx=2, pady=2)
            tk.Label(
                inner, text="Search CourtListener:", bg="#ffffff",
                fg="#555555", font=("TkDefaultFont", 11),
            ).pack(side="left", padx=(0, 6))
            entry = tk.Entry(
                inner, textvariable=entry_var, font=("TkDefaultFont", 13),
                relief="flat", bg="#ffffff",
            )
            entry.pack(side="left", fill="x", expand=True)
            focus_target = entry

        sx = popup.winfo_screenwidth()
        sy = popup.winfo_screenheight()
        popup.geometry(f"{pw}x{ph}+{(sx - pw) // 2}+{sy // 3}")

        def _submit(_e=None) -> None:
            query = entry_var.get().strip()
            if not query:
                # Empty search bar: open the main window only on the second
                # consecutive Return (a deliberate "show me everything").
                self._spotlight_empty_returns += 1
                if self._spotlight_empty_returns >= 2:
                    self._open_main_from_spotlight(popup)
                return
            self._spotlight_empty_returns = 0

            # 1. Statute / regulation / federal rule: "42 USC 1983(b)",
            # "29 CFR 1614.105", "Fed. R. Civ. P. 56", "Cal. Penal Code 187".
            # The section sign is optional — it can't be typed on a keyboard.
            statute = _parse_statute_query(query)
            if statute:
                self._close_quick_popup()
                _open_statute_action(self.root, statute, app=self)
                return

            # 1b. English Reports citation ("156 Eng. Rep. 145", "95 E.R. 807"):
            # handle it as an E.R. cite end-to-end.  _open_eng_rep opens the
            # CommonLII scan when the cite is in our index, and otherwise falls
            # back to a CommonLII search (in the browser) with a status note.
            # Either way it must NOT fall through to the Google Scholar /
            # CourtListener case search below: that treats "Eng. Rep." as a U.S.
            # reporter and spends many seconds on a doomed lookup that, after the
            # popup has already closed, looks like the app has hung.
            er_m = eng_rep.ER_CITE_RE.search(query)
            if er_m:
                self._close_quick_popup()
                _open_eng_rep(self.root, eng_rep.cite_spec(er_m),
                              self._status_var.set, app=self)
                return
            # ... or its original nominate-report form ("9 Exch. 341",
            # "Cro. Jac. 489").  Resolution-gated in eng_rep, so a U.S. cite
            # sharing an abbreviation falls through to the case search below.
            nom = eng_rep.iter_nominate_cites(query)
            if nom:
                self._close_quick_popup()
                _open_eng_rep(self.root, nom[0][2], self._status_var.set,
                              app=self)
                return

            # The opinion/brief readers recognize several case-citation forms
            # that the permissive hand-typed parser below cannot normalize on
            # its own: early federal reporters ("1 Sumner, 73", "35 Fed.
            # Rep. 665"), Federal Cases case numbers, federal slip opinions,
            # and similar special forms.  Reuse that shared detector so the
            # same citation opens directly from Spotlight too.
            detected = _spotlight_case_action(query)
            if detected and detected[2][0] != "cite":
                self._close_quick_popup()
                _follow_brief_action(
                    self, self.root, detected[2], self._status_var.set,
                )
                return

            # 2. Case citation: "365 U.S. 167" or "Monroe v. Pape, 365 U.S. 167, 171"
            parsed = _parse_citation_line(query)
            if detected:
                start, _end, (_kind, target) = detected
                cite, _, pin = target.partition("@")
                # Preserve the broader parser's case-name fallback when it
                # found one; otherwise take the caption preceding the shared
                # detector's citation span.
                name = parsed[0] if parsed else query[:start]
                name = name.strip().rstrip(",;–—- ").strip()
                parsed = name, cite, pin
            if parsed:
                name, cite, pin = parsed
                fetcher = (
                    self._get_scholar() if _SCHOLAR_AVAILABLE else None
                )
                client = (
                    self._get_client()
                    if self._token_var.get().strip() else None
                )
                if fetcher is not None or client is not None:
                    self._close_quick_popup()

                    def run() -> None:
                        # The popup is already gone; a citation that
                        # resolves nowhere would otherwise end in silence,
                        # which reads as the app having hung.
                        if not self._try_open_citation(
                            name, cite, pin, fetcher, client,
                        ):
                            label = f"{name}, {cite}" if name else cite
                            self._post_root(self._spotlight_notify,
                                            f"Nothing found for {label}")
                    threading.Thread(target=run, daemon=True).start()
                    return

            # 3. Fallback: show spotlight dropdown with search results
            # (keep the popup alive — it expands into the dropdown)
            self._show_spotlight_dropdown(popup, border, entry, query)

        def _dismiss(_e=None) -> None:
            self._close_quick_popup()
            self._mac_return_focus()

        entry.bind("<Return>", _submit)
        entry.bind("<Escape>", _dismiss)

        def _grab_focus(attempt: int = 0) -> None:
            try:
                popup.deiconify()
                popup.lift()
                if not popup.winfo_viewable():
                    if attempt < 25:
                        popup.after(20, lambda: _grab_focus(attempt + 1))
                    return
                # On Windows, Tk's focus_force cannot steal the foreground
                # from another process (the OS foreground-lock blocks it),
                # so go through the Win32 API on the real top-level HWND.
                if sys.platform == "win32":
                    self._win_force_foreground(popup)
                elif sys.platform == "darwin":
                    # The hotkey fires while another app is frontmost, and
                    # focus_force only moves focus *within* this app — the
                    # Python app itself must be activated or the popup's
                    # cursor blinks without receiving a single keystroke.
                    _mac_activate_app(allow_osascript=(attempt == 0))
                popup.focus_force()
                # `focus_target` is the real Tk entry — for a CustomTkinter
                # field that is the internal widget wrapped by CTkEntry, which is
                # also what `focus_get` reports, so the identity check below holds.
                focus_target.focus_force()
                focus_target.icursor(tk.END)
                try:
                    focus_target.select_range(0, tk.END)
                except tk.TclError:
                    pass
                # The entry may not hold keyboard focus on the first try;
                # retry until it does (or we run out of attempts).
                if popup.focus_get() is not focus_target and attempt < 25:
                    popup.after(20, lambda: _grab_focus(attempt + 1))
            except tk.TclError:
                pass

        popup.after(10, _grab_focus)

    def _open_main_from_spotlight(
        self, popup: tk.Toplevel, query: str = "",
    ) -> None:
        """Close the spotlight popup and bring up the full search window,
        optionally seeding and running *query*."""
        self._close_quick_popup()
        self._ensure_root_exists()
        self.root.deiconify()
        self.root.lift()
        if sys.platform == "win32":
            self._win_force_foreground(self.root)
        self.root.focus_force()
        self._query_entry.focus_set()
        if query:
            self._query_var.set(query)
            self._do_search()

    def _spotlight_notify(self, message: str) -> None:
        """Transient toast in the spotlight's spot, for a spotlight lookup
        that ends with nothing to open.  By then the popup is long closed
        and the main window is often hidden, so a status-bar note would go
        unseen and the app would just go quiet.  Display-only: it never
        takes focus; a click dismisses it, and it dismisses itself after a
        few seconds."""
        self._status_var.set(message)
        try:
            toast = tk.Toplevel(self.root)
            toast.overrideredirect(True)
            toast.attributes("-topmost", True)
            pw, ph = (600, 64) if _CTK_AVAILABLE else (520, 48)
            sx = toast.winfo_screenwidth()
            sy = toast.winfo_screenheight()
            toast.geometry(f"{pw}x{ph}+{(sx - pw) // 2}+{sy // 3}")
            if _CTK_AVAILABLE:
                _ensure_modern_theme()
                toast.configure(bg=_UI["window"])
                self._spot_knockout_corners(toast)
                card = ctk.CTkFrame(
                    toast, corner_radius=16, fg_color=_UI["window"],
                    border_width=1, border_color=_UI["border"],
                )
                card.pack(fill="both", expand=True)
                ctk.CTkLabel(
                    card, text=message, font=_ui_font(14),
                    text_color=_UI["muted"], anchor="center",
                ).pack(fill="both", expand=True, padx=16, pady=12)
            else:
                toast.configure(bg="#888888")
                inner = tk.Frame(toast, bg="#ffffff", padx=12, pady=8)
                inner.pack(fill="both", expand=True, padx=2, pady=2)
                tk.Label(
                    inner, text=message, bg="#ffffff", fg="#555555",
                    font=("TkDefaultFont", 12), anchor="center",
                ).pack(fill="both", expand=True)

            def dismiss(_e=None) -> None:
                try:
                    toast.withdraw()
                    toast.destroy()
                except tk.TclError:
                    pass
                self._mac_return_focus()

            _bind_recursive(toast, "<Button-1>", dismiss)
            toast.after(4000, dismiss)
        except tk.TclError:
            pass

    @staticmethod
    def _spot_tier_color(court_id: str) -> str:
        """Badge colour: the Supreme Court gets the deep navy accent, every
        other court a quieter slate, so SCOTUS results stand out at a glance."""
        return _UI["badge"] if court_id == "scotus" else _UI["badge_alt"]

    def _spot_build_row(self, parent, court_abbr: str, tier_color: str,
                        display_name: str, detail_text: str) -> dict:
        """Create one spotlight result row and return the widgets the caller
        needs to bind clicks and re-colour on highlight.  Builds a rounded,
        themed card when CustomTkinter is present, or the plain-Tk row otherwise.
        """
        if _CTK_AVAILABLE:
            row = ctk.CTkFrame(parent, corner_radius=10,
                               fg_color=_UI["window"], height=54)
            row.pack(side="top", fill="x", padx=10, pady=(4, 0))
            row.pack_propagate(False)
            badge = ctk.CTkLabel(
                row, text=court_abbr, fg_color=tier_color, corner_radius=6,
                text_color="#ffffff", font=_ui_font(11, "bold"),
                width=78, height=36,
            )
            badge.pack(side="left", padx=(10, 12), pady=9)
            text_frame = ctk.CTkFrame(row, fg_color="transparent")
            text_frame.pack(side="left", fill="both", expand=True, padx=(0, 12))
            name_lbl = ctk.CTkLabel(
                text_frame, text=display_name, fg_color="transparent",
                text_color=_UI["text"], font=_ui_font(14), anchor="w",
            )
            name_lbl.pack(fill="x", pady=(7, 0))
            detail_lbl = ctk.CTkLabel(
                text_frame, text=detail_text, fg_color="transparent",
                text_color=_UI["muted"], font=_ui_font(11), anchor="w",
            )
            detail_lbl.pack(fill="x")
            return {
                "row": row, "badge": badge, "text_frame": text_frame,
                "name": name_lbl, "detail": detail_lbl, "modern": True,
            }
        row = tk.Frame(parent, bg="#ffffff", height=52, cursor="hand2")
        row.pack(side="top", fill="x", padx=4, pady=(2, 0))
        row.pack_propagate(False)
        badge = tk.Label(
            row, text=court_abbr, bg=tier_color, fg="#ffffff",
            font=("TkDefaultFont", 10, "bold"), padx=6, pady=2,
            anchor="center", width=8,
        )
        badge.pack(side="left", padx=(6, 8))
        text_frame = tk.Frame(row, bg="#ffffff")
        text_frame.pack(side="left", fill="x", expand=True, padx=(0, 6))
        name_lbl = tk.Label(
            text_frame, text=display_name, bg="#ffffff", fg="#222222",
            font=("TkDefaultFont", 10), anchor="w",
        )
        name_lbl.pack(fill="x")
        detail_lbl = tk.Label(
            text_frame, text=detail_text, bg="#ffffff", fg="#888888",
            font=("TkDefaultFont", 8), anchor="w",
        )
        detail_lbl.pack(fill="x")
        return {
            "row": row, "badge": badge, "text_frame": text_frame,
            "name": name_lbl, "detail": detail_lbl, "modern": False,
        }

    def _spot_highlight_row(self, r: dict, selected: bool) -> None:
        """Colour a result row for the current keyboard/hover selection."""
        if r["modern"]:
            r["row"].configure(
                fg_color=_UI["selection"] if selected else _UI["window"]
            )
            return
        bg = "#d0e0f0" if selected else "#ffffff"
        for w in (r["row"], r["text_frame"], r["name"], r["detail"]):
            try:
                w.config(bg=bg)
            except tk.TclError:
                pass

    def _show_spotlight_dropdown(
        self, popup: tk.Toplevel, border: tk.Frame,
        entry: tk.Entry, query: str,
    ) -> None:
        """Expand the popup into a spotlight-style dropdown with streaming
        search results from Google Scholar and CourtListener."""

        # A fresh search retracts any dropdown still showing from the previous
        # query: bump the generation token so stale background callbacks are
        # ignored, and tear down the old results frame.
        self._spotlight_generation += 1
        my_gen = self._spotlight_generation
        old_frame = getattr(self, "_spotlight_results_frame", None)
        if old_frame is not None:
            try:
                old_frame.destroy()
            except tk.TclError:
                pass

        # The dropdown grows to fit its results, then stops at the bottom of the
        # screen and scrolls.  A common caption spans many jurisdictions and the
        # court hierarchy can return a dozen rows, so a long list must stay
        # reachable by wheel or arrow keys instead of spilling off-screen.
        max_rows = 10
        pw = 600 if _CTK_AVAILABLE else 580
        row_unit = 58 if _CTK_AVAILABLE else 54  # one row including its top gap
        sx = popup.winfo_screenwidth()
        sy = popup.winfo_screenheight()
        pos_x = (sx - pw) // 2
        pos_y = sy // 3

        # Results below the search bar.  Rows pack into `rows_frame`, which rides
        # inside a scrolling canvas so the list can exceed the visible popup; the
        # "Searching…" status stays pinned at the bottom of `results_frame`.
        if _CTK_AVAILABLE:
            results_frame = ctk.CTkFrame(border, fg_color=_UI["window"])
            results_frame.pack(fill="both", expand=True, padx=2, pady=(0, 8))
            canvas_bg = _UI["window"]
        else:
            results_frame = tk.Frame(border, bg="#f0f0f0")
            results_frame.pack(fill="both", expand=True, padx=2, pady=(0, 2))
            canvas_bg = "#f0f0f0"
        self._spotlight_results_frame = results_frame

        scroll_area = tk.Frame(results_frame, bg=canvas_bg)
        scroll_area.pack(side="top", fill="both", expand=True)
        results_canvas = tk.Canvas(scroll_area, bg=canvas_bg,
                                   highlightthickness=0, height=1)
        results_vsb = ttk.Scrollbar(
            scroll_area, orient="vertical", command=results_canvas.yview,
            style="Modern.Vertical.TScrollbar" if _CTK_AVAILABLE
            else "Vertical.TScrollbar",
        )
        results_canvas.configure(yscrollcommand=results_vsb.set)
        results_canvas.pack(side="left", fill="both", expand=True)
        if _CTK_AVAILABLE:
            rows_frame = ctk.CTkFrame(results_canvas, fg_color=_UI["window"])
        else:
            rows_frame = tk.Frame(results_canvas, bg="#f0f0f0")
        rows_window = results_canvas.create_window((0, 0), window=rows_frame,
                                                   anchor="nw")
        results_canvas.bind(
            "<Configure>",
            lambda e: results_canvas.itemconfigure(rows_window, width=e.width),
        )

        # Wheel scrolling.  The pointer usually rests on a row, not the canvas
        # gaps, so the handlers bind to the canvas here and to every row as it
        # streams in (see _add_result); covers Windows/macOS (<MouseWheel>) and
        # X11 (<Button-4/5>).
        def _wheel(direction: int) -> None:
            try:
                results_canvas.yview_scroll(direction, "units")
            except tk.TclError:
                pass

        def _on_wheel(e) -> None:
            _wheel(-1 if getattr(e, "delta", 0) > 0 else 1)

        def _bind_wheel(widget) -> None:
            _bind_recursive(widget, "<MouseWheel>", _on_wheel)
            _bind_recursive(widget, "<Button-4>", lambda _e: _wheel(-1))
            _bind_recursive(widget, "<Button-5>", lambda _e: _wheel(1))

        _bind_wheel(results_canvas)

        def _resize_to(n_rows: int = 0) -> None:
            """Grow the popup to fit the rows, capped at the screen bottom; past
            the cap the viewport holds its height and the extra rows scroll."""
            try:
                popup.update_idletasks()
                content_h = rows_frame.winfo_reqheight()
                # Measure the popup's natural (uncapped) height by letting the
                # canvas request the whole content, then clamp to the screen.
                results_canvas.configure(height=max(1, content_h),
                                         scrollregion=(0, 0, pw, content_h))
                popup.update_idletasks()
                natural = border.winfo_reqheight()
                max_h = sy - pos_y - 40
                if natural <= max_h:
                    if results_vsb.winfo_ismapped():
                        results_vsb.pack_forget()
                    results_canvas.yview_moveto(0.0)
                    popup.geometry(f"{pw}x{natural}+{pos_x}+{pos_y}")
                else:
                    results_canvas.configure(
                        height=max(row_unit, content_h - (natural - max_h)))
                    if not results_vsb.winfo_ismapped():
                        results_vsb.pack(side="right", fill="y")
                    popup.geometry(f"{pw}x{max_h}+{pos_x}+{pos_y}")
            except tk.TclError:
                pass

        def _scroll_into_view(idx: int) -> None:
            """Keep the keyboard-selected row visible as Up/Down move past the
            edge of the scrolling viewport."""
            if not (0 <= idx < len(result_rows)):
                return
            try:
                popup.update_idletasks()
                w = result_rows[idx]["row"]
                total = max(1, rows_frame.winfo_reqheight())
                top = w.winfo_y()
                bot = top + w.winfo_height()
                vtop = results_canvas.canvasy(0)
                vh = results_canvas.winfo_height()
                if top < vtop:
                    results_canvas.yview_moveto(top / total)
                elif bot > vtop + vh:
                    results_canvas.yview_moveto(max(0.0, bot - vh) / total)
            except tk.TclError:
                pass

        _resize_to(0)

        # Tracking state
        result_rows: list[dict] = []
        selected_idx = [-1]  # mutable via closure
        # Cross-source de-duplication: a signature per case already shown (with
        # the bucket it came from and its row, so a preferred source can take
        # the row over), and how many each source/court-tier has shown.
        shown: list[dict] = []
        bucket_counts: dict[str, int] = {}

        def _detail_text(cite: str, year: str, source_label: str) -> str:
            detail = f"{cite}" if cite else ""
            if year:
                detail = f"{detail} ({year})" if detail else f"({year})"
            if source_label:
                sep = "  ·  " if _CTK_AVAILABLE else "  — "
                detail = (f"{detail}{sep}{source_label}"
                          if detail else source_label)
            return detail

        def _replace_row(r: dict, cite: str, year: str, source_label: str,
                         open_fn) -> None:
            # A preferred source (Google Scholar) took over an already-shown
            # row: swap its opener and re-label the detail line.  Badge and name
            # stay — it is the same case, so they already match.
            r["open_fn"] = open_fn
            try:
                r["detail"].configure(
                    text=_detail_text(cite, year, source_label))
            except tk.TclError:
                pass

        def _add_result(bucket: str, court_id: str, name: str, cite: str,
                        year: str, source_label: str, open_fn) -> None:
            # Ignore results streaming in from a superseded search.
            if my_gen != self._spotlight_generation:
                return
            try:
                if not popup.winfo_exists():
                    return
            except tk.TclError:
                return

            # De-duplicate against the cases already on screen.  When the new
            # result is the same case as one shown, it either takes the row over
            # — a Google Scholar hit is the app's first-choice opener, so it
            # replaces a CourtListener row — or is dropped; either way the two
            # identities merge so a later hit de-duplicates against every form
            # seen.  A reverse-caption match carries no name key, so it
            # de-duplicates only by citation and sits beside the as-captioned
            # case rather than being eaten by it.
            sig = _case_signature(name, cite, year, court_id,
                                  include_name=(bucket != "reversed"))
            for entry in shown:
                if _same_case(sig, entry["sig"]):
                    if _prefer_source(bucket, entry["bucket"]):
                        _replace_row(entry["row"], cite, year, source_label,
                                     open_fn)
                        entry["bucket"] = bucket
                    entry["sig"]["cites"] |= sig["cites"]
                    entry["sig"]["series"] |= sig["series"]
                    entry["sig"]["court"] = entry["sig"]["court"] or sig["court"]
                    return

            # A genuinely new case: honour the overall row limit and the
            # source's per-tier display cap.
            if len(result_rows) >= max_rows:
                return
            if bucket and bucket_counts.get(bucket, 0) >= _BUCKET_CAPS.get(
                    bucket, 99):
                return

            # Append a fresh row at the bottom.  Rows are added in arrival order
            # and never moved, so a result already on screen keeps its position
            # while the dropdown grows downward to fit the new one.
            court_abbr = _COURT_BLUEBOOK.get(
                court_id, court_id.upper() if court_id else "?"
            )
            if court_id == "scotus":
                court_abbr = "SCOTUS"
            elif court_id == "engrep":
                court_abbr = "Eng. Rep."

            display_name = name[:80] + ("…" if len(name) > 80 else "")
            detail = _detail_text(cite, year, source_label)

            r = self._spot_build_row(
                rows_frame, court_abbr,
                self._spot_tier_color(court_id), display_name, detail,
            )
            _bind_wheel(r["row"])
            r["open_fn"] = open_fn
            result_rows.append(r)
            shown.append({"sig": sig, "bucket": bucket, "row": r})
            if bucket:
                bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
            this_idx = len(result_rows) - 1

            def on_click(_e=None, _r=r) -> None:
                self._close_quick_popup()
                _r["open_fn"]()

            _bind_recursive(r["row"], "<Button-1>", on_click)
            # Hovering a row selects it, so mouse and keyboard share one
            # highlight and a click always opens the row under the pointer.
            def on_hover(_e=None, i=this_idx) -> None:
                selected_idx[0] = i
                _highlight(i)
            _bind_recursive(r["row"], "<Enter>", on_hover)

            # Grow the dropdown to fit the row just added.
            _resize_to(len(result_rows))

        def _highlight(idx: int) -> None:
            for i, r in enumerate(result_rows):
                self._spot_highlight_row(r, i == idx)

        # Keyboard-selection guard: Return opens the highlighted row only
        # while the entry still shows the text these results answered — or
        # the text as it stood when the user last pressed Up/Down (a
        # deliberate signal they're choosing among the rows on screen, even
        # mid-edit).  Once the text is changed without arrow navigation,
        # Return runs a fresh search instead; only a mouse click (or renewed
        # Up/Down) picks from the now-stale list.  This keeps a row that
        # merely streamed in under the resting pointer (hover selects) from
        # hijacking a re-typed search.
        nav_text = [query]

        def _on_key(event) -> None:
            if not result_rows:
                return
            if event.keysym == "Down":
                nav_text[0] = entry.get().strip()
                selected_idx[0] = min(selected_idx[0] + 1,
                                      len(result_rows) - 1)
                _highlight(selected_idx[0])
                _scroll_into_view(selected_idx[0])
            elif event.keysym == "Up":
                nav_text[0] = entry.get().strip()
                selected_idx[0] = max(selected_idx[0] - 1, 0)
                _highlight(selected_idx[0])
                _scroll_into_view(selected_idx[0])
            elif event.keysym == "Return":
                if 0 <= selected_idx[0] < len(result_rows):
                    self._close_quick_popup()
                    result_rows[selected_idx[0]]["open_fn"]()

        entry.bind("<Down>", _on_key)
        entry.bind("<Up>", _on_key)
        # Override Return to select from dropdown once results exist
        def _entry_return(_e=None) -> None:
            current = entry.get().strip()
            if (result_rows and selected_idx[0] >= 0
                    and current in (query, nav_text[0])):
                # A result is highlighted and the text hasn't been changed
                # since the search (or the last arrow-key move) — open it.
                self._spotlight_empty_returns = 0
                _on_key(type("E", (), {"keysym": "Return"})())
                return
            if current:
                # New (or edited) query, or no still-valid selection: retract
                # the current dropdown and run a fresh search in the spotlight
                # interface rather than opening a stale row or jumping to the
                # main window.
                self._spotlight_empty_returns = 0
                self._show_spotlight_dropdown(popup, border, entry, current)
                return
            # Empty search bar: open the main window only on the second
            # consecutive Return.
            self._spotlight_empty_returns += 1
            if self._spotlight_empty_returns >= 2:
                self._open_main_from_spotlight(popup)
        entry.bind("<Return>", _entry_return)

        # Status label, pinned at the bottom so the result rows above it stay
        # put as results stream in.
        if _CTK_AVAILABLE:
            status_lbl = ctk.CTkLabel(
                results_frame, text="Searching…", fg_color="transparent",
                text_color=_UI["muted"], font=_ui_font(11), anchor="w",
            )
            status_lbl.pack(side="bottom", fill="x", padx=14, pady=(6, 8))
        else:
            status_lbl = tk.Label(
                results_frame, text="Searching…", bg="#f0f0f0", fg="#999999",
                font=("TkDefaultFont", 8), anchor="w",
            )
            status_lbl.pack(side="bottom", fill="x", padx=8, pady=(4, 4))
        # Re-fit now that the status line is packed, so it shows during the
        # initial "Searching…" state (the first _resize_to ran before it).
        _resize_to(0)
        search_done = [0]  # track how many searches completed
        # Do not start or wait for the lazy opinion database here.  If it was
        # already loaded, search it as an independent fourth Spotlight source;
        # otherwise the three existing sources proceed unchanged.
        loaded_opinion_db = self._loaded_opinion_db()
        total_searches = 3 + (1 if loaded_opinion_db is not None else 0)

        def _update_status() -> None:
            if my_gen != self._spotlight_generation:
                return
            try:
                if not popup.winfo_exists():
                    return
                n = len(result_rows)
                if search_done[0] >= total_searches:
                    status_lbl.configure(
                        text=f"{n} results" if n else "No results found"
                    )
                else:
                    status_lbl.configure(text=f"{n} results so far…")
            except tk.TclError:
                pass

        # Federal Appendix citations: Google Scholar and CourtListener both
        # mismatch these scans to the wrong case, so when the query *is* an
        # F. App'x citation, skip those searches and offer the static.case.law
        # PDF built straight from the citation the user typed.
        cite_m = _LINE_CITE_RE.search(query)
        is_reporter_cite = bool(cite_m)
        # A nominative early-SCOTUS cite gets a modern U.S. Reports fallback.
        # Keep the full query for Scholar, but give CL's exact endpoint just
        # the citation token it expects.
        case_search_queries = _citation_search_variants(query)
        lookup_cites = _citation_search_variants(
            cite_m.group(0) if cite_m else query
        )
        appx_url = (_static_case_law_url(cite_m.group(0))
                    if cite_m and _FED_APPX_RE.search(cite_m.group(0)) else None)
        if appx_url:
            cite_label = re.sub(r"\s+", " ", cite_m.group(0)).strip()

            def _open_appx(u=appx_url, t=cite_label):
                _open_case_law_pdf(
                    self.root, u, t, self._status_var.set, app=self,
                )

            self.root.after(
                0, _add_result, "appx", "", f"{cite_label} — Federal Appendix",
                cite_label, "", "case.law PDF", _open_appx,
            )
            search_done[0] = total_searches  # nothing else runs for an F. App'x cite
            self.root.after(0, _update_status)
            return

        # Launch Scholar and CL searches in parallel
        def scholar_search() -> None:
            if not _SCHOLAR_AVAILABLE:
                search_done[0] += 1
                self.root.after(0, _update_status)
                return
            fetcher = self._get_scholar()
            if fetcher is None:
                search_done[0] += 1
                self.root.after(0, _update_status)
                return
            results = []
            for search_query in case_search_queries:
                try:
                    candidate_results = fetcher.search_cases(
                        search_query, limit=10,
                    )
                except Exception:
                    candidate_results = []
                if candidate_results:
                    results = candidate_results
                    break
            if is_reporter_cite:
                # A reporter citation in the query pins the case, so keep
                # Google Scholar's own relevance order (top few).
                results = results[:3]
            else:
                # Just a name: Google Scholar, like CourtListener, ranks on
                # the whole opinion text, so rank the entire first page of
                # its results the way the CourtListener name passes are
                # ranked (see _cl_name_search): score each title's closeness
                # to the query and drop the weak ones, order by match tier
                # (the caption as typed beats a swapped caption beats a
                # one-party match) then by closeness — Scholar's own
                # relevance order stands in for CourtListener's
                # citation-count tiebreak — and keep only the best tier
                # present, as _filter_to_best_tier does across the pooled CL
                # passes.  Up to the best four are shown (the scholar bucket
                # cap); a couple of spares are kept past those so a duplicate
                # of a case another source already listed can be replaced.
                scored = [
                    (_match_tier(query, getattr(r, "title", "") or ""),
                     _name_match_score(query, getattr(r, "title", "") or ""),
                     i, r)
                    for i, r in enumerate(results)
                ]
                scored = [t for t in scored if t[1] >= _NAME_MATCH_MIN]
                scored.sort(key=lambda t: (-t[0], -t[1], t[2]))
                best_tier = scored[0][0] if scored else -1
                results = [r for tier, _s, _i, r in scored
                           if tier == best_tier]
                results = results[:_BUCKET_CAPS["scholar"] + 2]
            for r in results:
                court_id = _scholar_source_to_court_id(r.source)
                year = _scholar_source_year(r.source)
                # The case's own reporter citation sits in the source byline.
                cite = _scholar_result_cite(r)

                def make_opener(sr=r, cite=cite):
                    def open_it():
                        f = self._get_scholar()
                        if f is None:
                            return

                        def run():
                            try:
                                res = f.fetch_by_url(sr.url)
                            except Exception as exc:
                                print(f"[scholar] open {sr.url!r} failed: {exc}")
                                res = None
                            if res:
                                url, html = res

                                def show(u=url, h=html):
                                    try:
                                        _ScholarTextWindow(
                                            self.root, self, u, h, item=None,
                                        )
                                    except tk.TclError:
                                        pass
                                self._post_root(show)
                            else:
                                # Opinion page didn't load — show CourtListener
                                # and retry Scholar in the background.
                                self._post_root(
                                    self._scholar_case_fallback, sr.url, cite,
                                    "", True, sr.title,
                                )
                        threading.Thread(target=run, daemon=True).start()
                    return open_it

                self.root.after(
                    0, _add_result, "scholar", court_id, r.title, cite, year,
                    "Scholar", make_opener(),
                )
            search_done[0] += 1
            self.root.after(0, _update_status)

        def cl_search() -> None:
            client = (
                self._get_client()
                if self._token_var.get().strip() else None
            )
            if client is None:
                search_done[0] += 1
                self.root.after(0, _update_status)
                return

            if is_reporter_cite:
                # A reporter citation ("514 F. App'x 210"): resolve it precisely
                # via citation-lookup first — full-text search often mismatches
                # a bare citation — and fall back to a plain search only if that
                # finds nothing, dropping SCOTUS "order" entries as the main
                # search does.
                results: list[dict] = []
                for lookup_cite in lookup_cites:
                    try:
                        candidate_results = []
                        for entry in client.lookup_citation(lookup_cite):
                            if entry.get("status") != 200:
                                continue
                            for cl in entry.get("clusters") or []:
                                candidate_results.append(_item_from_cluster(cl))
                    except Exception:
                        candidate_results = []
                    if candidate_results:
                        results = candidate_results
                        break
                if not results:
                    for search_query in case_search_queries:
                        try:
                            data = client.search(
                                search_query, type="o", page_size=10,
                            )
                            candidate_results = [
                                it for it in (data.get("results") or [])
                                if not _is_scotus_order_item(it)
                            ]
                        except Exception:
                            candidate_results = []
                        if candidate_results:
                            results = candidate_results
                            break
                    results = results[:3]
                tagged = [("cl", it) for it in results]
            else:
                # Just words (a case name): CourtListener gives the name no
                # weight over the body text, so rank by case-name closeness
                # across the court hierarchy — Supreme Court (up to 3), federal
                # courts of appeals (up to 2), state courts of last resort (up
                # to 2) — or a single named jurisdiction when the query gives
                # one ("... (7th Cir. 2009)").  Each result is tagged with the
                # court tier that caps how many of it are shown.
                tagged = _cl_name_ranked_search(client, query)

            for bucket, item in tagged:
                case_name = re.sub(
                    r"<[^>]+>", "",
                    item.get("caseName") or item.get("case_name") or "",
                ).strip()
                court_id = str(
                    item.get("court_id") or item.get("court") or ""
                ).strip().lower()
                cite_str = _pick_citation(item.get("citation", []))
                date = item.get("dateFiled") or item.get("date_filed") or ""
                year = date[:4] if len(date) >= 4 else ""

                def make_opener(it=item, nm=case_name):
                    def open_it():
                        fetcher = (
                            self._get_scholar()
                            if _SCHOLAR_AVAILABLE else None
                        )
                        c = self._get_client()

                        def run():
                            # Federal Appendix cases are scans that Google
                            # Scholar almost never has — open straight on the
                            # static.case.law PDF instead of falling back to the
                            # (often wrong) CourtListener text.
                            if _item_is_fed_appx(it):
                                self._post_root(self._open_fed_appx_pdf, it)
                                return
                            # Same Scholar-first / CourtListener-fallback flow
                            # as the main window: show Scholar only when its
                            # first case verifies against the CourtListener text.
                            self._scholar_first_worker(it, fetcher, c)
                        threading.Thread(target=run, daemon=True).start()
                    return open_it

                self.root.after(
                    0, _add_result, bucket, court_id, case_name, cite_str,
                    year, "CourtListener", make_opener(),
                )
            search_done[0] += 1
            self.root.after(0, _update_status)

        def opinion_db_search() -> None:
            """Surface saved Scholar opinions even when live Scholar succeeds.

            ``GoogleScholarFetcher.search_cases`` keeps its database lookup as
            an offline fallback.  Spotlight is different: a loaded personal
            corpus is useful in its own right, so these at-most-three rows are
            searched, Bluebook-named, ranked, and streamed independently.
            """
            db = loaded_opinion_db
            try:
                hits = _opinion_db_spotlight_results(db, query, limit=3)
                for hit in hits:
                    name = str(hit.get("name") or "").strip()
                    court_id = str(hit.get("court") or "").strip().lower()
                    year = str(hit.get("year") or "").strip()
                    cites = hit.get("cites") or []
                    cite = str(hit.get("cite") or (cites[0] if cites else ""))
                    sid = str(hit.get("scholar_id") or "").strip()
                    item = {
                        "caseName": name,
                        "citation": list(cites),
                        "dateFiled": f"{year}-01-01" if year else "",
                        "court_id": court_id,
                    }

                    def make_opener(scholar_id=sid, db_item=item):
                        def open_it():
                            def run() -> None:
                                try:
                                    rec = db.get_by_scholar_id(scholar_id) or {}
                                except Exception as exc:
                                    print(f"[spotlight-db] open failed: {exc}")
                                    rec = {}
                                html = rec.get("html") or ""
                                url = rec.get("url") or (
                                    f"https://scholar.google.com/"
                                    f"scholar_case?case={scholar_id}"
                                    if scholar_id else ""
                                )
                                if not html:
                                    self._post_root(
                                        self._spotlight_notify,
                                        "That saved opinion could not be read.",
                                    )
                                    return

                                def show() -> None:
                                    try:
                                        _ScholarTextWindow(
                                            self.root, self, url, html,
                                            item=db_item,
                                        )
                                    except tk.TclError:
                                        pass

                                self._post_root(show)

                            threading.Thread(target=run, daemon=True).start()
                        return open_it

                    self.root.after(
                        0, _add_result, "opiniondb", court_id, name, cite,
                        year, "Opinion database", make_opener(),
                    )
            finally:
                search_done[0] += 1
                self.root.after(0, _update_status)

        # English Reports name search: offline, against our own index, so a
        # pre-1865 English case (which Scholar/CL often lack a clean scan of)
        # surfaces by name next to the online results.  search_by_name is
        # strict — a U.S. or post-1865 name returns nothing — so this row only
        # appears for a genuine English Reports case.
        def engrep_search() -> None:
            try:
                cases = eng_rep.search_by_name(query, limit=1)
            except Exception:
                cases = []
            for case in cases:
                def make_opener(c=case):
                    def open_it():
                        _open_eng_rep_case(
                            self.root, c, self._status_var.set, app=self,
                        )
                    return open_it

                # No year column: ERCase.year is CommonLII's neutral-cite year,
                # which for older cases isn't the decision year (Entick is
                # "[1799] EngR 236" but was decided 1765).  The E.R. citation
                # already identifies the case, so show it without a wrong year.
                self.root.after(
                    0, _add_result, "engrep", "engrep", case.name,
                    case.er_cite, "", "English Reports", make_opener(),
                )
            search_done[0] += 1
            self.root.after(0, _update_status)

        threading.Thread(target=scholar_search, daemon=True).start()
        threading.Thread(target=cl_search, daemon=True).start()
        if loaded_opinion_db is not None:
            threading.Thread(target=opinion_db_search, daemon=True).start()
        threading.Thread(target=engrep_search, daemon=True).start()

    @staticmethod
    def _win_force_foreground(popup: tk.Misc) -> bool:
        """Force *popup* to the foreground on Windows, defeating the
        foreground-lock that stops a background process from stealing focus.

        Returns True if the window became the foreground window.  Several
        Win32 quirks are handled here that the naive approach gets wrong:

          * 64-bit window handles must be passed through ``wintypes.HWND``
            argtypes or ctypes truncates them to 32 bits, corrupting the
            handle so ``SetForegroundWindow`` silently fails.
          * the real OS top-level window is obtained with ``GetAncestor``
            from ``winfo_id`` (``wm_frame`` is unreliable on Windows).
          * the system foreground-lock timeout is temporarily set to 0 so
            the call is honored even though we're not the active app.
        """
        try:
            import ctypes
            from ctypes import wintypes
        except Exception:
            return False

        try:
            user32 = ctypes.windll.user32
            kernel32 = ctypes.windll.kernel32

            # Signatures — critical so 64-bit HWNDs survive the call.
            user32.GetForegroundWindow.restype = wintypes.HWND
            user32.GetWindowThreadProcessId.argtypes = [
                wintypes.HWND, ctypes.POINTER(wintypes.DWORD)
            ]
            user32.GetWindowThreadProcessId.restype = wintypes.DWORD
            user32.AttachThreadInput.argtypes = [
                wintypes.DWORD, wintypes.DWORD, wintypes.BOOL
            ]
            user32.AttachThreadInput.restype = wintypes.BOOL
            user32.BringWindowToTop.argtypes = [wintypes.HWND]
            user32.SetForegroundWindow.argtypes = [wintypes.HWND]
            user32.SetForegroundWindow.restype = wintypes.BOOL
            user32.SetActiveWindow.argtypes = [wintypes.HWND]
            user32.SetActiveWindow.restype = wintypes.HWND
            user32.SetFocus.argtypes = [wintypes.HWND]
            user32.SetFocus.restype = wintypes.HWND
            user32.GetAncestor.argtypes = [wintypes.HWND, wintypes.UINT]
            user32.GetAncestor.restype = wintypes.HWND
            user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
            user32.IsIconic.argtypes = [wintypes.HWND]
            user32.SystemParametersInfoW.argtypes = [
                wintypes.UINT, wintypes.UINT, ctypes.c_void_p, wintypes.UINT
            ]
            user32.SystemParametersInfoW.restype = wintypes.BOOL

            GA_ROOT = 2
            SW_SHOW, SW_RESTORE = 5, 9
            SPI_GETFGLOCK, SPI_SETFGLOCK = 0x2000, 0x2001
            SPIF_SENDCHANGE = 0x0002

            # winfo_id() can be a child wrapper; GetAncestor gives the
            # actual OS top-level window SetForegroundWindow expects.
            hwnd = user32.GetAncestor(wintypes.HWND(popup.winfo_id()), GA_ROOT)
            if not hwnd:
                hwnd = popup.winfo_id()

            user32.ShowWindow(
                hwnd, SW_RESTORE if user32.IsIconic(hwnd) else SW_SHOW
            )

            # Clear the foreground-lock timeout for the duration of the call.
            old_timeout = wintypes.DWORD(0)
            user32.SystemParametersInfoW(
                SPI_GETFGLOCK, 0, ctypes.byref(old_timeout), 0
            )
            user32.SystemParametersInfoW(
                SPI_SETFGLOCK, 0, ctypes.c_void_p(0), SPIF_SENDCHANGE
            )

            fg_win = user32.GetForegroundWindow()
            fg_thread = user32.GetWindowThreadProcessId(fg_win, None)
            our_thread = kernel32.GetCurrentThreadId()

            attached = False
            if fg_thread and fg_thread != our_thread:
                attached = bool(
                    user32.AttachThreadInput(fg_thread, our_thread, True)
                )
            user32.BringWindowToTop(hwnd)
            user32.SetForegroundWindow(hwnd)
            user32.SetActiveWindow(hwnd)
            user32.SetFocus(hwnd)
            if attached:
                user32.AttachThreadInput(fg_thread, our_thread, False)

            # Restore the user's original foreground-lock timeout.
            user32.SystemParametersInfoW(
                SPI_SETFGLOCK, 0,
                ctypes.c_void_p(old_timeout.value), SPIF_SENDCHANGE,
            )
            return bool(user32.GetForegroundWindow() == hwnd)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Settings dialog
    # ------------------------------------------------------------------

    def _post_root(self, fn, *args) -> None:
        try:
            self.root.after(0, fn, *args)
        except tk.TclError:
            pass

    def _post_case_law_pdf(self, url: str, cite: str, pin: str = "",
                           name: str = "", expected_name: str = "") -> None:
        title = f"{name} — {cite}" if name and cite else (cite or name)
        if pin:
            title += f" at {pin}"

        def open_pdf() -> None:
            self._status_var.set(f"Opening {cite or name} (case.law PDF)…")
            _open_case_law_pdf(
                self.root, url, title, self._status_var.set, app=self,
                expected_name=expected_name,
            )

        self._post_root(open_pdf)

    def _try_open_citation(self, name: str, cite: str, pin: str,
                           fetcher, client, prefetch_pdf: bool = True) -> bool:
        """Resolve one case citation and open its window (call from a
        worker thread).  Google Scholar by citation first — retrying as a
        name+citation search — with a pin-cite jump; then the
        CourtListener text.  Returns False when nothing was found.

        ``prefetch_pdf=False`` opens the Scholar/CL text without warming the
        official PDF in the background — used by the PDF brief viewer, where a
        second PDF load alongside the open brief can hang the app."""
        # Federal Appendix cases are scans Google Scholar rarely has — open the
        # static.case.law PDF built straight from the citation.
        if _FED_APPX_RE.search(cite):
            url = _static_case_law_url(cite)
            if url:
                title = f"{name} — {cite}" if name else cite
                self._post_root(
                    lambda u=url, t=title: _open_case_law_pdf(
                        self.root, u, t, self._status_var.set, app=self,
                    )
                )
                return True
        if fetcher is not None:
            result = None
            try:
                result = fetcher.fetch_by_citation(cite)
                if not result and name:
                    # Accept a name+cite search hit only when the *result*
                    # itself is this case — bearing the cite in its title or
                    # byline, or matching the case name — never merely
                    # because the search query found something (any opinion
                    # quoting the cite also matches the query).
                    hits = fetcher.search_cases(f"{name} {cite}", limit=3)
                    for hit in hits:
                        if _scholar_bears_citation(hit, cite) or (
                            _name_match_score(name, hit.title or "")
                            >= _NAME_MATCH_MIN
                        ):
                            result = fetcher.fetch_by_url(hit.url)
                            break
                # Scholar sometimes indexes early Supreme Court opinions only
                # under the modern U.S. Reports citation.  Keep the literal
                # cite first (it can be a different reporter), then retry its
                # verified counterpart only when that lookup found nothing.
                alt_cite = _us_reports_cite(cite)
                if not result and alt_cite:
                    result = fetcher.fetch_by_citation(alt_cite)
                    if not result and name:
                        hits = fetcher.search_cases(
                            f"{name} {alt_cite}", limit=3,
                        )
                        for hit in hits:
                            if _scholar_bears_citation(hit, alt_cite) or (
                                _name_match_score(name, hit.title or "")
                                >= _NAME_MATCH_MIN
                            ):
                                result = fetcher.fetch_by_url(hit.url)
                                break
            except Exception as exc:
                print(f"[citelist] scholar {cite!r}: {exc}")
            if result:
                url, html = result

                def open_scholar() -> None:
                    try:
                        w = _ScholarTextWindow(self.root, self, url, html,
                                               item=None,
                                               prefetch_pdf=prefetch_pdf)
                        if pin:
                            w.jump_to_cite_page(cite, pin)
                    except tk.TclError:
                        pass

                self._post_root(open_scholar)
                return True
        # Google Scholar found the case on its results page but the opinion
        # page didn't load — fall back to CourtListener now and retry this exact
        # Scholar opinion in the background (the toggle lights up if it comes).
        retry_url = ""
        if fetcher is not None:
            try:
                retry_url = fetcher.take_post_search_failure() or ""
            except Exception:
                retry_url = ""
        if client is not None:
            try:
                target = _cl_item_for_citation(client, cite, name=name)
                if target:
                    parts, blocks, plain, cluster = _assemble_case_parts(
                        client, target,
                    )
                    if parts or plain:
                        def open_cl() -> None:
                            try:
                                w = _ScholarTextWindow(
                                    self.root, self, "", "",
                                    item=target, cl_text=plain,
                                    cl_parts=parts, cl_blocks=blocks,
                                    prefetch_pdf=prefetch_pdf,
                                )
                                if retry_url:
                                    w._retry_scholar_link(cite, pin, retry_url)
                            except tk.TclError:
                                pass

                        self._post_root(open_cl)
                        return True
            except Exception as exc:
                print(f"[citelist] courtlistener {cite!r}: {exc}")
        pdf = _case_law_pdf_for_cite(cite) if cite else None
        if pdf:
            self._post_case_law_pdf(pdf, cite, pin, name)
            return True
        return False

    def _show_citation_list_dialog(self) -> None:
        """Dialog that opens a batch of cases: one citation per line
        ("Monroe v. Pape, 365 U.S. 167, 171 (1961)").  Each line is
        resolved on Google Scholar first (jumping to the pin cite when
        the text is paginated by that reporter), falling back to the
        CourtListener text; the lines that resolved nowhere are listed."""
        dlg = tk.Toplevel(self.root)
        dlg.title("Open Citation List")
        dlg.geometry("560x420")
        dlg.minsize(440, 320)
        frame = ttk.Frame(dlg, padding=10)
        frame.pack(fill="both", expand=True)
        ttk.Label(
            frame,
            text="One citation per line — case name optional, pin cite "
                 "after the page number:",
        ).pack(anchor="w")
        ttk.Label(
            frame, foreground="gray",
            text="e.g.  Monroe v. Pape, 365 U.S. 167, 171 (1961)",
        ).pack(anchor="w", pady=(0, 4))
        box = tk.Text(frame, height=9, wrap="none", undo=True)
        box.pack(fill="both", expand=True)
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=(6, 0))
        open_btn = ttk.Button(row, text="Open All")
        open_btn.pack(side="left")
        status_var = tk.StringVar()
        ttk.Label(row, textvariable=status_var, foreground="gray").pack(
            side="left", padx=8, fill="x", expand=True
        )
        fail_box = tk.Text(frame, height=4, foreground="#a31515",
                           state="disabled")

        def post(fn, *args) -> None:
            try:
                self.root.after(0, fn, *args)
            except tk.TclError:
                pass

        def set_status(s: str) -> None:
            try:
                status_var.set(s)
            except tk.TclError:
                pass

        def show_failures(lines: list[str]) -> None:
            try:
                fail_box.config(state="normal")
                fail_box.delete("1.0", "end")
                fail_box.insert("1.0", "\n".join(lines))
                fail_box.config(state="disabled")
                fail_box.pack(fill="x", pady=(6, 0))
            except tk.TclError:
                pass

        def go() -> None:
            raw = [ln.strip() for ln in box.get("1.0", "end").splitlines()]
            lines = [ln for ln in raw if ln]
            if not lines:
                status_var.set("Nothing to open.")
                return
            entries, failures = [], []
            for ln in lines:
                parsed = _parse_citation_line(ln)
                if parsed:
                    entries.append((ln,) + parsed)
                else:
                    failures.append(f"{ln}   (no citation recognized)")
            fetcher = self._get_scholar() if _SCHOLAR_AVAILABLE else None
            client = (
                self._get_client()
                if self._token_var.get().strip() else None
            )
            if fetcher is None and client is None:
                status_var.set("Neither Google Scholar nor CourtListener "
                               "is available.")
                return
            open_btn.config(state="disabled")
            n, opened = len(entries), [0]

            def run() -> None:
                for i, (ln, name, cite, pin) in enumerate(entries, 1):
                    post(set_status, f"({i}/{n}) Searching {cite}…")
                    if self._try_open_citation(name, cite, pin,
                                               fetcher, client):
                        opened[0] += 1
                    else:
                        failures.append(ln)

                def finish() -> None:
                    try:
                        open_btn.config(state="normal")
                    except tk.TclError:
                        return
                    if failures:
                        set_status(
                            f"Opened {opened[0]} of {len(lines)}; "
                            f"{len(failures)} not found:"
                        )
                        show_failures(failures)
                    else:
                        set_status(f"Opened all {opened[0]} citation(s).")

                post(finish)

            threading.Thread(target=run, daemon=True).start()

        open_btn.config(command=go)

    def _show_quick_lookup(self) -> None:
        """Ctrl+S: one-line lookup that takes either a case citation
        (resolved exactly like a line of the citation-list dialog, pin
        cite included) or a statute/regulation citation."""
        dlg = tk.Toplevel(self.root)
        dlg.title("Quick Look Up")
        dlg.resizable(False, False)
        frame = ttk.Frame(dlg, padding=12)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Citation:").grid(row=0, column=0, sticky="w")
        query_var = tk.StringVar()
        entry = ttk.Entry(frame, textvariable=query_var, width=46)
        entry.grid(row=0, column=1, padx=6, sticky="we")
        entry.focus_set()
        status_var = tk.StringVar(
            value="e.g.  Monroe v. Pape, 365 U.S. 167, 171   ·   "
                  "42 USC 1983(b)   ·   29 CFR 1614.105(a)"
        )
        ttk.Label(frame, textvariable=status_var, foreground="gray").grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(8, 0)
        )
        open_btn = ttk.Button(frame, text="Open")
        open_btn.grid(row=0, column=2)
        frame.columnconfigure(1, weight=1)

        def set_status(s: str) -> None:
            try:
                status_var.set(s)
            except tk.TclError:
                pass

        def go(_e=None) -> None:
            q = query_var.get().strip()
            if not q:
                return
            # Statute/regulation first: "42 USC 1983" would otherwise
            # read as volume 42, reporter "USC", page 1983
            statute = _parse_statute_query(q)
            if statute:
                _open_statute_action(
                    self.root, statute, set_status, app=self
                )
                return
            parsed = _parse_citation_line(q)
            if not parsed:
                set_status("Couldn't read that — try a reporter citation "
                           "or '42 USC 1983'.")
                return
            name, cite, pin = parsed
            fetcher = self._get_scholar() if _SCHOLAR_AVAILABLE else None
            client = (
                self._get_client()
                if self._token_var.get().strip() else None
            )
            if fetcher is None and client is None:
                set_status("Neither Google Scholar nor CourtListener "
                           "is available.")
                return
            open_btn.config(state="disabled")
            set_status(f"Searching {cite}…")

            def run() -> None:
                ok = self._try_open_citation(name, cite, pin,
                                             fetcher, client)

                def finish() -> None:
                    try:
                        open_btn.config(state="normal")
                    except tk.TclError:
                        return
                    set_status(f"Opened {cite}." if ok
                               else f"Not found: {cite}")

                self._post_root(finish)

            threading.Thread(target=run, daemon=True).start()

        open_btn.config(command=go)
        entry.bind("<Return>", go)

    def _show_statute_lookup(self) -> None:
        """Small dialog that opens a statute, regulation or federal rule by
        typed citation ("42 USC 1983(b)", "29 CFR 1614.105(a)",
        "Fed. R. Evid. 404(b)") in the statute viewer."""
        dlg = tk.Toplevel(self.root)
        dlg.title("Look Up Statute / Regulation / Rule")
        dlg.resizable(False, False)
        frame = ttk.Frame(dlg, padding=12)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Citation:").grid(row=0, column=0, sticky="w")
        query_var = tk.StringVar()
        entry = ttk.Entry(frame, textvariable=query_var, width=38)
        entry.grid(row=0, column=1, padx=6, sticky="we")
        entry.focus_set()
        status_var = tk.StringVar(
            value="e.g.  42 USC 1983(b)   ·   Fed. R. Evid. 404(b)   ·   "
                  "Cal. Penal Code 187   ·   Fla. Stat. 776.012"
        )
        ttk.Label(frame, textvariable=status_var, foreground="gray").grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(8, 0)
        )

        def go(_e=None) -> None:
            parsed = _parse_statute_query(query_var.get())
            if not parsed:
                status_var.set(
                    "Couldn't read that — try '42 USC 1983', "
                    "'29 CFR 1614.105(a)', 'Fed. R. Evid. 404(b)' or "
                    "'Cal. Penal Code 187'."
                )
                return
            # Parent on the root so the statute window outlives the dialog.
            # (A state we only link out to opens in the browser instead.)
            _open_statute_action(
                self.root, parsed, status_var.set, app=self
            )

        ttk.Button(frame, text="Look Up", command=go).grid(row=0, column=2)
        entry.bind("<Return>", go)
        frame.columnconfigure(1, weight=1)

    def _show_eng_rep_search(self) -> None:
        """Search the English Reports by case name (or E.R. citation), listing
        every decent match — the many-results counterpart of the spotlight's
        single strict English Reports row.  Modeless: opening a case leaves
        the box up for the next search."""
        dlg = _ui_toplevel(self.root)
        _ensure_modern_ttk_styles(dlg)
        dlg.title("Search English Reports")
        dlg.geometry(_fit_toplevel_geometry(dlg, 720, 460,
                                            min_width=520, min_height=300))
        dlg.minsize(520, 300)

        top = _ui_frame(dlg)
        top.pack(fill="x", padx=14, pady=(12, 0))
        query_var = tk.StringVar()
        entry = _ui_entry(top, textvariable=query_var)
        entry.pack(side="left", fill="x", expand=True)
        search_btn = _ui_button(top, "Search", primary=True, width=92)
        search_btn.pack(side="left", padx=(8, 0))

        box = _ui_frame(dlg, card=True)
        box.pack(fill="both", expand=True, padx=12, pady=(10, 8))
        sb_style = ("Modern.Vertical.TScrollbar" if _CTK_AVAILABLE
                    else "Vertical.TScrollbar")
        lb_kw = dict(activestyle="dotbox", borderwidth=0, highlightthickness=0)
        if _CTK_AVAILABLE:
            lb_kw.update(bg=_UI["window"], fg=_UI["text"],
                         selectbackground=_UI["selection"],
                         selectforeground=_UI["text"],
                         font=("TkDefaultFont", 11))
        lb = tk.Listbox(box, **lb_kw)
        sb = ttk.Scrollbar(box, orient="vertical", command=lb.yview,
                           style=sb_style)
        lb.configure(yscrollcommand=sb.set)
        pad = 8 if _CTK_AVAILABLE else 0
        sb.pack(side="right", fill="y", pady=pad, padx=(0, pad))
        lb.pack(side="left", fill="both", expand=True, padx=(pad, 0), pady=pad)

        btns = _ui_frame(dlg)
        btns.pack(fill="x", padx=14, pady=(0, 12))
        open_btn = _ui_button(btns, "Open", primary=True, width=92)
        open_btn.pack(side="right")
        status_var = tk.StringVar(
            value="Case name (e.g. Hadley v Baxendale) or citation "
                  "(156 Eng. Rep. 145).")
        _install_status_label(btns, status_var, padx=(0, 10))

        results: "list[eng_rep.ERCase]" = []   # rows behind the listbox lines
        gen = [0]                    # search generation; stale results dropped

        def show(cases: "list[eng_rep.ERCase]", note: str) -> None:
            results[:] = cases
            lb.delete(0, "end")
            for c in cases:
                lb.insert("end", f"{c.name}  ·  {c.er_cite}  ·  {c.neutral}")
            if cases:
                lb.selection_set(0)
            status_var.set(note)

        def search(_e=None) -> None:
            q = query_var.get().strip()
            if not q:
                return
            gen[0] += 1
            my_gen = gen[0]
            status_var.set("Searching…")

            def run() -> None:
                try:
                    m = eng_rep.ER_CITE_RE.search(q)
                    if m:
                        vol, page = int(m.group(1)), int(m.group(2))
                        cases = eng_rep.lookup(vol, page)
                        note = (f"{len(cases)} case(s) at {vol} E.R. {page}."
                                if cases else
                                f"{vol} Eng. Rep. {page} isn't in the index.")
                    else:
                        cap = 250
                        cases = _eng_rep_name_search(q, cap)
                        if not cases:
                            note = ("No matching English Reports case."
                                    if eng_rep.is_available() else
                                    "The English Reports index isn't "
                                    "available.")
                        elif len(cases) == cap:
                            note = (f"Showing the best {cap} matches — add a "
                                    "word to narrow.")
                        else:
                            note = (f"{len(cases)} matches." if len(cases) > 1
                                    else "1 match.")
                except Exception as exc:
                    cases, note = [], f"Search failed: {exc}"

                def post() -> None:
                    if my_gen == gen[0] and lb.winfo_exists():
                        show(cases, note)

                try:
                    self.root.after(0, post)
                except tk.TclError:
                    pass  # app closed while searching

            threading.Thread(target=run, daemon=True).start()

        def open_selected(_e=None) -> None:
            sel = lb.curselection()
            if sel and results:
                # Parent on the root so the viewer outlives this box.
                _open_eng_rep_case(self.root, results[sel[0]],
                                   self._status_var.set, app=self)

        search_btn.configure(command=search)
        open_btn.configure(command=open_selected)
        entry.bind("<Return>", search)
        lb.bind("<Double-Button-1>", open_selected)
        lb.bind("<Return>", open_selected)
        dlg.bind("<Escape>", lambda _e: dlg.destroy())
        entry.focus_set()

    # ------------------------------------------------------------------
    # Opinion database — find / open / merge / rebuild
    # ------------------------------------------------------------------

    def _open_db_record(self, scholar_id: str) -> None:
        """Open an opinion stored in the database, by its Scholar id."""
        db = self._get_opinion_db()
        if db is None:
            return
        rec = db.get_by_scholar_id(scholar_id)
        if not rec or not rec.get("html"):
            messagebox.showwarning(
                "Not in Database",
                "That opinion is no longer in the database.",
            )
            return
        self._open_scholar_window(
            rec.get("url", ""), rec["html"], None, None, "from database", True,
        )

    def _show_db_find(self) -> None:
        """Search the local opinion database by party name, reporter citation,
        or Google Scholar number — without touching the network."""
        db = self._get_opinion_db()
        if db is None:
            messagebox.showwarning(
                "Database Unavailable",
                "The opinion database could not be opened.",
            )
            return
        dlg = tk.Toplevel(self.root)
        dlg.title("Find Opinion in Database")
        dlg.resizable(False, False)
        frame = ttk.Frame(dlg, padding=12)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Search:").grid(row=0, column=0, sticky="w")
        query_var = tk.StringVar()
        entry = ttk.Entry(frame, textvariable=query_var, width=46)
        entry.grid(row=0, column=1, padx=6, sticky="we")
        entry.focus_set()
        status_var = tk.StringVar(
            value=f"{db.count()} opinions stored   ·   "
                  "e.g.  Roe v. Wade   ·   410 U.S. 113   ·   <Scholar number>"
        )
        ttk.Label(frame, textvariable=status_var, foreground="gray").grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(8, 0)
        )
        frame.columnconfigure(1, weight=1)

        def go(_e=None) -> None:
            q = query_var.get().strip()
            if not q:
                return
            try:
                hits = db.find(q)
            except Exception as exc:
                status_var.set(f"Search error: {exc}")
                return
            if not hits:
                status_var.set(f"No opinion in the database for {q!r}.")
                return
            # Even a single hit goes through the picker — it carries the
            # Open / Refresh / Delete actions for the stored opinion.
            dlg.destroy()
            _DbMatchDialog(self.root, self, hits)

        ttk.Button(frame, text="Find", command=go).grid(row=0, column=2)
        entry.bind("<Return>", go)

    def _merge_db_file(self) -> None:
        """Merge another ``opinions.jsonl`` into this one (e.g. one pulled from
        GitHub or shared by a colleague).  De-duped by Scholar number."""
        db = self._get_opinion_db()
        if db is None:
            messagebox.showwarning(
                "Database Unavailable",
                "The opinion database could not be opened.",
            )
            return
        path = filedialog.askopenfilename(
            title="Choose an opinions.jsonl to merge in",
            filetypes=[("Opinion database", "*.jsonl"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            same = Path(path).resolve() == Path(db.jsonl_path).resolve()
        except Exception:
            same = False
        if same:
            messagebox.showinfo(
                "Merge", "That is already the current database file."
            )
            return
        self._status_var.set("Merging database file…")

        def run() -> None:
            try:
                stats = db.merge_from(path)
            except Exception as exc:
                self._post_root(
                    lambda e=exc: messagebox.showerror("Merge Failed", str(e))
                )
                self._post_root(lambda: self._status_var.set("Merge failed."))
                return

            def done() -> None:
                self._status_var.set(
                    f"Merged: +{stats['added']} new, "
                    f"{stats['skipped']} already present."
                )
                msg = (
                    f"Added {stats['added']} new opinion(s).\n"
                    f"Skipped {stats['skipped']} already in the database."
                )
                if stats["errors"]:
                    msg += f"\n{stats['errors']} line(s) could not be read."
                if stats["added"]:
                    msg += (
                        f"\n\nRemember to commit {Path(db.jsonl_path).name} "
                        "to Git to sync the change."
                    )
                messagebox.showinfo("Merge Complete", msg)

            self._post_root(done)

        threading.Thread(target=run, daemon=True).start()

    def _rebuild_db_index(self) -> None:
        """Rebuild the local SQLite search index from ``opinions.jsonl`` (use
        after editing it by hand or resolving a Git merge)."""
        db = self._get_opinion_db()
        if db is None:
            messagebox.showwarning(
                "Database Unavailable",
                "The opinion database could not be opened.",
            )
            return
        self._status_var.set("Rebuilding search index…")

        def run() -> None:
            try:
                db.rebuild_index()
                n = db.count()
            except Exception as exc:
                self._post_root(
                    lambda e=exc: messagebox.showerror("Rebuild Failed", str(e))
                )
                return
            self._post_root(
                lambda: self._status_var.set(f"Search index rebuilt — {n} opinions.")
            )
            self._post_root(
                lambda: messagebox.showinfo(
                    "Index Rebuilt",
                    f"Search index rebuilt from {Path(db.jsonl_path).name}.\n"
                    f"{n} opinion(s) indexed.",
                )
            )

        threading.Thread(target=run, daemon=True).start()

    # ------------------------------------------------------------------
    # Open Brief — load a brief and highlight every citation in it
    # ------------------------------------------------------------------

    def _open_brief(self) -> None:
        """Ctrl+B / Brief menu: pick a brief and open it with its citations
        highlighted and clickable.  PDF, Word, RTF and text briefs are all read
        to text and shown in the same reader (a PDF's text layer is extracted;
        scanned PDFs with no text layer have nothing to detect)."""
        path = filedialog.askopenfilename(
            title="Open a brief",
            parent=self.root,
            filetypes=[
                ("Briefs", "*.pdf *.docx *.doc *.rtf *.txt"),
                ("PDF", "*.pdf"),
                ("Word document", "*.docx *.doc"),
                ("Rich Text", "*.rtf"),
                ("Text", "*.txt"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return
        self._open_brief_text(path)

    def _open_brief_text(self, path: str) -> None:
        try:
            text = brief_reader.extract_text(path)
        except Exception as exc:
            messagebox.showerror(
                "Open Brief", f"Could not read this file:\n\n{exc}",
                parent=self.root,
            )
            return
        if not text.strip():
            messagebox.showwarning(
                "Open Brief",
                "No selectable text was found in this file.  If it's a scanned "
                "document (images only), its citations can't be detected.",
                parent=self.root,
            )
            return
        if path.lower().endswith(".pdf"):
            try:
                with open(path, "rb") as fh:
                    _brief_cache_record(fh.read(), os.path.basename(path),
                                        "text")
            except Exception as exc:
                print(f"[brief-cache] could not cache {path}: {exc}")
        _BriefTextWindow(self.root, self, os.path.basename(path), text)

    def _open_linked_pdf(self) -> None:
        """Brief menu: import a PDF and show it *as a PDF* with its citations
        detected and drawn as clickable links on the page (cases / statutes /
        rules / regulations / Constitution open in the app).  Falls back to the
        text reader when the in-app PDF viewer libraries aren't installed."""
        path = filedialog.askopenfilename(
            title="Import a PDF to link its citations",
            parent=self.root,
            filetypes=[("PDF", "*.pdf"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except Exception as exc:
            messagebox.showerror(
                "Import PDF", f"Could not read this file:\n\n{exc}",
                parent=self.root,
            )
            return
        pdf_data = _normalize_pdf_bytes(data)
        if pdf_data is None:
            messagebox.showerror(
                "Import PDF", "That doesn't look like a PDF file.",
                parent=self.root,
            )
            return
        _brief_cache_record(pdf_data, os.path.basename(path), "linked")
        _LinkedPdfWindow(self.root, self, pdf_data, os.path.basename(path))

    def _fill_recent_briefs_menu(self) -> None:
        """Rebuild Brief ▸ Recent Briefs from the 30-day cache."""
        menu = self._recent_briefs_menu
        menu.delete(0, "end")
        entries = _brief_cache_entries()
        if not entries:
            menu.add_command(
                label="(no briefs opened in the last 30 days)",
                state="disabled",
            )
            return
        for e in entries:
            opened = time.strftime(
                "%b %d", time.localtime(e.get("last_opened") or 0))
            label = f"{e.get('name') or 'brief.pdf'}   ·   {opened}"
            menu.add_command(
                label=label[:80],
                command=lambda e=e: self._reopen_recent_brief(e),
            )

    def _reopen_recent_brief(self, entry: dict) -> None:
        """Open a brief from the cache, restarting its 30-day clock."""
        data = _brief_cache_read(entry)
        name = entry.get("name") or "brief.pdf"
        if data is None:
            messagebox.showwarning(
                "Recent Briefs",
                f"The cached copy of “{name}” is no longer available.",
                parent=self.root,
            )
            _brief_cache_entries()  # prune
            return
        _brief_cache_record(data, name, entry.get("mode") or "linked")
        if entry.get("mode") == "text":
            self._open_brief_from_bytes(data, name)
        else:
            _LinkedPdfWindow(self.root, self, data, name)

    def _open_brief_from_bytes(self, data: bytes, name: str) -> None:
        """Text-reader fallback for an imported PDF when the in-app PDF viewer
        isn't available: write the bytes out, extract the text layer, and show
        the same clickable citations in the brief reader."""
        tmp: Optional[str] = None
        try:
            fd, tmp = tempfile.mkstemp(suffix=".pdf")
            os.close(fd)
            with open(tmp, "wb") as fh:
                fh.write(data)
            text = brief_reader.extract_text(tmp)
        except Exception as exc:
            messagebox.showerror(
                "Import PDF", f"Could not read this PDF:\n\n{exc}",
                parent=self.root,
            )
            return
        finally:
            if tmp:
                try:
                    os.unlink(tmp)
                except Exception:
                    pass
        if not text.strip():
            messagebox.showwarning(
                "Import PDF",
                "No selectable text was found in this PDF (it may be a scan).",
                parent=self.root,
            )
            return
        _BriefTextWindow(self.root, self, name, text)

    _HOTKEY_MODIFIERS = {
        "Shift_L", "Shift_R", "Control_L", "Control_R", "Alt_L", "Alt_R",
        "Meta_L", "Meta_R", "Command", "Command_L", "Command_R", "Win_L",
        "Win_R",
    }
    _HOTKEY_SPECIAL_KEYS = {
        "space": "space", "Return": "enter", "Escape": "esc",
        "BackSpace": "backspace", "Tab": "tab", "Delete": "delete",
        "Up": "up", "Down": "down", "Left": "left", "Right": "right",
        "Home": "home", "End": "end", "Prior": "page_up",
        "Next": "page_down", "Insert": "insert",
    }

    def _stop_global_hotkey(self) -> None:
        if self._hotkey_listener is None:
            return
        if sys.platform == "darwin":
            return
        try:
            self._hotkey_listener.stop()
        except Exception:
            pass
        self._hotkey_listener = None

    def _hotkey_from_event(self, event) -> Optional[str]:
        keysym = getattr(event, "keysym", "") or ""
        if keysym in self._HOTKEY_MODIFIERS:
            return None
        state = int(getattr(event, "state", 0) or 0)
        mods: list[str] = []
        if state & 0x0004:
            mods.append("<ctrl>")
        if state & 0x0008:
            mods.append("<alt>")
        # Tk reports Command/Windows as Mod2/Mod4 depending on platform/theme.
        if state & (0x0010 if sys.platform == "darwin" else 0x0040):
            mods.append("<cmd>")
        if state & 0x0001:
            mods.append("<shift>")

        if re.fullmatch(r"F\d{1,2}", keysym):
            key = f"<{keysym.lower()}>"
        elif keysym in self._HOTKEY_SPECIAL_KEYS:
            key = f"<{self._HOTKEY_SPECIAL_KEYS[keysym]}>"
        elif len(keysym) == 1 and keysym.isalnum():
            key = keysym.lower()
        else:
            ch = getattr(event, "char", "") or ""
            key = ch.lower() if len(ch) == 1 and ch.isprintable() else ""
        if not key:
            return ""
        if not mods and not re.fullmatch(r"<f\d{1,2}>", key):
            return ""
        return "+".join(mods + [key])

    def _apply_spotlight_hotkey(self, hotkey: str) -> bool:
        old = getattr(self, "_spotlight_hotkey", _default_spotlight_hotkey())
        self._spotlight_hotkey = hotkey
        if sys.platform == "darwin" and self._hotkey_listener is not None:
            _save_spotlight_hotkey(hotkey)
            return True
        self._setup_global_hotkey()
        if _HOTKEY_AVAILABLE and self._hotkey_listener is None:
            self._spotlight_hotkey = old
            self._setup_global_hotkey()
            return False
        _save_spotlight_hotkey(hotkey)
        return True

    def _show_spotlight_shortcut_dialog(self) -> None:
        if not _HOTKEY_AVAILABLE:
            messagebox.showinfo(
                "Spotlight Shortcut",
                "Global shortcuts need the pynput package.",
                parent=self.root,
            )
            return

        if sys.platform != "darwin":
            self._stop_global_hotkey()
        dlg = _ui_toplevel(self.root)
        dlg.title("Spotlight Shortcut")
        dlg.geometry("500x190" if _CTK_AVAILABLE else "480x170")
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.transient(self.root)

        outer = _ui_frame(dlg)
        outer.pack(fill="both", expand=True, padx=16, pady=14)
        _ui_label(outer, "Press the new shortcut", size=14, weight="bold",
                  anchor="w").pack(fill="x")
        _ui_label(
            outer,
            "Use a modifier combo such as Ctrl+Alt+K, or a function key.",
            size=11, muted=True, anchor="w",
        ).pack(fill="x", pady=(2, 10))
        current_var = tk.StringVar(
            value=f"Current: {_display_hotkey(self._spotlight_hotkey)}"
        )
        _ui_label(outer, size=12, anchor="w", textvariable=current_var).pack(
            fill="x", pady=(0, 6)
        )
        status_var = tk.StringVar(
            value=(
                "Waiting for keys... On macOS, changes apply after relaunch."
                if sys.platform == "darwin" else "Waiting for keys..."
            )
        )
        _ui_label(outer, size=11, muted=True, anchor="w",
                  textvariable=status_var).pack(fill="x")

        def close(restart: bool = True) -> None:
            try:
                dlg.grab_release()
            except tk.TclError:
                pass
            try:
                dlg.destroy()
            except tk.TclError:
                pass
            if restart:
                self._setup_global_hotkey()

        def on_key(event) -> str:
            modifier_bits = 0x0001 | 0x0004 | 0x0008 | 0x0010 | 0x0040
            if event.keysym == "Escape" and not (event.state & modifier_bits):
                close()
                return "break"
            hotkey = self._hotkey_from_event(event)
            if hotkey is None:
                return "break"
            if not hotkey:
                status_var.set("Use Ctrl, Alt, Shift, Cmd, or a function key.")
                return "break"
            if self._apply_spotlight_hotkey(hotkey):
                msg = f"Spotlight shortcut: {_display_hotkey(hotkey)}"
                if sys.platform == "darwin":
                    msg += " (after relaunch)"
                self._status_var.set(msg)
                close(restart=False)
            else:
                status_var.set("That shortcut could not be registered.")
            return "break"

        btn_frame = _ui_frame(dlg)
        btn_frame.pack(fill="x", padx=16, pady=(0, 14))
        _ui_button(
            btn_frame, "Reset to Default", width=132,
            command=lambda: (
                self._apply_spotlight_hotkey(_default_spotlight_hotkey()),
                close(restart=False),
            ),
        ).pack(side="left")
        _ui_button(btn_frame, "Cancel", command=close, width=88).pack(
            side="right"
        )

        dlg.bind("<KeyPress>", on_key, add="+")
        dlg.protocol("WM_DELETE_WINDOW", close)
        dlg.after(50, dlg.focus_force)

    def _show_settings_dialog(self) -> None:
        dlg = _ui_toplevel(self.root)
        dlg.title("Settings")
        dlg.geometry("480x210" if _CTK_AVAILABLE else "460x175")
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.transient(self.root)

        outer = _ui_frame(dlg)
        outer.pack(fill="both", expand=True, padx=16, pady=14)
        _ui_label(outer, "CourtListener API Token", size=14, weight="bold",
                  anchor="w").pack(fill="x")
        _ui_label(outer, "Used for CourtListener search and text retrieval.",
                  size=11, muted=True, anchor="w").pack(fill="x", pady=(2, 10))

        entry = _ui_entry(outer, textvariable=self._token_var, show="*")
        entry.pack(fill="x")

        show_var = tk.BooleanVar(value=False)

        def _toggle() -> None:
            entry.configure(show="" if show_var.get() else "*")

        _ui_checkbox(outer, "Show token", show_var, _toggle).pack(
            anchor="w", pady=(8, 0))

        btn_frame = _ui_frame(dlg)
        btn_frame.pack(fill="x", padx=16, pady=(0, 14))
        _ui_button(
            btn_frame, "Save & Close", primary=True, width=120,
            command=lambda: (_save_token(self._token_var.get().strip()),
                             dlg.destroy()),
        ).pack(side="right")
        _ui_button(btn_frame, "Cancel", command=dlg.destroy, width=88).pack(
            side="right", padx=8
        )

    # ------------------------------------------------------------------
    # Court picker
    # ------------------------------------------------------------------

    def _show_court_picker(self) -> None:
        _CourtPickerDialog(self.root, self._selected_courts, self._on_courts_applied)

    def _on_courts_applied(self, selected: set[str]) -> None:
        # Selecting everything is the same as no filter
        if selected >= _all_court_ids():
            selected = set()
        self._selected_courts = selected
        if not selected:
            self._courts_btn_var.set("Courts: All ▾")
        elif len(selected) == 1:
            cid = next(iter(selected))
            label = "SCOTUS" if cid == "scotus" else _COURT_BLUEBOOK.get(cid, cid)
            self._courts_btn_var.set(f"Courts: {label} ▾")
        else:
            self._courts_btn_var.set(f"Courts: {len(selected)} selected ▾")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    _COL_LABELS = {
        "case_name": "Case Name",
        "court": "Court",
        "date_filed": "Date Filed",
        "citation": "Citation",
        "status": "Status",
    }

    def _configure_tree_columns(self, tree: ttk.Treeview) -> None:
        for col, label in self._COL_LABELS.items():
            tree.heading(
                col, text=label,
                command=lambda c=col, t=tree: self._sort_tree(t, c),
            )
        tree.column("case_name", width=310, minwidth=150)
        tree.column("court", width=70, minwidth=50, anchor="center")
        tree.column("date_filed", width=85, minwidth=70, anchor="center")
        tree.column("citation", width=140, minwidth=80)
        tree.column("status", width=110, minwidth=70)

    def _sort_tree(self, tree: ttk.Treeview, col: str) -> None:
        """Sort *tree* by *col*, toggling direction on repeated clicks."""
        current_col, reverse = self._sort_state.get(id(tree), (None, False))
        reverse = (not reverse) if col == current_col else False
        self._sort_state[id(tree)] = (col, reverse)

        rows = [(tree.set(iid, col), iid) for iid in tree.get_children("")]
        rows.sort(key=lambda x: x[0].lower(), reverse=reverse)
        for idx, (_, iid) in enumerate(rows):
            tree.move(iid, "", idx)

        # Update headings to show the active sort indicator.
        for c, label in self._COL_LABELS.items():
            if c == col:
                label += "  ▼" if reverse else "  ▲"
            tree.heading(c, text=label)

    def _format_row(self, item: dict) -> tuple:
        """Return the tuple of column values for inserting a row into the tree."""
        case_name = item.get("caseName") or item.get("case_name") or "(unknown)"
        case_name = re.sub(r"<[^>]+>", "", case_name).strip()
        court = item.get("court") or item.get("court_id") or ""
        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        citation_str = _pick_citation(item.get("citation", []))
        status = item.get("status") or item.get("precedentialStatus") or ""
        return (case_name, court, date_filed, citation_str, status)

    def _iid_to_idx(self, iid: str) -> int:
        """Convert a tree row iid to an index into self._results."""
        return int(iid)

    def _get_selected_item(self) -> Optional[tuple[int, dict]]:
        """Return (index, result-dict) for whichever tree has a selection."""
        for tree in (self._tree, self._orders_tree):
            sel = tree.selection()
            if sel:
                idx = self._iid_to_idx(sel[0])
                return idx, self._results[idx]
        return None

    def _on_row_select(self, source_tree: ttk.Treeview) -> None:
        sel = source_tree.selection()
        if not sel:
            return
        # Deselect the other trees so only one row is ever active
        other = self._orders_tree if source_tree is self._tree else self._tree
        if other.selection():
            other.selection_remove(*other.selection())
        if self._scholar_tree.selection():
            self._scholar_tree.selection_remove(*self._scholar_tree.selection())
        self._download_btn.config(state="normal")
        self._scholar_btn.configure(state="normal")
        self._show_preview(self._iid_to_idx(sel[0]))

    def _on_scholar_row_select(self) -> None:
        sel = self._scholar_tree.selection()
        if not sel:
            return
        for tree in (self._tree, self._orders_tree):
            if tree.selection():
                tree.selection_remove(*tree.selection())
        self._download_btn.config(state="disabled")  # no CourtListener record
        self._scholar_btn.configure(state="normal")
        idx = int(sel[0])
        if 0 <= idx < len(self._scholar_results):
            r = self._scholar_results[idx]
            self._set_preview(r.snippet or "(no snippet on the results page)")

    def _selected_scholar_result(self):
        sel = self._scholar_tree.selection()
        if sel:
            idx = int(sel[0])
            if 0 <= idx < len(self._scholar_results):
                return self._scholar_results[idx]
        return None

    def _on_right_click(self, event: tk.Event, tree: ttk.Treeview) -> None:
        """Right-click: open the 'Citing Opinions' window for the clicked row."""
        iid = tree.identify_row(event.y)
        if not iid:
            return
        tree.selection_set(iid)
        idx = self._iid_to_idx(iid)
        if 0 <= idx < len(self._results):
            item = self._results[idx]
            _CitingOpinionsWindow(self.root, self, item)

    def _get_client(self) -> Optional[CourtListenerClient]:
        token = self._token_var.get().strip()
        if not token:
            messagebox.showerror(
                "Missing Token",
                "Please enter your CourtListener API token.\n\n"
                "Go to Settings → API Token…",
            )
            return None
        if self._client is None or self._client._session.headers.get(
            "Authorization"
        ) != f"Token {token}":
            self._client = CourtListenerClient(api_token=token)
            _save_token(token)
        return self._client

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _do_search(self) -> None:
        if self._search_thread and self._search_thread.is_alive():
            return

        query = self._query_var.get().strip()
        if not query:
            messagebox.showwarning("Empty Query", "Please enter a search query.")
            return

        # A CourtListener token is needed for the CourtListener results only.
        # Without one, alert the user but still let Google Scholar (and the
        # other resources) search — just skip the CourtListener half.
        token = self._token_var.get().strip()
        if token:
            client = self._get_client()
            if client is None:
                return
        else:
            client = None
            messagebox.showwarning(
                "No CourtListener Token",
                "No CourtListener API token is set — CourtListener results will "
                "be skipped.\n\nGoogle Scholar will still search.  Add a token "
                "under Settings → API Token… to include CourtListener.",
            )

        # CourtListener accepts space-separated court IDs; empty set = all
        court = " ".join(sorted(self._selected_courts)) or None
        date_from = self._date_from_var.get().strip() or None
        date_to = self._date_to_var.get().strip() or None
        page_size = self._page_size_var.get()

        # Clear previous results
        self._search_btn.config(state="disabled")
        self._download_btn.config(state="disabled")
        self._scholar_btn.configure(state="disabled")
        self._status_var.set("Searching…")
        for row in self._tree.get_children():
            self._tree.delete(row)
        for row in self._orders_tree.get_children():
            self._orders_tree.delete(row)
        for row in self._scholar_tree.get_children():
            self._scholar_tree.delete(row)
        self._results.clear()
        self._scholar_results = []
        self._preview_cache.clear()
        self._set_preview("")

        # Google Scholar search runs in parallel with the CourtListener one
        if _SCHOLAR_AVAILABLE:
            # Build the fetcher with the opinion database attached so a blocked
            # Scholar search can fall back to local results.
            fetcher = self._get_scholar()
            self._scholar_status_var.set("Searching…")

            def scholar_run() -> None:
                try:
                    res = fetcher.search_cases(query, limit=15)
                except Exception as exc:
                    print(f"[scholar] search failed: {exc}")
                    res = []
                self.root.after(0, self._on_scholar_search_results, res)

            threading.Thread(target=scholar_run, daemon=True).start()
        else:
            self._scholar_status_var.set("(needs beautifulsoup4)")

        # No CourtListener token: the Scholar search above is all there is to
        # run, so re-enable the controls and stop here.
        if client is None:
            self._search_btn.config(state="normal")
            self._status_var.set(
                "Searching Google Scholar — CourtListener skipped (no token)."
                if _SCHOLAR_AVAILABLE else
                "CourtListener skipped (no token); Google Scholar unavailable."
            )
            return

        def run() -> None:
            try:
                data = client.search(
                    query,
                    type="o",
                    court=court,
                    date_filed_min=date_from,
                    date_filed_max=date_to,
                    highlight=True,
                    page_size=page_size,
                )
                self.root.after(0, self._on_results, data)
            except CourtListenerError as exc:
                self.root.after(0, self._on_error, str(exc))
            except Exception as exc:
                self.root.after(0, self._on_error, f"Unexpected error: {exc}")

        self._search_thread = threading.Thread(target=run, daemon=True)
        self._search_thread.start()

    def _on_results(self, data: dict) -> None:
        self._search_btn.config(state="normal")
        results = data.get("results", [])
        count = data.get("count", len(results))
        self._results = results
        # Normalize citations from the API: strip any HTML tags (<mark>, etc.)
        # immediately so every downstream consumer gets clean plain-text strings.
        for item in results:
            raw = item.get("citation")
            if isinstance(raw, list):
                item["citation"] = [re.sub(r"<[^>]+>", "", c).strip() for c in raw]
            elif raw:
                item["citation"] = re.sub(r"<[^>]+>", "", str(raw)).strip()

        for i, item in enumerate(results):
            # Each search result has an 'opinions' list.  The opinion with the
            # most outbound citations is the main opinion for this cluster.
            opinions = item.get("opinions") or []
            main_op = max(opinions, key=lambda o: len(o.get("cites") or []), default=None)

            # Preview text comes from the main opinion's snippet field.
            if main_op:
                raw = main_op.get("snippet") or ""
                text = re.sub(r"<[^>]+>", "", raw).strip()
                if text:
                    self._preview_cache[i] = text

            # Route to orders tree only for SCOTUS cases with ≤ 2 outbound
            # citations.  Published orders don't exist for lower courts, so
            # we leave everything else in the main tree.
            court_val = str(item.get("court_id") or "")
            cites_count = len(main_op.get("cites") or []) if main_op else None
            row = self._format_row(item)
            if "scotus" in court_val and cites_count is not None and cites_count <= 2:
                self._orders_tree.insert("", "end", iid=str(i), values=row)
            else:
                self._tree.insert("", "end", iid=str(i), values=row)

        if results:
            self._status_var.set(
                f"Showing {len(results)} of {count:,} results. "
                "Select a row and click Download PDF (or double-click)."
            )
        else:
            self._status_var.set("No results found.")

    def _set_preview(self, text: str) -> None:
        self._preview_text.config(state="normal")
        self._preview_text.delete("1.0", "end")
        self._preview_text.insert("1.0", text)
        self._preview_text.config(state="disabled")

    def _show_preview(self, idx: int) -> None:
        """Populate the preview strip for CourtListener result at *idx*."""
        text = self._preview_cache.get(idx, "")
        self._set_preview(
            text if text else "(No preview available — download PDF for full opinion)"
        )

    def _on_error(self, message: str) -> None:
        self._search_btn.config(state="normal")
        self._status_var.set(f"Error: {message}")
        messagebox.showerror("API Error", message)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _download_selected(self) -> None:
        selected = self._get_selected_item()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a case first.")
            return

        idx, item = selected

        safe_name = _build_default_filename(item)

        save_path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{safe_name}.pdf",
            title="Save Opinion PDF",
        )
        if not save_path:
            return

        client = self._get_client()
        if client is None:
            return

        self._status_var.set("Resolving PDF URL…")
        self._download_btn.config(state="disabled")
        self._scholar_btn.configure(state="disabled")
        self._search_btn.config(state="disabled")

        def run() -> None:
            try:
                print(f"\n[download] raw item keys: {list(item.keys())}")
                print(f"[download] local_path   = {item.get('local_path') or item.get('localPath')!r}")
                print(f"[download] download_url = {item.get('download_url')!r}")
                print(f"[download] cluster_id   = {item.get('cluster_id') or item.get('id')!r}")

                pdf_url = self._resolve_pdf_url(client, item)
                print(f"[download] resolved url = {pdf_url!r}")

                if not pdf_url:
                    # Last-ditch: assemble full case text from CourtListener
                    # (cluster metadata + all sub-opinions) and save as .txt.
                    cluster_id = item.get("cluster_id") or item.get("id")
                    if cluster_id:
                        try:
                            self.root.after(
                                0, self._status_var.set,
                                "No PDF found — fetching opinion text from CourtListener…"
                            )
                            print(f"[download] no PDF found; assembling text for cluster {cluster_id}")
                            text = _assemble_case_text(client, item)
                            if text.strip():
                                txt_path = os.path.splitext(save_path)[0] + ".txt"
                                with open(txt_path, "w", encoding="utf-8") as f:
                                    f.write(text)
                                self.root.after(0, self._on_text_download_done, txt_path)
                                return
                        except Exception as exc:
                            print(f"[download] text assembly failed: {exc}")
                    self.root.after(
                        0,
                        self._on_error,
                        "No downloadable PDF or text found for this opinion.",
                    )
                    return

                self.root.after(0, self._status_var.set, f"Downloading… {pdf_url}")
                print(f"[download] fetching {pdf_url}")
                fetched = _fetch_pdf_bytes(pdf_url, client=client, timeout=60)
                if fetched is None:
                    raise RuntimeError("The source returned something that isn't a PDF.")
                data, pdf_url = fetched
                with open(save_path, "wb") as f:
                    f.write(data)

                self.root.after(0, self._on_download_done, save_path)
            except Exception as exc:
                self.root.after(0, self._on_error, f"Download failed: {exc}")
            finally:
                self.root.after(0, self._restore_buttons)

        threading.Thread(target=run, daemon=True).start()

    def _resolve_pdf_url(
        self, client: CourtListenerClient, item: dict
    ) -> Optional[str]:
        """
        Attempt to find a PDF URL for the selected search result.

        Strategy (local_path always preferred over download_url):
        0. US Reports citation → LOC CDN preferred for vols 1-501 (GovInfo
           backup), GovInfo preferred for vols 502-583 (LOC CDN backup through
           vol 542), then an opinion carved from the Court's own volume PDF
           (vols 584+; fetched from supremecourt.gov once and kept in the
           "US Reports" folder).  Volumes on none of those fall through to
           local_path.
        0.5. Non-SCOTUS cases: try static.case.law (Harvard CAP) first.
             Only falls through if the URL returns a non-200 response.
        1. local_path from the search result (if already present).
        2. Fetch the opinion directly by ID to get its local_path.
        3. download_url from the search result (original court source).
        4. download_url from the fetched opinion record.
        5. Walk the cluster's sub_opinions checking local_path then download_url.
        """
        storage_base = "https://storage.courtlistener.com/"
        item.pop("_case_law_pdf_choices", None)

        # Determine whether this is a SCOTUS case.
        court_val = str(item.get("court_id") or "")
        is_scotus = "scotus" in court_val

        def _head_ok(url: str, label: str) -> bool:
            try:
                session = (
                    client._session if _is_courtlistener_url(url)
                    else _anon_session
                )
                headers = dict(_anon_session.headers)
                if session is not _anon_session:
                    auth = getattr(session, "headers", {}).get("Authorization")
                    if auth:
                        headers["Authorization"] = auth
                resp = session.head(
                    url, timeout=10, allow_redirects=True, headers=headers)
                if resp.status_code == 200:
                    return True
                print(f"[resolve] {label} returned {resp.status_code}: {url}")
            except Exception as exc:
                print(f"[resolve] {label} check failed ({exc}): {url}")
            return False

        # Gather EVERY citation we know — the search result often exposes only
        # one (frequently a nominative reporter like "19 How. 393"), while the
        # parallel U.S./F. cite that finds a PDF lives on the cluster record.
        all_cites = _gather_all_citations(client, item)
        print(f"[resolve] citations to try: {all_cites}")

        # A SCOTUS opinion opened from Google Scholar sometimes carries only its
        # Supreme Court Reporter ("S. Ct.") cite, not the "U.S." cite every
        # official-PDF path below keys on.  Ask CourtListener whether that
        # S. Ct. cite has a parallel U.S. Reports cite and, if so, try it too —
        # that's what lets "View PDF" reach the official opinion scan.
        us_from_sct = _us_reports_cite_via_courtlistener(client, all_cites)
        if us_from_sct and us_from_sct not in all_cites:
            print(f"[resolve] S. Ct. cite resolved to U.S. Reports cite: "
                  f"{us_from_sct}")
            all_cites.append(us_from_sct)

        # 0. Official US Reports PDF — try every U.S.-Reports cite among them.
        #    For vols 1-501 the LOC (Library of Congress) CDN scan is preferred,
        #    with GPO's GovInfo edition only as a backup; for vols 502-583
        #    GovInfo is preferred, with the LOC CDN as backup through vol 542.
        #    Beyond that an opinion is carved out of the Court's bound-volume/
        #    preliminary-print PDF (vols 584+; downloaded from supremecourt.gov
        #    on first use, reused from the "US Reports" folder after).
        def _try_govinfo(cite: str) -> Optional[str]:
            gov = _us_reports_govinfo_url(cite)
            if not gov:
                return None
            link_url, direct_url = gov
            if _head_ok(link_url, "GovInfo link"):
                print(f"[resolve] using GovInfo link URL: {link_url}")
                return link_url
            if _head_ok(direct_url, "GovInfo direct PDF"):
                print(f"[resolve] using GovInfo direct PDF URL: {direct_url}")
                return direct_url
            return None

        def _try_loc(cite: str) -> Optional[str]:
            loc_url = _us_reports_loc_url(cite)
            if loc_url and _head_ok(loc_url, "LOC US Reports"):
                print(f"[resolve] using LOC US Reports PDF: {loc_url}")
                return loc_url
            return None

        for cite in all_cites:
            m = _US_CITE_RE.search(cite)
            loc_preferred = bool(m) and int(m.group(1)) <= _LOC_PREFERRED_MAX
            sources = (
                (_try_loc, _try_govinfo) if loc_preferred
                else (_try_govinfo, _try_loc)
            )
            for source in sources:
                url = source(cite)
                if url is not None:
                    return url
            local_pdf = us_reports_pdf.extract_citation(cite)
            if local_pdf is not None:
                url = local_pdf.as_uri()
                print(f"[resolve] using local US Reports volume: {url}")
                return url

        # 0.5. Non-SCOTUS: the Harvard CAP static.case.law copy.  Try every
        #      parallel cite before giving up; when more than one reporter scan
        #      exists, keep all of them for the case window's View PDF menu.
        if not is_scotus:
            expected_name = re.sub(
                r"<[^>]+>", "",
                str(item.get("caseName") or item.get("case_name") or ""),
            ).strip()
            choices = _case_law_pdf_choices_for_cites(
                all_cites, expected_name=expected_name)
            if choices:
                item["_case_law_pdf_choices"] = choices
                print(f"[resolve] using static.case.law PDF: {choices[0].url}")
                return choices[0].url

        # 1. local_path already present on the search result
        local = item.get("local_path") or item.get("localPath") or ""
        if local:
            url = storage_base + local.lstrip("/")
            if _head_ok(url, "local_path (search result)"):
                print(f"[resolve] using local_path from search result: {local}")
                return url

        # 2. Fetch the opinion directly to get its local_path (preferred over
        #    download_url — CourtListener's stored copy is more reliable than
        #    the original court URL).
        opinion_id = item.get("id")
        fetched_op: Optional[dict] = None
        if opinion_id:
            try:
                print(f"[resolve] fetching opinion {opinion_id} for local_path")
                fetched_op = client.get_opinion(int(opinion_id))
                print(f"[resolve] opinion local_path = {fetched_op.get('local_path')!r}")
                print(f"[resolve] opinion download_url = {fetched_op.get('download_url')!r}")
                local = fetched_op.get("local_path") or ""
                if local:
                    url = storage_base + local.lstrip("/")
                    if _head_ok(url, "local_path (opinion record)"):
                        print(f"[resolve] using local_path from opinion record")
                        return url
            except Exception as exc:
                print(f"[resolve] direct opinion fetch failed: {exc}")

        # 3. download_url from the search result (original court source)
        url = item.get("download_url") or ""
        if url:
            if _head_ok(url, "download_url (search result)"):
                print(f"[resolve] using download_url from search result: {url}")
                return url

        # 4. download_url from the fetched opinion record
        if fetched_op:
            dl = fetched_op.get("download_url") or ""
            if dl:
                if _head_ok(dl, "download_url (opinion record)"):
                    print(f"[resolve] using download_url from opinion record: {dl}")
                    return dl

        # 5. Fall back to cluster → sub_opinions walk
        cluster_id = item.get("cluster_id") or item.get("id")
        if cluster_id:
            try:
                print(f"[resolve] fetching cluster {cluster_id}")
                cluster = client.get_cluster(int(cluster_id), fields="sub_opinions")
                print(f"[resolve] sub_opinions = {cluster.get('sub_opinions')!r}")
                for op_url in cluster.get("sub_opinions", []):
                    print(f"[resolve] fetching sub-opinion {op_url}")
                    op = client._get_url(op_url, {"fields": "download_url,local_path"})
                    print(f"[resolve]   local_path={op.get('local_path')!r}  download_url={op.get('download_url')!r}")
                    local = op.get("local_path") or ""
                    if local:
                        url = storage_base + local.lstrip("/")
                        if _head_ok(url, "local_path (sub-opinion)"):
                            return url
                    dl = op.get("download_url") or ""
                    if dl:
                        if _head_ok(dl, "download_url (sub-opinion)"):
                            return dl
            except Exception as exc:
                print(f"[resolve] cluster walk failed: {exc}")

        return None

    # ------------------------------------------------------------------
    # Google Scholar text fetch
    # ------------------------------------------------------------------

    def _get_scholar(self) -> Optional["GoogleScholarFetcher"]:
        if not _SCHOLAR_AVAILABLE:
            messagebox.showerror(
                "Missing Dependency",
                "Google Scholar fetching requires beautifulsoup4.\n\n"
                "Install it with:\n    pip install beautifulsoup4",
            )
            return None
        db = self._get_opinion_db(wait=False)
        if self._scholar is None:
            # Hand the fetcher the same name matcher used to rank
            # CourtListener/Scholar results, so a blocked search ranks local
            # database candidates the same way (see search_cases' fallback).
            self._scholar = GoogleScholarFetcher(
                db=db,
                name_scorer=_name_match_score,
                name_min=_NAME_MATCH_MIN,
            )
        elif db is not None and hasattr(self._scholar, "set_opinion_db"):
            self._scholar.set_opinion_db(db)
        return self._scholar

    def _attach_opinion_db_to_scholar(self, db) -> None:
        fetcher = self._scholar
        if fetcher is not None and hasattr(fetcher, "set_opinion_db"):
            try:
                fetcher.set_opinion_db(db)
            except Exception as exc:
                print(f"[db] attaching opinion database to Scholar failed: {exc}")

    def _open_opinion_db_sync(self):
        import opinion_db
        db = opinion_db.OpinionDB()
        print(
            f"[db] opinion database ready "
            f"({db.count()} opinions) "
            f"at {db.jsonl_path}"
        )
        return db

    def _start_opinion_db_load(self) -> None:
        """Start opening the opinion DB on a background thread.

        Google Scholar searches can run without the local DB; this keeps the
        first user lookup from waiting on JSONL/index sync. Once the DB is
        ready, the existing Scholar fetcher is updated in-place.
        """
        def run() -> None:
            db = None
            failed = False
            try:
                db = self._open_opinion_db_sync()
            except Exception as exc:
                print(f"[db] opinion database unavailable: {exc}")
                failed = True
            with self._opinion_db_lock:
                self._opinion_db = db
                self._opinion_db_failed = failed
                self._opinion_db_loading = False
                self._opinion_db_thread = None
            if db is not None:
                self._attach_opinion_db_to_scholar(db)

        t = threading.Thread(target=run, daemon=True)
        with self._opinion_db_lock:
            if (self._opinion_db is not None or self._opinion_db_failed
                    or self._opinion_db_loading):
                return
            self._opinion_db_loading = True
            self._opinion_db_thread = t
        t.start()

    def _get_opinion_db(self, wait: bool = True):
        """The searchable opinion database (lazily opened). Returns the
        ``OpinionDB`` or ``None`` if it can't be opened; the app still runs,
        just without the database (opinions then come straight from Scholar).

        ``wait=False`` is for normal Scholar lookups: kick off a background
        open/sync and return immediately so the search can continue against the
        online sources. Explicit database actions keep the default blocking
        behavior because the user asked for the local database itself.
        """
        with self._opinion_db_lock:
            db = self._opinion_db
            failed = self._opinion_db_failed
            loading = self._opinion_db_loading
            thread = self._opinion_db_thread
        if db is not None or failed:
            return db
        if not wait:
            self._start_opinion_db_load()
            return None
        if loading and thread is not None:
            thread.join()
            with self._opinion_db_lock:
                return self._opinion_db
        with self._opinion_db_lock:
            if self._opinion_db is not None or self._opinion_db_failed:
                return self._opinion_db
            self._opinion_db_loading = True
        db = None
        failed = False
        try:
            db = self._open_opinion_db_sync()
        except Exception as exc:
            print(f"[db] opinion database unavailable: {exc}")
            failed = True
        with self._opinion_db_lock:
            self._opinion_db = db
            self._opinion_db_failed = failed
            self._opinion_db_loading = False
            self._opinion_db_thread = None
        if db is not None:
            self._attach_opinion_db_to_scholar(db)
        return db

    def _loaded_opinion_db(self):
        """Return the opinion DB only if its lazy load already finished.

        Spotlight uses this non-starting probe so its local-results pass never
        makes the first quick search wait for (or initiate) an index rebuild.
        """
        with self._opinion_db_lock:
            return self._opinion_db

    # ------------------------------------------------------------------
    # Software update  (Settings ▸ Check for Updates…)
    # ------------------------------------------------------------------

    def _check_for_updates(self) -> None:
        """Ask GitHub whether the main branch is newer than the installed
        version and, if so, offer to download it and restart."""
        self._status_var.set("Checking for updates…")

        def run() -> None:
            try:
                import updater
                res = updater.check(_load_config().get("installed_commit"))
            except Exception as exc:
                res = {"ok": False, "error": f"Update check failed: {exc}"}
            self.root.after(0, lambda: self._on_update_check(res))

        threading.Thread(target=run, daemon=True).start()

    def _on_update_check(self, res: dict) -> None:
        if not res.get("ok"):
            self._status_var.set("Update check failed.")
            messagebox.showerror(
                "Check for Updates",
                res.get("error") or "Could not check for updates.")
            return
        if not res.get("update"):
            self._status_var.set("GetCases is up to date.")
            messagebox.showinfo(
                "Check for Updates",
                res.get("reason") or "You're running the latest version.")
            return
        date = (res.get("latest_date") or "")[:10]
        msg = (
            (res.get("reason") or "A newer version is available.")
            + (f"\nLatest change: {date}" if date else "")
            + "\n\nUpdate now? GetCases will download the latest files from "
            "GitHub and restart.\n\nYour saved opinions will be preserved."
        )
        if messagebox.askyesno("Update Available", msg):
            self._perform_update(res.get("latest_sha") or "")

    def _perform_update(self, latest_sha: str) -> None:
        """Back up saved opinions, download and install the latest files,
        record a pending merge, then relaunch.  Runs off the UI thread; every
        file is verified before anything on disk is overwritten."""
        self._status_var.set("Downloading update…")

        def run() -> None:
            import opinion_db
            import updater
            staging = None
            try:
                # 1) Back up the local opinions file *outside* the app dir, so
                #    the file replacement below cannot touch it.
                backup = None
                jsonl = opinion_db.data_dir() / "opinions.jsonl"
                if jsonl.is_file():
                    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
                    backup = (_CONFIG_PATH.parent
                              / f"opinions.pre-update-{int(time.time())}.jsonl")
                    shutil.copy2(jsonl, backup)
                    # Record the pending merge *before* any file is overwritten,
                    # so even a partial failure still restores saved opinions on
                    # the next launch (the merge is a harmless no-op if the file
                    # was never actually replaced).
                    data = _load_config()
                    data["pending_opinions_merge"] = str(backup)
                    _save_config(data)
                # 2) Download + verify the latest tree, then copy it over the
                #    install (download_and_stage validates before we overwrite).
                staging = Path(tempfile.mkdtemp(prefix="getcases_update_"))
                root = updater.download_and_stage(staging)
                updater.apply_over(root, updater.app_dir())
                # 3) Drop the derived search index so it rebuilds cleanly from
                #    the new opinions.jsonl on next launch.
                for name in ("opinions.index.db", "opinions.index.db-wal",
                             "opinions.index.db-shm"):
                    try:
                        (opinion_db.data_dir() / name).unlink()
                    except OSError:
                        pass
                # 4) Record the new version now that the install is in place.
                data = _load_config()
                data["installed_commit"] = latest_sha
                _save_config(data)
                self.root.after(0, self._finish_update_relaunch)
            except Exception as exc:
                self.root.after(0, lambda e=exc: self._update_failed(e))
            finally:
                if staging is not None:
                    shutil.rmtree(staging, ignore_errors=True)

        threading.Thread(target=run, daemon=True).start()

    def _finish_update_relaunch(self) -> None:
        self._status_var.set("Update installed — restarting…")
        import updater
        try:
            updater.relaunch()
        finally:
            try:
                self.root.destroy()
            except Exception:
                pass

    def _update_failed(self, exc: Exception) -> None:
        self._status_var.set("Update failed.")
        messagebox.showerror(
            "Update Failed",
            f"The update could not be completed:\n\n{exc}\n\n"
            "Your saved opinions were not changed.")

    def _apply_pending_update_merge(self) -> None:
        """After an update+restart, merge the pre-update opinions backup into
        the freshly downloaded opinions.jsonl so locally saved cases survive,
        then delete the backup.  Non-fatal — retried next launch on failure."""
        data = _load_config()
        backup = data.get("pending_opinions_merge")
        if not backup:
            return
        backup_path = Path(backup)
        try:
            if backup_path.is_file():
                db = self._get_opinion_db()
                if db is None:
                    return  # DB unavailable now; keep flag and retry next launch
                stats = db.merge_from(backup_path)
                print(f"[update] restored saved opinions: {stats}")
                self._status_var.set(
                    f"Update complete — {stats.get('added', 0)} saved "
                    "opinion(s) preserved.")
                try:
                    backup_path.unlink()
                except OSError:
                    pass
            # Clear the flag (merged, or the backup is already gone).
            data = _load_config()
            data.pop("pending_opinions_merge", None)
            _save_config(data)
        except Exception as exc:
            print(f"[update] post-update merge failed "
                  f"(will retry next launch): {exc}")

    def _on_scholar_search_results(self, results: list) -> None:
        self._scholar_results = results
        for row in self._scholar_tree.get_children():
            self._scholar_tree.delete(row)
        for i, r in enumerate(results):
            self._scholar_tree.insert("", "end", iid=str(i), values=(r.title, r.source))
        self._scholar_status_var.set(
            f"{len(results)} results" if results else "no results (blocked?)"
        )

    def _open_selected_scholar_result(self) -> None:
        r = self._selected_scholar_result()
        if r is not None:
            self._open_scholar_url(r.url, _scholar_result_cite(r), r.title)

    def _open_scholar_url(self, url: str, cite: str = "",
                          name: str = "") -> None:
        """Open a Scholar case page (from the Scholar results column).  If the
        opinion page won't load, fall back to CourtListener via ``cite`` and
        retry Scholar in the background."""
        fetcher = self._get_scholar()
        if fetcher is None:
            return
        self._status_var.set("Fetching opinion from Google Scholar…")

        def run() -> None:
            try:
                result = fetcher.fetch_by_url(url)
            except Exception as exc:
                print(f"[scholar] open {url!r} failed: {exc}")
                result = None
            if result:
                self.root.after(
                    0, self._on_scholar_result, result, None, None,
                    "opened from Scholar search",
                )
            else:
                self.root.after(
                    0, self._scholar_case_fallback, url, cite, "", True, name,
                )

        threading.Thread(target=run, daemon=True).start()

    def _scholar_case_fallback(
        self, url: str, cite: str, pin: str = "", prefetch_pdf: bool = True,
        name: str = "",
    ) -> None:
        """A Google Scholar opinion page failed to load (Google is flaky).  Show
        the CourtListener view located by the case's reporter citation and retry
        the Scholar opinion in the background — the "Google Scholar Text" button
        lights up if it comes through.  With no citation to locate the case on
        CourtListener, just retry Scholar and open it if it returns."""
        client = self._get_client() if self._token_var.get().strip() else None
        fetcher = self._get_scholar()
        cite = (cite or "").strip()

        if cite and client is not None:
            self._status_var.set(
                f"Google Scholar busy — loading {cite} from CourtListener…"
            )

            def run() -> None:
                try:
                    target = _cl_item_for_citation(client, cite)
                    if not target:
                        pdf = _case_law_pdf_for_cite(cite)
                        if pdf:
                            self._post_case_law_pdf(
                                pdf, cite, pin, expected_name=name)
                            return
                        self._post_root(
                            lambda: self._status_var.set(
                                f"No CourtListener match for {cite}."
                            )
                        )
                        return
                    parts, blocks, plain, cluster = _assemble_case_parts(
                        client, target,
                    )

                    def open_cl() -> None:
                        try:
                            w = _ScholarTextWindow(
                                self.root, self, "", "", item=target,
                                cl_text=plain, cl_parts=parts, cl_blocks=blocks,
                                prefetch_pdf=prefetch_pdf,
                            )
                            w._retry_scholar_link(cite, pin, url)
                        except tk.TclError:
                            pass

                    self._post_root(open_cl)
                except Exception as exc:
                    pdf = _case_law_pdf_for_cite(cite)
                    if pdf:
                        self._post_case_law_pdf(
                            pdf, cite, pin, expected_name=name)
                        return
                    self._post_root(
                        lambda e=exc: self._status_var.set(f"CourtListener: {e}")
                    )

            threading.Thread(target=run, daemon=True).start()
            return

        if cite:
            self._status_var.set(f"Checking case.law for {cite}…")

            def try_pdf() -> None:
                pdf = _case_law_pdf_for_cite(cite)
                if pdf:
                    self._post_case_law_pdf(
                        pdf, cite, pin, expected_name=name)
                    return
                self._post_root(
                    self._status_var.set,
                    "Could not load this case from Google Scholar."
                    if fetcher is None
                    else "Google Scholar busy — retrying…",
                )
                if fetcher is not None:
                    threading.Thread(target=retry, daemon=True).start()

            def retry() -> None:
                for _ in range(3):
                    time.sleep(4.0)
                    try:
                        result = fetcher.fetch_by_url(url)
                    except Exception:
                        result = None
                    if result:
                        r_url, html = result
                        self._post_root(
                            lambda u=r_url, h=html: _ScholarTextWindow(
                                self.root, self, u, h, item=None,
                            )
                        )
                        return
                self._post_root(
                    lambda: self._status_var.set(
                        "Google Scholar still unavailable for this case."
                    )
                )

            threading.Thread(target=try_pdf, daemon=True).start()
            return

        if fetcher is None:
            self._status_var.set("Could not load this case from Google Scholar.")
            return

        # No citation to locate the case on CourtListener — retry Scholar alone.
        self._status_var.set("Google Scholar busy — retrying…")

        def retry() -> None:
            for _ in range(3):
                time.sleep(4.0)
                try:
                    result = fetcher.fetch_by_url(url)
                except Exception:
                    result = None
                if result:
                    r_url, html = result
                    self._post_root(
                        lambda u=r_url, h=html: _ScholarTextWindow(
                            self.root, self, u, h, item=None,
                        )
                    )
                    return
            self._post_root(
                lambda: self._status_var.set(
                    "Google Scholar still unavailable for this case."
                )
            )

        threading.Thread(target=retry, daemon=True).start()

    def _fetch_scholar_text(self) -> None:
        # A row in the Scholar results column: open it directly, unverified.
        if self._selected_scholar_result() is not None:
            self._open_selected_scholar_result()
            return

        selected = self._get_selected_item()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a case first.")
            return

        fetcher = self._get_scholar()
        if fetcher is None:
            return
        client = self._get_client()
        _, item = selected

        self._download_btn.config(state="disabled")
        self._scholar_btn.configure(state="disabled")
        self._search_btn.config(state="disabled")
        self._status_var.set("Searching Google Scholar…")

        def status_cb(msg: str) -> None:
            self.root.after(0, self._status_var.set, msg)

        def run() -> None:
            # Federal Appendix cases are scans Google Scholar almost never
            # carries — open straight on the official PDF rather than falling
            # back to the CourtListener text.
            if _item_is_fed_appx(item):
                self.root.after(0, self._open_fed_appx_pdf, item)
                return
            self._scholar_first_worker(
                item, fetcher, client,
                status=status_cb, done=self._restore_buttons,
            )

        threading.Thread(target=run, daemon=True).start()

    def _open_fed_appx_pdf(self, item: dict) -> None:
        """Open a Federal Appendix case straight on its official PDF.  These
        are scans Google Scholar lacks, so build the static.case.law URL from
        the F. App'x citation and show it directly; if that can't be built,
        fall back to resolving the PDF from the case record."""
        self._restore_buttons()
        cite = _fed_appx_cite(item)
        url = _static_case_law_url(cite) if cite else None
        if url:
            name = re.sub(
                r"<[^>]+>", "",
                item.get("caseName") or item.get("case_name") or "",
            ).strip()
            title = f"{name} — {cite}" if name else cite
            self._status_var.set(f"Opening {cite} (case.law)…")
            _open_case_law_pdf(
                self.root, url, title, self._status_var.set, app=self,
                expected_name=name,
            )
            return
        self._status_var.set("Federal Appendix case — opening the PDF…")
        _ScholarTextWindow(self.root, self, "", "", item=item)

    # ------------------------------------------------------------------
    # Scholar-first open: prefer Google Scholar, but only show it when its
    # first case verifies against the CourtListener text.
    # ------------------------------------------------------------------

    def _scholar_first_worker(
        self, item: dict, fetcher, client,
        prefetch_pdf: bool = True, status=None, done=None,
    ) -> None:
        """Worker-thread body for opening a CourtListener-identified case.

        Tries Google Scholar's first case (by primary citation):
          • Scholar errors           → give up, show the CourtListener text.
          • first case matches CL    → show the Scholar text and we're done.
          • first case doesn't match → show the CourtListener text now, but
            keep hunting for a matching Scholar case in the background; if one
            turns up the "Google Scholar Text" button lights up so the reader
            can switch over.

        (Federal Appendix cases are handled by callers before this runs.)
        """
        def finish() -> None:
            if done is not None:
                self._post_root(done)

        cluster_id = item.get("cluster_id") or item.get("id")
        vkey = f"verified:cluster:{cluster_id}" if cluster_id else ""

        # A previously verified Scholar copy — open it straight away.
        if fetcher is not None and vkey:
            cached = fetcher.get_cached(vkey)
            if cached:
                url, html = cached
                self._post_root(
                    self._open_scholar_window, url, html, item, None,
                    "verified match (cached)", prefetch_pdf,
                )
                finish()
                return

        # Step 1 — Google Scholar's first case, by the primary citation.
        quick_result = None
        quick_error = False
        if fetcher is not None:
            primary = _pick_citation(item.get("citation", []))
            if primary:
                if status:
                    status("Searching Google Scholar…")
                try:
                    quick_result = fetcher.fetch_by_citation(primary)
                except Exception as exc:
                    print(f"[scholar] first-case fetch error for {primary!r}: {exc}")
                    quick_error = True

        # An error from Google Scholar — give up right away, show the CL text.
        if quick_error:
            self._assemble_and_open_cl(
                item, client, prefetch_pdf, finish, search=False,
                note="Google Scholar error — showing CourtListener text",
            )
            return

        # Step 2 — assemble the CourtListener case (to verify against, and to
        # show if Scholar doesn't pan out).
        cl_parts: list = []
        cl_blocks: list = []
        cl_text: Optional[str] = None
        if client is not None and cluster_id:
            if status:
                status("Loading CourtListener text…")
            try:
                cl_parts, cl_blocks, cl_plain, _ = _assemble_case_parts(
                    client, item,
                )
                cl_text = cl_plain or None
            except Exception as exc:
                print(f"[scholar] CourtListener assembly failed: {exc}")

        # Step 3 — decide based on Scholar's first case vs. the CL text.
        if quick_result is not None:
            url, html = quick_result
            if cl_text is None:
                # Nothing to verify against — accept the first Scholar result.
                self._post_root(
                    self._open_scholar_window, url, html, item, None,
                    "unverified (no CourtListener text)", prefetch_pdf,
                )
                finish()
                return
            sim = text_similarity(
                blocks_to_text(parse_opinion_blocks(html)), cl_text,
            )
            print(f"[scholar] first-case similarity {sim:.2f}")
            if sim >= _SCHOLAR_MATCH_THRESHOLD:
                # Matches — show it and we're done.
                if vkey and fetcher is not None:
                    fetcher.put_cached(vkey, url, html)
                self._post_root(
                    self._open_scholar_window, url, html, item, cl_text,
                    "verified against CourtListener", prefetch_pdf,
                )
                finish()
                return
            # Doesn't match — fall through to the CourtListener text below.

        # Scholar's first case either didn't match or wasn't there: open the
        # CourtListener text now and keep looking for a Scholar match.
        search = fetcher is not None
        if cl_parts or cl_text:
            self._post_root(
                self._open_cl_window, item, cl_parts, cl_blocks, cl_text,
                "", prefetch_pdf, search,
            )
            finish()
            return

        # No CourtListener text at all — last resort: a full Scholar search.
        if fetcher is not None:
            try:
                result, cl_text2, note = _find_scholar_for_item(
                    client, fetcher, item, status or (lambda _m: None),
                )
            except Exception as exc:
                print(f"[scholar] full search failed: {exc}")
                result, cl_text2, note = None, None, ""
            if result:
                s_url, s_html = result
                self._post_root(
                    self._open_scholar_window, s_url, s_html, item,
                    cl_text2, note, prefetch_pdf,
                )
                finish()
                return
        self._post_root(self._scholar_open_failed, item, "")
        finish()

    def _assemble_and_open_cl(
        self, item: dict, client, prefetch_pdf, finish,
        search: bool = False, note: str = "",
    ) -> None:
        """Worker thread: assemble the CourtListener case and open it (showing
        a failure dialog when there's nothing to display)."""
        parts: list = []
        blocks: list = []
        plain = ""
        if client is not None:
            try:
                parts, blocks, plain, _ = _assemble_case_parts(client, item)
            except Exception as exc:
                print(f"[scholar] CourtListener assembly failed: {exc}")
        if parts or plain:
            self._post_root(
                self._open_cl_window, item, parts, blocks, plain,
                note, prefetch_pdf, search,
            )
        else:
            self._post_root(self._scholar_open_failed, item, note)
        finish()

    def _open_scholar_window(
        self, url: str, html: str, item: Optional[dict],
        cl_text: Optional[str], note: str, prefetch_pdf: bool = True,
    ) -> None:
        self._status_var.set(
            f"Scholar text loaded — {note}" if note
            else f"Scholar text loaded from {url}"
        )
        _ScholarTextWindow(
            self.root, self, url, html, item=item, cl_text=cl_text,
            note=note, prefetch_pdf=prefetch_pdf,
        )

    def _open_cl_window(
        self, item: Optional[dict], parts, blocks, plain,
        note: str = "", prefetch_pdf: bool = True, search: bool = False,
    ) -> "_ScholarTextWindow":
        self._status_var.set(
            "Loaded CourtListener text — searching Google Scholar…"
            if search else "Loaded CourtListener text."
        )
        win = _ScholarTextWindow(
            self.root, self, "", "", item=item, cl_text=plain, note=note,
            cl_parts=parts or [], cl_blocks=blocks or [],
            prefetch_pdf=prefetch_pdf,
        )
        if search:
            win._search_for_scholar_version(plain)
        return win

    def _scholar_open_failed(self, item: Optional[dict], note: str) -> None:
        self._status_var.set("Could not load this case.")
        messagebox.showwarning(
            "Case Unavailable",
            "Could not load this case from Google Scholar or CourtListener."
            + (f"\n\n({note})" if note else ""),
        )

    def _on_scholar_result(
        self,
        result: Optional[tuple[str, str]],
        item: Optional[dict] = None,
        cl_text: Optional[str] = None,
        note: str = "",
    ) -> None:
        self._restore_buttons()
        if result is None:
            target_item = dict(item) if item else {}
            has_cluster = bool(
                target_item.get("cluster_id") or target_item.get("id")
            )
            if not has_cluster:
                self._status_var.set("Google Scholar text unavailable.")
                messagebox.showwarning(
                    "Scholar Text Unavailable",
                    "Could not find a Google Scholar opinion matching this case."
                    + (f"\n\n({note})" if note else ""),
                )
                return
            self._status_var.set(
                "Scholar unavailable — loading CourtListener text…"
            )
            self._scholar_btn.configure(state="disabled")
            client = self._get_client()
            if client is None:
                self._status_var.set("Google Scholar text unavailable.")
                return

            def run() -> None:
                try:
                    parts, blocks, plain, cluster = _assemble_case_parts(
                        client, target_item,
                    )
                except Exception as exc:
                    import traceback
                    traceback.print_exc()
                    parts, blocks, plain, cluster = [], [], "", {}
                self.root.after(
                    0, self._on_cl_fallback_ready,
                    parts, blocks, plain, target_item, cl_text, note,
                )

            threading.Thread(target=run, daemon=True).start()
            return

        url, html = result
        self._status_var.set(
            f"Scholar text loaded — {note}" if note else f"Scholar text loaded from {url}"
        )
        _ScholarTextWindow(
            self.root, self, url, html, item=item, cl_text=cl_text, note=note
        )

    def _on_cl_fallback_ready(
        self, parts, blocks, plain, item, cl_text, note,
    ) -> None:
        self._restore_buttons()
        if not parts and not blocks:
            self._status_var.set("Google Scholar text unavailable.")
            messagebox.showwarning(
                "Scholar Text Unavailable",
                "Could not find a Google Scholar opinion matching this case,\n"
                "and CourtListener text could not be loaded either.\n\n"
                + (f"({note})" if note else ""),
            )
            return
        self._status_var.set("Loaded CourtListener text (Scholar unavailable).")
        _ScholarTextWindow(
            self.root, self, "", "",
            item=item, cl_text=cl_text or plain, note=note,
            cl_parts=parts, cl_blocks=blocks,
        )

    def _restore_buttons(self) -> None:
        self._download_btn.config(state="normal")
        self._scholar_btn.configure(state="normal")
        self._search_btn.config(state="normal")

    def _on_download_done(self, path: str) -> None:
        self._status_var.set(f"Saved: {path}")
        if messagebox.askyesno(
            "Download Complete", f"PDF saved to:\n{path}\n\nOpen it now?"
        ):
            self._open_file(path)

    def _on_text_download_done(self, path: str) -> None:
        self._status_var.set(f"Saved: {path}")
        if messagebox.askyesno(
            "Text Saved",
            f"No PDF was available.\nOpinion text saved to:\n{path}\n\nOpen it now?",
        ):
            self._open_file(path)

    @staticmethod
    def _open_file(path: str) -> None:
        if sys.platform == "win32":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])


class _CourtPickerDialog:
    """
    Checkbox-tree dialog for choosing which courts to search.

    The tree mirrors ``court_catalog.CATALOG``: Federal (Supreme Court,
    Courts of Appeals, District Courts, Specialized) and State (each state
    with its appellate courts).  Clicking a group toggles everything under
    it; groups show ☑ / ☐ / ◪ for all / none / some selected.  An empty
    selection means "all courts" (no filter).
    """

    _GLYPH_ALL, _GLYPH_NONE, _GLYPH_SOME = "☑", "☐", "◪"

    def __init__(
        self,
        parent: tk.Misc,
        selected: set[str],
        on_apply,
    ) -> None:
        self._on_apply = on_apply
        self._checked: set[str] = set(selected)
        self._labels: dict[str, str] = {}        # tree iid → bare label
        self._group_leaves: dict[str, set[str]] = {}  # group iid → descendant ids

        win = _ui_toplevel(parent)
        self._win = win
        win.title("Select Courts")
        win.geometry("440x560")
        win.minsize(360, 400)
        win.transient(parent)
        win.grab_set()
        _ensure_modern_ttk_styles(win)

        header = _ui_label(win, "Select Courts to Search", size=15, weight="bold",
                           anchor="w")
        header.pack(fill="x", padx=16, pady=(14, 0))
        _ui_label(win, "Clicking a group toggles everything under it.",
                  size=11, muted=True, anchor="w").pack(fill="x", padx=16,
                                                        pady=(2, 8))

        tree_frame = _ui_frame(win, card=True)
        tree_frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))
        tree_style = "Modern.Treeview" if _CTK_AVAILABLE else "Treeview"
        pad = 8 if _CTK_AVAILABLE else 0
        self._tree = ttk.Treeview(tree_frame, show="tree", selectmode="none",
                                  style=tree_style)
        sb_style = "Modern.Vertical.TScrollbar" if _CTK_AVAILABLE else "Vertical.TScrollbar"
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tree.yview,
                            style=sb_style)
        self._tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y", pady=pad, padx=(0, pad))
        self._tree.pack(side="left", fill="both", expand=True, padx=(pad, 0),
                        pady=pad)

        self._build_nodes("", _COURT_CATALOG)
        # Open the two top-level branches so the structure is visible
        for iid in self._tree.get_children(""):
            self._tree.item(iid, open=True)
        self._refresh_glyphs()
        self._tree.bind("<Button-1>", self._on_click)

        bot = _ui_frame(win)
        bot.pack(fill="x", padx=16, pady=(0, 14))
        self._count_var = tk.StringVar()
        _ui_label(bot, muted=True, textvariable=self._count_var).pack(side="left")
        _ui_button(bot, "Apply", command=self._apply, primary=True,
                   width=92).pack(side="right")
        _ui_button(bot, "Cancel", command=win.destroy, width=88).pack(
            side="right", padx=8)
        _ui_button(bot, "Clear", command=self._clear, width=80).pack(side="right")
        self._update_count()

    # -- tree construction ---------------------------------------------------

    def _build_nodes(self, parent_iid: str, nodes) -> set[str]:
        leaves: set[str] = set()
        for label_or_id, payload in nodes:
            if isinstance(payload, list):
                iid = self._tree.insert(parent_iid, "end", text=label_or_id)
                self._labels[iid] = label_or_id
                sub = self._build_nodes(iid, payload)
                self._group_leaves[iid] = sub
                leaves |= sub
            else:
                cid, label = label_or_id, payload
                self._tree.insert(parent_iid, "end", iid=cid, text=label)
                self._labels[cid] = label
                leaves.add(cid)
        return leaves

    # -- interaction -----------------------------------------------------------

    def _on_click(self, event: tk.Event) -> None:
        # Let clicks on the expander triangle expand/collapse as usual
        if "indicator" in self._tree.identify_element(event.x, event.y):
            return
        iid = self._tree.identify_row(event.y)
        if not iid:
            return
        if iid in self._group_leaves:
            leaves = self._group_leaves[iid]
            if leaves <= self._checked:
                self._checked -= leaves
            else:
                self._checked |= leaves
        else:
            self._checked.symmetric_difference_update({iid})
        self._refresh_glyphs()
        self._update_count()

    def _refresh_glyphs(self) -> None:
        for iid, label in self._labels.items():
            if iid in self._group_leaves:
                leaves = self._group_leaves[iid]
                if leaves and leaves <= self._checked:
                    glyph = self._GLYPH_ALL
                elif leaves & self._checked:
                    glyph = self._GLYPH_SOME
                else:
                    glyph = self._GLYPH_NONE
            else:
                glyph = self._GLYPH_ALL if iid in self._checked else self._GLYPH_NONE
            self._tree.item(iid, text=f"{glyph} {label}")

    def _update_count(self) -> None:
        n = len(self._checked)
        self._count_var.set(
            "All courts (no filter)" if n == 0 else f"{n} court(s) selected"
        )

    def _clear(self) -> None:
        self._checked.clear()
        self._refresh_glyphs()
        self._update_count()

    def _apply(self) -> None:
        self._on_apply(set(self._checked))
        self._win.destroy()


class _DbMatchDialog:
    """Pick a stored opinion from a database search and act on it: Open it,
    Refresh it (replace the stored copy with the latest Google Scholar
    version — e.g. to pick up newly added reporter pagination), or Delete it
    (after which the case is only ever reloaded from Google Scholar itself,
    never from the cache)."""

    def __init__(self, parent: tk.Misc, app: "CourtListenerGUI", candidates: list[dict]) -> None:
        self._app = app
        self._candidates = list(candidates)
        self._busy = False
        win = _ui_toplevel(parent)
        self._win = win
        win.title("Opinions in Database")
        win.geometry("680x440")
        win.minsize(440, 280)
        _ensure_modern_ttk_styles(win)
        frame = _ui_frame(win)
        frame.pack(fill="both", expand=True, padx=14, pady=12)
        heading = ("1 opinion found" if len(candidates) == 1
                   else f"{len(candidates)} opinions match — choose one")
        _ui_label(
            frame, heading, size=14, weight="bold", anchor="w",
        ).pack(anchor="w", fill="x")
        card = _ui_frame(frame, card=True)
        card.pack(fill="both", expand=True, pady=(8, 6))
        pad = 8 if _CTK_AVAILABLE else 0
        cols = ("name", "cite", "court", "year")
        tree = ttk.Treeview(
            card, columns=cols, show="headings", selectmode="browse", height=8,
            style="Modern.Treeview" if _CTK_AVAILABLE else "Treeview",
        )
        for col, title, width in (
            ("name", "Case", 320), ("cite", "Citation", 150),
            ("court", "Court", 90), ("year", "Year", 60),
        ):
            tree.heading(col, text=title)
            tree.column(col, width=width, anchor="w")
        for i, h in enumerate(candidates):
            tree.insert(
                "", "end", iid=str(i),
                values=(h.get("name") or "(unknown)", h.get("cite", ""),
                        h.get("court", ""), h.get("year", "")),
            )
        tree.pack(fill="both", expand=True, padx=pad, pady=pad)
        self._tree = tree
        self._status_var = tk.StringVar(value="")
        _ui_label(frame, muted=True, anchor="w",
                  textvariable=self._status_var).pack(fill="x", pady=(0, 6))
        btns = _ui_frame(frame)
        btns.pack(fill="x")
        _ui_button(btns, "Open", command=self._open, primary=True,
                   width=92).pack(side="right")
        _ui_button(btns, "Cancel", command=win.destroy, width=88).pack(
            side="right", padx=(0, 8)
        )
        _ui_button(btns, "Delete", command=self._delete, width=88).pack(
            side="left")
        _ui_button(btns, "Refresh Opinion", command=self._refresh,
                   width=136).pack(side="left", padx=(8, 0))
        tree.bind("<Double-1>", lambda _e: self._open())
        if candidates:
            tree.selection_set("0")
            tree.focus_set()

    def _selected(self) -> Optional[tuple[str, dict]]:
        sel = self._tree.selection()
        if not sel or self._busy:
            return None
        return sel[0], self._candidates[int(sel[0])]

    def _post(self, fn, *args) -> None:
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass  # dialog closed while the worker ran

    def _open(self) -> None:
        picked = self._selected()
        if picked is None:
            return
        _iid, h = picked
        self._win.destroy()
        self._app._open_db_record(h["scholar_id"])

    def _delete(self) -> None:
        """Remove the opinion from the database and scrub the query cache, so
        reopening the case can only come fresh from Google Scholar's site."""
        picked = self._selected()
        if picked is None:
            return
        iid, h = picked
        name = h.get("name") or h.get("cite") or "this opinion"
        if not messagebox.askyesno(
            "Delete Opinion",
            f"Remove “{name}” from the local database?\n\n"
            "It will not be served from any cache again — opening the case "
            "later will fetch it fresh from Google Scholar.",
            parent=self._win,
        ):
            return
        self._busy = True
        self._status_var.set("Deleting…")

        def run() -> None:
            ok, err = False, ""
            try:
                db = self._app._get_opinion_db()
                rec = db.get_by_scholar_id(h["scholar_id"]) if db else None
                ok = bool(db) and db.delete(h["scholar_id"])
                # Scrub the fetcher's query cache too, or the next open
                # would resurrect the deleted copy from there.
                if rec and rec.get("url") and _SCHOLAR_AVAILABLE:
                    fetcher = self._app._get_scholar()
                    if fetcher is not None:
                        n = fetcher.purge_cached_opinion(rec["url"])
                        if n:
                            print(f"[db] purged {n} cache entr"
                                  f"{'y' if n == 1 else 'ies'} for {name!r}")
            except Exception as exc:
                err = str(exc)

            def done() -> None:
                self._busy = False
                if not ok:
                    self._status_var.set(
                        f"Delete failed{': ' + err if err else '.'}")
                    return
                try:
                    self._tree.delete(iid)
                except tk.TclError:
                    pass
                self._status_var.set(f"Deleted {name}.")
                if not self._tree.get_children():
                    self._win.destroy()

            self._post(done)

        threading.Thread(target=run, daemon=True).start()

    def _refresh(self) -> None:
        """Replace the stored copy with the latest Google Scholar version of
        the same opinion (recent SCOTUS cases gain reporter pagination as
        the official cite is assigned)."""
        picked = self._selected()
        if picked is None:
            return
        iid, h = picked
        name = h.get("name") or h.get("cite") or "this opinion"
        if not _SCHOLAR_AVAILABLE:
            self._status_var.set(
                "Refreshing needs beautifulsoup4 (pip install beautifulsoup4).")
            return
        fetcher = self._app._get_scholar()
        db = self._app._get_opinion_db()
        if fetcher is None or db is None:
            return
        rec = db.get_by_scholar_id(h["scholar_id"])
        if not rec or not rec.get("url"):
            self._status_var.set("No Scholar URL stored for this opinion.")
            return
        self._busy = True
        self._status_var.set(
            f"Fetching the latest Google Scholar version of {name}…")

        def run() -> None:
            msg = ""
            new_summary: Optional[dict] = None
            try:
                import opinion_db as _odb
                result = fetcher.refetch_by_url(rec["url"])
                if not result:
                    msg = ("Google Scholar didn't return the opinion "
                           "(blocked or unavailable) — kept the stored copy.")
                else:
                    new_url, html = result
                    new_rec = _odb.extract_record(new_url, html)
                    if new_rec is None:
                        msg = "The fetched page carries no Scholar id — kept the stored copy."
                    else:
                        # Keep enrichments the page itself can't provide.
                        for k in ("name", "court", "year", "date_filed", "source"):
                            if not new_rec.get(k) and rec.get(k):
                                new_rec[k] = rec[k]
                        db.replace(new_rec)
                        changed = len(html) - len(rec.get("html") or "")
                        msg = (f"Updated {name} to the latest Google Scholar "
                               f"version ({changed:+,} characters).")
                        new_summary = {
                            "scholar_id": new_rec["scholar_id"],
                            "name": new_rec.get("name", ""),
                            "cite": (new_rec.get("cites") or [""])[0],
                            "court": new_rec.get("court", ""),
                            "year": new_rec.get("year", ""),
                        }
            except Exception as exc:
                msg = f"Refresh failed: {exc}"

            def done() -> None:
                self._busy = False
                self._status_var.set(msg)
                if new_summary is not None:
                    self._candidates[int(iid)] = new_summary
                    try:
                        self._tree.item(iid, values=(
                            new_summary["name"] or "(unknown)",
                            new_summary["cite"], new_summary["court"],
                            new_summary["year"]))
                    except tk.TclError:
                        pass

            self._post(done)

        threading.Thread(target=run, daemon=True).start()


_OP_ID_RE = re.compile(r"/opinions/(\d+)/?")


def _extract_opinion_id(url: str) -> Optional[int]:
    """Parse an opinion ID out of a CourtListener opinions URL."""
    m = _OP_ID_RE.search(str(url))
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# RTF generation + rich clipboard (used by the Scholar text window)
# ---------------------------------------------------------------------------
# The reporter-citation regexes (case cites, short forms, ``Id.``) and the
# short-cite index live in ``citations`` so the brief viewer can reuse them
# without pulling in tkinter; they're imported above under their old private
# names (``_TEXT_CITE_RE`` etc.).

# A citation line in the Scholar header: each parallel cite sits on its own
# centered line, e.g. "306 Md. 556 (1986)" / "510 A.2d 562" / "87 F.4th 563 (2023)"
_HEADER_CITE_RE = re.compile(
    r"^\s*(\d{1,4})\s+([A-Z][A-Za-z0-9.'’ ]{0,24}?)\s+(\d{1,5})\s*(?:\(|$)"
)
# The CourtListener metadata header can put all parallels on one centered
# comma-separated line.  Scan each field rather than recognizing only its
# first citation.
_HEADER_CITE_SCAN_RE = re.compile(
    r"(?<!\w)(\d{1,4})\s+([A-Z][A-Za-z0-9.'’ ]{0,24}?)\s+(\d{1,5})"
    r"(?=\s*(?:[,;(]|$))"
)

# A line that is *only* a reporter citation (optionally with a year), e.g.
# "512 U.S. 477 (1994)" — the running reference at the top of an opinion.
_CITE_ONLY_LINE_RE = re.compile(
    r"^\d{1,4}\s+[A-Z][A-Za-z0-9.'’ ]{0,30}?\s+\d{1,5}(?:\s*\(\d{4}\))?$"
)

# Marker opening a footnote body, e.g. "[4] …" or "* …" (fallback when the
# parser found no footnote anchor ids)
_FN_BODY_MARK_RE = re.compile(r"^\s*(?:\[([^\]\s]{1,6})\]|(\*{1,3}|†|‡))(?=\s|$)")


def _fix_name_case(name: str) -> str:
    """Render an all-caps surname from an opinion header in normal case:
    REHNQUIST → Rehnquist, O'CONNOR → O'Connor, McAULIFFE → McAuliffe."""
    def fix(wd: str) -> str:
        alpha = [c for c in wd if c.isalpha()]
        if len(alpha) <= 2 or sum(c.isupper() for c in alpha) <= len(alpha) // 2:
            return wd  # already mixed case (Wood, St.)
        out = "'".join(
            p[:1].upper() + p[1:].lower() if p else p for p in wd.split("'")
        )
        if out.startswith("Mc") and len(out) > 2:
            out = "Mc" + out[2].upper() + out[3:]
        return out

    return " ".join(fix(w) for w in name.split())


_CAPTION_SMALL_WORDS = {
    "of", "the", "and", "on", "in", "for", "a", "an", "ex", "rel", "re", "et", "al",
}

# Entity initialisms that stay all-caps in a normal-cased caption — title-casing
# would otherwise turn "SEILA LAW LLC" into "Seila Law Llc".  (Unlike "Co.",
# "Inc.", "Corp.", which are abbreviated words and *do* take title case.)
_ENTITY_INITIALISMS = {
    "llc", "llp", "lllp", "pllc", "plc", "pc", "pa", "lp", "na", "sa",
    "ag", "nv", "bv",
}


def _titlecase_caps(s: str) -> str:
    """Normal-case an all-caps caption fragment: 'MERCY HOSPITAL' →
    'Mercy Hospital', 'UNITED STATES' → 'United States', 'IN RE GAULT' →
    'In re Gault'.  Mixed-case words, abbreviations, and entity initialisms
    ('LLC', 'LLP') pass through unchanged."""
    return normal_case_caption(s)


# Entity abbreviations that legitimately end a party name with a period, so the
# period must be kept ("Acme Co.", "Foo Corp.", "Bar Inc.").  Initialisms with
# internal periods (L.L.C., N.A., S.A.) are recognized separately.
_PARTY_ABBR_SUFFIXES = {
    "co", "cos", "corp", "corps", "inc", "ltd", "llc", "llp", "lp", "lllp",
    "plc", "pc", "pa", "pllc", "na", "sa", "ag", "nv", "bros",
}


def _party_ends_in_abbrev(s: str) -> bool:
    """True if the party name ends in an abbreviation whose trailing period is
    part of the name (an entity suffix like 'Co.' or an initialism like
    'L.L.C.'), as opposed to a stray sentence period."""
    parts = s.split()
    if not parts:
        return False
    last = parts[-1]
    if "." in last[:-1]:  # an internal period → initialism (L.L.C., N.A.)
        return True
    return last.rstrip(".").replace("’", "'").lower() in _PARTY_ABBR_SUFFIXES


# A comma segment of a caption that is a procedural designation, personal
# suffix, or descriptive office — not part of the party's name — so
# _caption_party can strip such segments from the right.  Role/office
# segments swallow their qualifiers ("Warden of the Penitentiary", "U.S.
# Senator from the State of New York", "Sec'y of the U.S. Senate").
_ROLE_WORD = (
    r"(?:cross[- ])?(?:plaintiffs?|defendants?|appellants?|appellees?|"
    r"petitioners?|respondents?|intervenors?|movants?|claimants?|relators?|"
    r"libell?ants?|libell?ees?|garnishees?|contestants?)"
)
_US_STATE_NAMES = (
    r"alabama|alaska|arizona|arkansas|california|colorado|connecticut|"
    r"delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|"
    r"kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|"
    r"mississippi|missouri|montana|nebraska|nevada|new\s+hampshire|"
    r"new\s+jersey|new\s+mexico|new\s+york|north\s+carolina|north\s+dakota|"
    r"ohio|oklahoma|oregon|pennsylvania|rhode\s+island|south\s+carolina|"
    r"south\s+dakota|tennessee|texas|utah|vermont|virginia|washington|"
    r"west\s+virginia|wisconsin|wyoming|district\s+of\s+columbia"
)
_PARTY_DESIGNATION_RE = re.compile(
    r"(?:"
    r"et\s+als?\.?|etc\.?|jr\.?|sr\.?|ii|iii|iv|deceased|minor|an?\s+minor|"
    r"m\.?d\.?|ph\.?d\.?|esq\.?|afl[-\s]?cio(?:[-\s]?clc)?|"
    + _ROLE_WORD + r"(?:[-/\s]+" + _ROLE_WORD + r")*(?:\s+in\s+error)?|"
    # a role ending one consolidated caption, with the next spilling into the
    # same segment ("Appellant. United States of America")
    + _ROLE_WORD + r"\.\s+.*|"
    r"in\s+error|successors?\b.*|"
    # a bare state name qualifying a governmental party ("Humboldt County,
    # California"; "City of Milwaukee, Wisconsin")
    r"(?:" + _US_STATE_NAMES + r")|"
    # offices / roles, with any qualifiers around them ("Warden", "Michigan
    # Secretary of State", "District Attorney of Dallas County",
    # "Corrections Director", "Assessor of Contra Costa County")
    r"[\w.'’ -]{0,40}\b"
    r"(?:secretary|sec'y|superintendent|directors?|commissioners?|"
    r"administrat(?:ors?|rix)|attorneys?|att'y|assessors?|treasurer|"
    r"wardens?|sheriffs?|governor|senators?|representatives?|"
    r"execut(?:ors?|rix)|trustees?|receivers?|guardians?|conservators?|"
    r"comptroller|clerk|marshal|postmaster|solicitor)\b[\w.'’ -]{0,60}|"
    r"president\s+of\b.*|grand\s+jury\b.*|fictitious\w*\b.*|"
    r"national\s+association|"
    # descriptive phrases
    r"(?:individually|personally)\b.*|(?:as|by|in\s+(?:his|her|its|their))\b.*|"
    r"on\s+behalf\b.*|for\s+the\s+use\b.*|d/?b/?a\b.*|"
    r"(?:[A-Za-z]\.\s*){2,}(?:[;,]\s*(?:[A-Za-z]\.\s*){2,})+|"  # minors' initials
    r"an?\s+(?:\w+\s+)*(?:corporation|company|partnership|association|"
    r"municipality|municipal\s+corporation|body\s+politic)\b.*"
    r")\s*\.?\s*$",
    re.IGNORECASE,
)

# A comma segment that is a business-entity suffix continuing the previous
# segment ("Socony-Vacuum Oil Co., Inc." — one party, two segments).
_ENTITY_SUFFIX_RE = re.compile(
    r"(?:inc|incorporated|l\.?l\.?c|l\.?l\.?p|l\.?p|ltd|ltda|p\.?l\.?c|"
    r"p\.?c|s\.?a|s\.?p\.?a|a\.?g|n\.?v|s\.?l|gmbh|co|corp|n\.?a)\.?",
    re.IGNORECASE,
)

# A segment *ending* in an organization word plausibly continues the first
# segment's name ("CHICAGO, BURLINGTON AND QUINCY RAILROAD COMPANY";
# "LOCAL 174, TEAMSTERS, ..., WAREHOUSEMEN & HELPERS OF AMERICA").
_ORG_TAIL_RE = re.compile(
    r"\b(?:company|co\.?|corp\.?|corporation|incorporated|railroad|railway|"
    r"r\.?r\.?|union|association|ass'n|brotherhood|workers|helpers|"
    r"america|bank|trust|society|club|university|college|institute|"
    r"partners(?:hip)?|group|bros\.?|brothers|sons|"
    r"inc\.?|llc|llp|ltd\.?|plc)\s*\.?\s*$",
    re.IGNORECASE,
)


def _caption_party(s: str) -> str:
    """One side of a Scholar caption → its Bluebook party name.  Drops the
    procedural designation and 'et al.'; when Scholar mixes cases, the
    all-caps run is the operative name ('Brent BREWBAKER' → 'Brewbaker',
    'UNITED STATES of America' → 'United States').

    Comma segments after the first are handled structurally rather than by
    cutting at the first comma (which would amputate a party whose own name
    contains one — "CHICAGO, BURLINGTON AND QUINCY RAILROAD COMPANY" is one
    party, not "CHICAGO"):

      1. cut at the first sign of a co-party / consolidated caption (a
         repeated party, an "and Ace Garage" co-defendant, a new "v."),
      2. strip procedural designations, offices and suffixes from the right,
      3. keep a business-entity suffix ("…Oil Co., Inc."), and
      4. keep the remaining segments only when they read as one entity name
         (few, short, ending in an organization word — "…RAILROAD COMPANY",
         "…HELPERS OF AMERICA"); otherwise they are co-parties: dropped."""

    def clean_seg(p: str) -> str:
        p = re.sub(r"\s*\[[^\]]{1,4}\]\s*$", "", p.strip())  # footnote marker
        p = re.sub(r"[:]+$", "", p).strip()
        return re.sub(r"\s+et\s+als?\.?$", "", p, flags=re.IGNORECASE).strip()

    raw = [clean_seg(p) for p in re.split(r"[,;]", s)]
    segs = [p for p in raw if p]
    if segs:
        # A charter-era bank's formal corporate style contains structural
        # commas that do not separate parties: "The President, Directors,
        # and Company of the Bank of ...".  Reassemble that fixed title
        # before the ordinary co-party logic examines comma segments.  The
        # opinion-evidence refinement later decides whether the embedded
        # "Bank of ..." may stand alone as the working party name.
        if (len(segs) >= 3
                and re.fullmatch(r"(?:the\s+)?(?:president|governor)",
                                 segs[0], re.IGNORECASE)
                and re.fullmatch(r"(?:directors?|dirs?\.?|trustees?)",
                                 segs[1], re.IGNORECASE)
                and re.match(
                    r"(?:and|&)\s+(?:company|co\.?|corporation)\s+of\s+"
                    r"(?:the\s+)?bank\s+of\b",
                    segs[2], re.IGNORECASE)):
            segs = [", ".join(segs[:3])] + segs[3:]
        # 1. Consolidation cut: a later segment that re-names the first party
        # (same final token), contains its own " v. ", or introduces another
        # party ("and Ace Garage" — unless it ends the entity's own name,
        # "…, and Paperhangers of America") starts a different case/party.
        first_last_tok = segs[0].split()[-1].rstrip(".,").lower() if segs[0].split() else ""
        cut = len(segs)
        for i in range(1, len(segs)):
            seg = segs[i]
            toks = [t.rstrip(".,").lower() for t in seg.split()]
            # An "and X" segment continues the entity's own name only when it
            # ends in an organization word and is not itself a complete
            # company ("…, and Paperhangers of America" continues; "…, and
            # Midway Mfg. Co." is a co-plaintiff).
            and_co_party = (
                re.match(r"and\b", seg, re.IGNORECASE)
                and (not _ORG_TAIL_RE.search(seg)
                     or any(_ENTITY_SUFFIX_RE.fullmatch(t.strip(".,"))
                            or t.strip(".,").lower() in ("mfg", "mfrs")
                            for t in seg.split()))
            )
            if (re.search(r"\s[vV]s?\.\s", f" {seg} ")
                    or re.match(r"(?:et\s+als?\.?|successors?)\b", seg,
                                re.IGNORECASE)
                    or (first_last_tok and first_last_tok in toks)
                    or and_co_party):
                cut = i
                break
        segs = segs[:cut]
        # 2. Designations / offices / suffixes strip from the right.
        while len(segs) > 1 and _PARTY_DESIGNATION_RE.fullmatch(segs[-1]):
            segs.pop()
        # 3. A business-entity suffix continues the first segment's name.
        kept = [segs[0]]
        rest = segs[1:]
        if rest and _ENTITY_SUFFIX_RE.fullmatch(rest[0]):
            kept.append(rest.pop(0))
        # 4. The remaining segments stay only when they read as one entity
        # name; anything else is a co-party list, dropped as before.  Bare
        # leftover suffixes ("…, LLC, Inc." from a consolidated caption)
        # never extend the name a second time.
        joined = ", ".join(kept + rest)
        if rest and (
            all(_ENTITY_SUFFIX_RE.fullmatch(p) for p in rest)
            or not (len(rest) <= 3
                    and _ORG_TAIL_RE.search(rest[-1])
                    and len(joined) <= 72)
        ):
            rest = []
        s = ", ".join(kept + rest)
    else:
        s = ""
    s = s.strip().lstrip(".;").rstrip(";").strip()
    s = re.sub(r"\s+et\s+als?\.?$", "", s, flags=re.IGNORECASE).strip()
    # Repeated dotted abbreviations from consolidated captions ("United
    # States U.S. U.S.") collapse to the first occurrence.
    s = re.sub(r"\b((?:[A-Za-z]+\.)+)(\s+\1)+", r"\1", s)
    s = re.sub(r"\b([Uu]nited\s+[Ss]tates)(?:\s+U\.?S\.?A?\.?)+", r"\1", s)
    # Drop a stray trailing period, but keep an entity abbreviation's period
    # ("Acme Co.", "Foo Corp.", "Bar Inc.").
    if s.endswith(".") and not _party_ends_in_abbrev(s):
        s = s[:-1].rstrip()
    # Scholar signals surnames with all caps ("Corrine Morgan THOMAS"), but
    # entity initialisms ("McDonald's USA, LLC") must remain intact.
    s = collapse_personal_all_caps_run(s)
    return _titlecase_caps(s).strip(" ,;&")


_CIRCUIT_ORDINALS = {
    # Spelled-out (opinion headers) and digit ordinals (results bylines).
    "first": "ca1", "second": "ca2", "third": "ca3", "fourth": "ca4",
    "fifth": "ca5", "sixth": "ca6", "seventh": "ca7", "eighth": "ca8",
    "ninth": "ca9", "tenth": "ca10", "eleventh": "ca11",
    "1st": "ca1", "2nd": "ca2", "3rd": "ca3", "4th": "ca4", "5th": "ca5",
    "6th": "ca6", "7th": "ca7", "8th": "ca8", "9th": "ca9", "10th": "ca10",
    "11th": "ca11",
}

# Google Scholar prefixes a state result's court with the state's Bluebook
# abbreviation minus periods/spaces ("N.D." → "ND", "Cal." → "Cal").  Map
# that key to the state's court list (court of last resort first) so the
# prefix selects the right CourtListener court ids.
_SCHOLAR_STATE_PREFIX: dict[str, list[tuple[str, str, str]]] = {}
for _state_name, _state_courts in _STATE_COURTS:
    _pref_key = _state_courts[0][1].replace(".", "").replace(" ", "").lower()
    _SCHOLAR_STATE_PREFIX.setdefault(_pref_key, _state_courts)


def _classify_state_court(text: str, courts: list[tuple[str, str, str]]) -> str:
    """Pick a state's CourtListener court id from a court description,
    matching against the catalog's court labels (rule of last resort first)."""
    t = re.sub(r"\s+", " ", text or "").strip().lower()
    high = courts[0][0]
    inter = courts[1][0] if len(courts) > 1 else ""

    def by_label(*keywords: str) -> str:
        for cid, _abbr, label in courts:
            ll = label.lower()
            if any(k in ll for k in keywords):
                return cid
        return ""

    # Most specific named courts first, matched to the catalog's labels.
    if "criminal" in t:
        hit = by_label("criminal")
        if hit:
            return hit
    if "civil" in t:
        hit = by_label("civil")
        if hit:
            return hit
    if "appellate division" in t:
        return "nyappdiv" if high == "ny" else (by_label("appellate division")
                                                or inter or high)
    if "special appeals" in t:
        return by_label("special") or inter or high
    if "commonwealth" in t:
        return by_label("commonwealth") or inter or high
    if "superior" in t:
        return by_label("superior") or inter or high
    # Generic intermediate appellate court (its name varies by state:
    # "Court of Appeal(s)", "Appeals Court", "Appellate Court", "District
    # Court of Appeal").  Maryland, New York, and D.C. name their highest
    # court this way instead.
    if (re.search(r"courts? of appeal", t) or "appeals court" in t
            or "appellate court" in t):
        return high if high in ("md", "ny", "dc") else (inter or high)
    if "supreme" in t:
        return high
    return high


def _scholar_court_id(blocks) -> str:
    """CourtListener court ID inferred from the Scholar header's court line
    (used when a case was opened from Scholar with no CourtListener record)."""
    for b in blocks[:8]:
        if b.kind != "center":
            continue
        t = re.sub(r"\s+", " ", b.text()).strip().rstrip(".").lower()
        if not t or "court" not in t or "district court" in t or "bankruptcy" in t:
            continue
        if "supreme court" in t and "united states" in t:
            return "scotus"
        m = re.search(
            r"court of appeals,? (?:for the )?(\w+(?: of columbia)?) circuit", t
        )
        if m:
            word = m.group(1)
            if word == "federal":
                return "cafc"
            if "columbia" in word:
                return "cadc"
            return _CIRCUIT_ORDINALS.get(word, "")
        for state, courts in _STATE_COURTS:
            if state.lower() not in t:
                continue
            return _classify_state_court(t, courts)
    return ""


# Shared with opinion_db's caption extraction; the implementation lives in
# bluebook_names.cut_companion_cases (tkinter-free).  The private alias keeps
# this module's call sites and tests stable.
_cut_companion_cases = cut_companion_cases


_PROCEDURAL_ALIAS_RE = re.compile(
    r",?\s+(?:d/b/a|d\.b\.a\.|dba|a/k/a|a\.k\.a\.|f/k/a|f\.k\.a\.|"
    r"doing\s+business\s+as|also\s+known\s+as|formerly\s+known\s+as)\s+",
    re.IGNORECASE,
)


def _trim_procedural_caption(text: str) -> str:
    """Remove structural matter following an ``In re``-style case name.

    Lowercase text alone is not a boundary (``In re Estate of Smith`` and
    ballot-title matters need it).  Alias introductions and comma-delimited
    litigation roles are reliable boundaries, however: neither identifies the
    primary party used in the citation.  Cutting at the marker also handles a
    run of aliases whose individual corporate suffixes contain commas.
    """
    alias = _PROCEDURAL_ALIAS_RE.search(text or "")
    if alias:
        text = text[:alias.start()]
    text = re.sub(
        r",\s*(?:et\s+al\.?|debtors?|appellants?|appellees?|petitioners?|"
        r"respondents?|relators?|movants?|claimants?|plaintiffs?|defendants?|"
        r"creditors?|trustees?)\b.*$",
        "", text, flags=re.IGNORECASE,
    )
    return text.strip().rstrip(",")


def _scholar_caption_name(blocks) -> str:
    """Bluebook case name derived from the Scholar page's party caption."""

    def refine(name: str) -> str:
        body = _scholar_body_text(blocks)
        return simplify_historical_entity_caption(
            refine_caption_case(name, body), body)

    for b in blocks[:8]:
        if b.kind != "center":
            continue
        # Keep a trailing period here — it may belong to an entity abbreviation
        # ending the caption ("… v. Acme Co."); _caption_party drops only a
        # stray one.
        # A reporter star-page can occur in the middle of a long, wrapped
        # caption.  It is pagination metadata, not part of the case name.
        t = re.sub(
            r"\s+", " ",
            "".join(s.text for s in b.spans if not s.pagenum),
        ).strip()
        # An Alabama-style "(Re <underlying case>)" cross-reference carries
        # its own " v. " and would masquerade as this case's caption.
        t = strip_related_case_note(t)
        if not t or _HEADER_CITE_RE.match(t) or t.startswith(("No.", "Nos.")):
            continue
        # An in-re style caption owns no separator, so any "v." inside it
        # belongs to a companion case ("In re Nexium Antitrust Litigation.
        # AstraZeneca AB v. United Food…") — classify it before splitting.
        # Only a role tail is cut at a comma; a comma can continue the
        # matter's own title ("In re Title, Ballot Title & Submission
        # Clause for 2015-2016 #156").  Companions are cut BEFORE the role
        # trim: trimming first would delete the companion's ", et al.,
        # Defendants-Appellants, v. …" tail — the very " v. " that proves
        # a companion case follows.
        if re.match(r"(?:IN\s+RE|EX\s+PARTE|(?:IN\s+THE\s+)?MATTER\s+OF)\b", t, re.IGNORECASE):
            t2 = _trim_procedural_caption(_cut_companion_cases(t))
            return refine(_titlecase_caps(t2.strip()))
        # Google Scholar renders the party separator in lowercase ("… v. …")
        # even for ALL-CAPS captions ("MERCY HOSPITAL, INC. v. JACKSON"), so
        # a lowercase "v."/"vs." — or the earliest reports' spelled-out
        # "versus"/"against" — is the reliable separator and never collides
        # with an uppercase middle initial like the "V." in "Francis V.
        # Lorenzo" or a party's own "AGAINST".  Only fall back to a
        # case-insensitive split (for a caption that happens to capitalize
        # the separator) when no lowercase one is found.
        sides = re.split(r"\s+(?:vs?\.|versus|against)\s+", t, maxsplit=1)
        if len(sides) != 2:
            sides = re.split(r"\s+[vV]s?\.\s+", t, maxsplit=1)
        if len(sides) != 2:
            sides = re.split(
                r"\s+(?:VERSUS|Versus|AGAINST|Against)\s+", t, maxsplit=1)
        if len(sides) == 2:
            sides[1] = _cut_companion_cases(sides[1])
            left, right = _caption_party(sides[0]), _caption_party(sides[1])
            if left and right:
                return refine(f"{left} v. {right}")
        # A single-party caption ("HAYBURN'S CASE.", "THE AMISTAD.") has no
        # separator to split on; without this branch the cite line would
        # become the case name downstream.  Court, date, and docket lines
        # never qualify (cites and "No." lines were filtered above; the
        # digit guard covers public-domain cites — "2006-Ohio-3799" — that
        # the volume-reporter-page pattern does not).
        if (len(t) <= 80
                and sum(c.isalpha() for c in t) >= 4
                and not t[0].isdigit()
                and "court" not in t.lower()
                and not re.match(
                    r"(?:Argued|Reargued|Decided|Submitted|Filed|Released|"
                    r"January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\b",
                    t, re.IGNORECASE)
                # Procedural notes ride the header as their own centered
                # lines ("As Modified on Denial of Petition for Rehearing
                # December 1, 1964.") and are never the caption.
                and not re.search(
                    r"\b(?:rehearing|certiorari|en banc|as amended|"
                    r"as modified|as corrected|petition for)\b",
                    t, re.IGNORECASE)):
            return refine(
                _titlecase_caps(_cut_companion_cases(t).split(",")[0].strip()))
    return ""


def _scholar_body_text(blocks) -> str:
    """The opinion's prose paragraphs, joined — the mixed-case evidence
    :func:`refine_caption_case` reads party-name capitalization from.
    Capped: the parties are named within the first pages."""
    out: list = []
    total = 0
    for b in blocks:
        if b.kind in ("center", "heading"):
            continue  # caption/heading lines: their casing is stylized
        t = re.sub(r"\s+", " ", b.text()).strip()
        if not t:
            continue
        out.append(t)
        total += len(t)
        if total > 40000:
            break
    return "\n".join(out)


def _scholar_source_segments(source: str) -> list[str]:
    """A Scholar result byline reads "<citations> - <court>, <year> -
    Google Scholar"; split it on the dashes and drop the publisher tail."""
    segs = [s.strip() for s in re.split(r"\s+-\s+", source or "") if s.strip()]
    while segs and segs[-1].lower() == "google scholar":
        segs.pop()
    return segs


def _scholar_court_desc_to_id(desc: str) -> str:
    """Map a Scholar court description to a CourtListener court id.  Handles
    state-prefixed bylines ("Cal: Court of Appeal", "La: Court of Appeals,
    4th Circuit") and federal ones ("Supreme Court", "Court of Appeals, 9th
    Circuit"), keeping a state's own appellate circuits out of the federal
    circuits."""
    desc = re.sub(r"\s+", " ", desc or "").strip().rstrip(".")
    if not desc:
        return ""
    # State-prefixed: a Bluebook state abbreviation, then a colon.
    m = re.match(r"([A-Za-z][A-Za-z.]{0,5}):\s*(.+)$", desc)
    if m:
        key = m.group(1).replace(".", "").lower()
        courts = _SCHOLAR_STATE_PREFIX.get(key)
        if courts:
            return _classify_state_court(m.group(2), courts)
        desc = m.group(2)  # unknown prefix — classify the remainder generically
    low = desc.lower()
    if low in ("supreme court", "us supreme court", "u.s. supreme court",
               "united states supreme court") or (
            "supreme court" in low and "united states" in low):
        return "scotus"
    m = re.search(
        r"court of appeals,?\s*(?:for the\s+)?(\w+(?: of columbia)?)\s+circuit", low
    )
    if m:
        word = m.group(1)
        if word == "federal":
            return "cafc"
        if "columbia" in word or word == "dc":
            return "cadc"
        return _CIRCUIT_ORDINALS.get(word, "")
    return ""


def _scholar_source_to_court_id(source: str) -> str:
    """Parse a Scholar result's source byline into a CourtListener court id."""
    segs = _scholar_source_segments(source)
    if not segs:
        return ""
    court_year = re.sub(r",?\s*(1[6-9]\d{2}|20\d{2})\s*$", "", segs[-1])
    return _scholar_court_desc_to_id(court_year)


def _scholar_source_year(source: str) -> str:
    """Extract the decision year from a Scholar source byline (preferring the
    year trailing the court segment over any stray year in the citations)."""
    segs = _scholar_source_segments(source)
    if segs:
        m = re.search(r"(1[6-9]\d{2}|20\d{2})\s*$", segs[-1])
        if m:
            return m.group(1)
    m = re.search(r"\b(1[6-9]\d{2}|20\d{2})\b", source or "")
    return m.group(1) if m else ""


def _normalize_scholar_cite(cite: str) -> str:
    """Normalize a Scholar-style reporter citation to Bluebook form: restore
    the periods Scholar drops from multi-capital reporters ("US" → "U.S.",
    "NW" → "N.W.") and fix reporter spacing ("F. 3d" → "F.3d")."""
    cite = re.sub(r"\s+", " ", cite or "").strip().strip(",")
    m = re.match(r"^(\d+)\s+(.+?)\s+(\d+)$", cite)
    if not m:
        return cite
    vol, rep, page = m.group(1), m.group(2), m.group(3)
    rep = re.sub(r"\b([A-Z]{2,})\b",
                 lambda mm: ".".join(mm.group(1)) + ".", rep)
    rep = _respace_reporter(rep)
    return f"{vol} {rep} {page}"


def _scholar_source_cite(source: str) -> str:
    """Pick the best reporter citation from a Scholar byline's leading
    citation segment ("529 NW 2d 155" / "512 US 477, 114 S. Ct. 2364 …"),
    normalized to Bluebook form."""
    segs = _scholar_source_segments(source)
    if len(segs) < 2:
        return ""  # only a court/year segment, no citations
    cites = []
    for part in segs[0].split(","):
        part = part.strip()
        if not part or "…" in part or "..." in part:
            continue  # skip truncated parallel cites
        norm = _normalize_scholar_cite(part)
        if re.match(r"^\d+\s+.+\s+\d+$", norm):
            cites.append(norm)
    return _pick_citation(cites) if cites else ""


def _scholar_result_cite(r) -> str:
    """A reporter citation for a Scholar search result — from its byline, or
    failing that its title/snippet.  Used to locate the case on CourtListener
    when Scholar's opinion page won't load."""
    cite = _scholar_source_cite(getattr(r, "source", "") or "")
    if not cite:
        m = _TEXT_CITE_RE.search(
            f"{getattr(r, 'title', '')} {getattr(r, 'snippet', '')}"
        )
        if m:
            cite = re.sub(r"\s+", " ", m.group(0))
    return cite or ""


def _rtf_escape(s: str) -> str:
    out: list[str] = []
    for ch in s:
        if ch in "\\{}":
            out.append("\\" + ch)
        elif ch == "\n":
            out.append("\\line ")
        elif ord(ch) < 128:
            out.append(ch)
        else:
            cp = ord(ch)
            if cp > 32767:  # RTF \u takes a signed 16-bit value
                cp -= 65536
            out.append(f"\\u{cp}?")
    return "".join(out)


# Color table: 1 = star-pagination marker (purple), 2 = dissent (dark red),
# 3 = concurrence (dark green), 4 = unclassified separate opinion (slate).
# Citation links stay black in copied and
# exported text; the blue is only an on-screen affordance.  The dissent/
# concurrence colors are used only on the running heading of a section in the
# RTF export — opinion body text is always black.
_RTF_HEADER = (
    "{\\rtf1\\ansi\\deff0"
    "{\\fonttbl{\\f0\\froman Times New Roman;}}"
    "{\\colortbl ;\\red142\\green68\\blue173;"
    "\\red163\\green21\\blue21;\\red26\\green122\\blue60;"
    "\\red89\\green99\\blue111;}"
    "\\f0\\fs22\n"
)


def _rtf_document(
    body: str, two_columns: bool = False, page_footer: bool = False
) -> str:
    sect = "\\sectd\\sbknone\\cols2\\colsx432\n" if two_columns else ""
    footer = "{\\footer\\pard\\qc\\fs18\\chpgn\\par}\n" if page_footer else ""
    return _RTF_HEADER + sect + footer + body + "}"


def _run_to_rtf(seg: str, active: set[str], part_colors: bool = False) -> str:
    codes: list[str] = []
    for t in active:
        if t.startswith("fnt_") and len(t) == 8:
            italic, bold, small, sup = (c == "1" for c in t[4:])
            if italic:
                codes.append("\\i")
            if bold:
                codes.append("\\b")
            if small:
                codes.append("\\fs18")
            if sup:
                codes.append("\\super\\fs16")
    if "underline" in active:
        codes.append("\\ul")
    if "pagenum" in active:
        codes.append("\\cf1\\b")
    elif part_colors and "part-dissent" in active:
        codes.append("\\cf2")
    elif part_colors and "part-concurrence" in active:
        codes.append("\\cf3")
    elif part_colors and "part-separate" in active:
        codes.append("\\cf4")
    esc = _rtf_escape(seg)
    return "{" + "".join(codes) + " " + esc + "}" if codes else esc


def _fn_bookmark(side: str, fid: str) -> str:
    """RTF bookmark name for a footnote anchor: the in-text reference
    ("fnref") or the footnote body ("fndef")."""
    safe = re.sub(r"\W+", "_", str(fid))
    return ("FNR_" if side == "fnref" else "FNB_") + safe


def _collapse_pagenum_omission_seam(
    value: str, previous_char: str, omitted_pagenum: bool,
) -> str:
    """Remove the right-hand space only when omitting an inline page marker
    would otherwise leave spaces on both sides of the omission.

    This deliberately does not insert spacing where either side lacked it and
    does not collapse across a newline.  CourtListener source text is not as
    consistent as Scholar about surrounding ``*123`` with two spaces.
    """
    if omitted_pagenum and previous_char == " " and value.startswith(" "):
        return value.lstrip(" ")
    return value


def _dump_to_rtf(
    txt: tk.Text, start: str, end: str, part_colors: bool = False,
    fn_links: Optional[dict[str, tuple[str, str]]] = None,
    omit_tags: Optional[set[str]] = None,
) -> str:
    """Convert a Tk Text range (with the Scholar window's tags) to an RTF
    body.  `fn_links` maps link-tag names to ("fnref"|"fndef", id);
    matching runs become RTF bookmark/hyperlink pairs so footnote markers
    stay clickable in the exported document.  Text under any tag in
    `omit_tags` is dropped (quote-ready copies omit footnote markers)."""
    fn_links = fn_links or {}
    omit_tags = omit_tags or set()
    out: list[str] = []
    # Seed with tags already open at *start*; dump only reports transitions.
    active: set[str] = set(txt.tag_names(start))
    active.discard("sel")
    par_open = False
    pending_marks: list[str] = []   # bookmarks to emit at the next run
    marks_done: set[str] = set()    # bookmark names must be unique
    omitted_pagenum = False
    previous_char = ""

    def par_prefix() -> str:
        if "center" in active:
            return "\\pard\\qc\\sa120 "
        if "blockquote" in active:
            return "\\pard\\li720\\ri720\\sa120 "
        return "\\pard\\sa120 "

    def queue_mark(tag: str) -> None:
        side, fid = fn_links[tag]
        name = _fn_bookmark(side, fid)
        if name not in marks_done:
            marks_done.add(name)
            pending_marks.append(
                "{\\*\\bkmkstart " + name + "}{\\*\\bkmkend " + name + "}"
            )

    def fn_target() -> Optional[str]:
        for t in active:
            if t in fn_links:
                side, fid = fn_links[t]
                # a reference links to the body and vice versa
                return _fn_bookmark("fndef" if side == "fnref" else "fnref",
                                    fid)
        return None

    for t in active:
        if t in fn_links:
            queue_mark(t)
    for key, value, _index in txt.dump(start, end, text=True, tag=True):
        if key == "tagon":
            active.add(value)
            if value in fn_links:
                queue_mark(value)
        elif key == "tagoff":
            active.discard(value)
        elif key == "text":
            if "justify-pad" in active:
                continue
            if active & omit_tags:
                if "pagenum" in active and "pagenum" in omit_tags:
                    omitted_pagenum = True
                continue
            value = _collapse_pagenum_omission_seam(
                value, previous_char, omitted_pagenum,
            )
            omitted_pagenum = False
            for i, seg in enumerate(value.split("\n")):
                if i and par_open:
                    out.append("\\par\n")
                    par_open = False
                if seg:
                    if not par_open:
                        out.append(par_prefix())
                        par_open = True
                    if pending_marks:
                        out.extend(pending_marks)
                        pending_marks.clear()
                    run = _run_to_rtf(seg, active, part_colors)
                    target = fn_target()
                    if target:
                        run = ("{\\field{\\*\\fldinst{HYPERLINK \\\\l \""
                               + target + "\"}}{\\fldrslt " + run + "}}")
                    out.append(run)
            if value:
                previous_char = value[-1]
    if par_open:
        out.append("\\par\n")
    return "".join(out)


def _blocks_rtf_body(blocks) -> str:
    """An RTF body built straight from parsed opinion blocks — no Tk widget —
    so the "Download Cited Cases" compiler can export an opinion to RTF off the
    main thread.  Mirrors :func:`_dump_to_rtf`'s run handling (the same
    ``_run_to_rtf`` codes for italic/bold/small/super/underline and the purple
    star-pagination markers), producing the same document the reader's
    "Export → RTF" does.  The block list already holds the whole opinion,
    footnotes included, so nothing else need be appended."""
    out: list[str] = []
    for b in blocks:
        spans = getattr(b, "spans", None) or []
        if not spans:
            continue
        kind = getattr(b, "kind", "para")
        if kind == "center":
            prefix = "\\pard\\qc\\sa120 "
        elif kind == "blockquote":
            prefix = "\\pard\\li720\\ri720\\sa120 "
        else:
            prefix = "\\pard\\sa120 "
        runs: list[str] = []
        for s in spans:
            text = getattr(s, "text", "") or ""
            if not text:
                continue
            active = {
                "fnt_"
                + ("1" if getattr(s, "italic", False) else "0")
                + ("1" if getattr(s, "bold", False) else "0")
                + ("1" if getattr(s, "small", False) else "0")
                + ("1" if getattr(s, "sup", False) else "0")
            }
            if getattr(s, "underline", False):
                active.add("underline")
            if getattr(s, "pagenum", False):
                active.add("pagenum")
            runs.append(_run_to_rtf(text, active))
        joined = "".join(runs)
        if not joined.strip():
            continue
        out.append(prefix + joined + "\\par\n")
    return "".join(out)


def _opinion_rtf_from_blocks(blocks, citation_line: str = "") -> str:
    """A complete two-column RTF document for an opinion, from parsed blocks.

    When *citation_line* (the Bluebook citation) is given it becomes a centered
    running head on every page — matching the reader's RTF export — over a
    page-number footer.  Used by the brief-authority compiler, which has no live
    reader window to dump from."""
    body = _blocks_rtf_body(blocks)
    if citation_line:
        header = ("{\\header\\pard\\qc\\fs18\\i "
                  + _rtf_escape(citation_line.rstrip(".")) + "\\par}\n")
        return (_RTF_HEADER + "\\sectd\\sbknone\\cols2\\colsx432\n"
                + header + "{\\footer\\pard\\qc\\fs18\\chpgn\\par}\n"
                + body + "}")
    return _rtf_document(body, two_columns=True, page_footer=True)


def _scholar_item_from_blocks(blocks, fallback_name: str = "",
                              fallback_cite: str = "") -> dict:
    """A search-result-shaped item (caseName, citation, dateFiled, court_id)
    scraped from an opinion's own header blocks, so :func:`_build_default_filename`
    can produce a Bluebook file name even when CourtListener has no record of
    the case (a Google-Scholar-only opinion)."""
    name = _scholar_caption_name(blocks) or fallback_name
    header = "  ".join(
        b.text() for b in blocks[:8]
        if getattr(b, "kind", "") in ("center", "heading")
    )
    header = re.sub(r"\bU\.\s+S\.", "U.S.", header)
    header = re.sub(r"\b(\d{1,4})\s+US\s+(\d{1,5})\b", r"\1 U.S. \2", header)
    cite = ""
    m = _TEXT_CITE_RE.search(header)
    if m:
        cite = re.sub(r"\s+", " ", m.group(0)).strip()
    if not cite:
        cite = fallback_cite
    years = re.findall(r"\b(1[6-9]\d{2}|20\d{2})\b", header)
    year = years[-1] if years else ""
    return {
        "caseName": name,
        "citation": [cite] if cite else [],
        "dateFiled": f"{year}-01-01" if year else "",
        "court_id": _scholar_court_id(blocks),
    }


def _plain_without_layout_chars(
    txt: tk.Text, start: str, end: str,
    omit_tags: Optional[set[str]] = None,
) -> str:
    """Text content without temporary on-screen justification fragments.
    Text under any tag in `omit_tags` is dropped too (omitted footnote
    markers in quote-ready copies)."""
    out: list[str] = []
    omit_tags = omit_tags or set()
    active: set[str] = set(txt.tag_names(start))
    omitted_pagenum = False
    previous_char = ""
    for key, value, _index in txt.dump(start, end, text=True, tag=True):
        if key == "tagon":
            active.add(value)
        elif key == "tagoff":
            active.discard(value)
        elif key == "text" and "justify-pad" not in active:
            if active & omit_tags:
                if "pagenum" in active and "pagenum" in omit_tags:
                    omitted_pagenum = True
                continue
            value = _collapse_pagenum_omission_seam(
                value, previous_char, omitted_pagenum,
            )
            omitted_pagenum = False
            out.append(value)
            if value:
                previous_char = value[-1]
    return "".join(out)


def _dump_statute_rtf(txt: tk.Text, start: str, end: str) -> str:
    """Convert a statute / rule / constitution window's Text range to an RTF
    body, mapping the ``_StatuteWindow`` tag set — section headings, bold
    enumerators, indent levels, source credits and notes — to RTF.  Mirrors
    :func:`_dump_to_rtf` but for that window's tags (the Scholar tags it knows
    don't appear here, so the two need separate dumpers)."""
    out: list[str] = []
    active: set[str] = set(txt.tag_names(start))
    active.discard("sel")
    par_open = False

    def indent_level() -> int:
        for i in range(6, -1, -1):
            if f"ind{i}" in active:
                return i
        return 0

    def par_prefix() -> str:
        parts = ["\\pard"]
        li = indent_level() * 360  # ~0.25" per nesting level
        if li:
            parts.append(f"\\li{li}")
        if "sechead" in active:
            parts.append("\\sb120\\sa120")
        elif "credit" in active or "notehead" in active:
            parts.append("\\sb120\\sa60")
        else:
            parts.append("\\sa120")
        return "".join(parts) + " "

    def run_to_rtf(seg: str) -> str:
        codes: list[str] = []
        if "sechead" in active:
            codes.append("\\b\\fs28")
        elif "headline" in active or "notehead" in active or "enum" in active:
            codes.append("\\b")
        if "credit" in active or "notebody" in active:
            codes.append("\\fs18")
        esc = _rtf_escape(seg)
        return "{" + "".join(codes) + " " + esc + "}" if codes else esc

    for key, value, _index in txt.dump(start, end, text=True, tag=True):
        if key == "tagon":
            active.add(value)
        elif key == "tagoff":
            active.discard(value)
        elif key == "text":
            for i, seg in enumerate(value.split("\n")):
                if i and par_open:
                    out.append("\\par\n")
                    par_open = False
                if seg:
                    if not par_open:
                        out.append(par_prefix())
                        par_open = True
                    out.append(run_to_rtf(seg))
    if par_open:
        out.append("\\par\n")
    return "".join(out)


# ---------------------------------------------------------------------------
# PDF (LaTeX) + Markdown export.  Both writers share a formatting-preserving
# dump of the reader's Tk Text widget (_dump_export_paragraphs) that mirrors
# _dump_to_rtf's tag handling: on-screen justification fragments are dropped,
# the hidden hyphenation originals are kept.
# ---------------------------------------------------------------------------


@dataclass
class _ExpRun:
    """One styled run of exported opinion text."""

    text: str
    italic: bool = False
    bold: bool = False
    small: bool = False
    sup: bool = False
    underline: bool = False
    pagenum: bool = False   # reporter star-pagination marker, e.g. "*123"
    fnref: str = ""         # footnote anchor id of an in-text reference
    fndef: str = ""         # footnote anchor id opening a footnote body

    def same_style(self, other: "_ExpRun") -> bool:
        return (
            self.italic, self.bold, self.small, self.sup, self.underline,
            self.pagenum, self.fnref, self.fndef,
        ) == (
            other.italic, other.bold, other.small, other.sup,
            other.underline, other.pagenum, other.fnref, other.fndef,
        )


@dataclass
class _ExpPara:
    """One paragraph of exported opinion text."""

    kind: str = "para"      # para | center | blockquote | heading | fnhead
    runs: list = field(default_factory=list)


def _tk_ix(index: str) -> tuple[int, int]:
    """A Tk Text index string ("line.char") as a comparable tuple."""
    line, _, char = index.partition(".")
    return int(line), int(char)


def _dump_export_paragraphs(
    txt: tk.Text,
    ranges: list[tuple[str, str]],
    fn_links: dict[str, tuple[str, str]],
) -> list[_ExpPara]:
    """Convert Tk Text ranges (with the Scholar window's tags) into styled
    paragraphs — the shared source for the LaTeX and Markdown exports."""
    paras: list[_ExpPara] = []
    for start, end in ranges:
        active: set[str] = set(txt.tag_names(start))
        active.discard("sel")
        cur: Optional[_ExpPara] = None

        def para_kind() -> str:
            for k in ("fnhead", "center", "blockquote", "heading"):
                if k in active:
                    return k
            return "para"

        def styled(seg: str) -> _ExpRun:
            r = _ExpRun(text=seg)
            for t in active:
                if len(t) == 8 and t[:4] in ("fnt_", "fna_"):
                    r.italic, r.bold, r.small, r.sup = (
                        c == "1" for c in t[4:]
                    )
                elif t == "underline":
                    r.underline = True
                elif t == "pagenum":
                    r.pagenum = True
                elif t in fn_links:
                    side, fid = fn_links[t]
                    if side == "fnref":
                        r.fnref = fid
                    else:
                        r.fndef = fid
            return r

        for key, value, _index in txt.dump(start, end, text=True, tag=True):
            if key == "tagon":
                active.add(value)
            elif key == "tagoff":
                active.discard(value)
            elif key == "text":
                if "justify-pad" in active:
                    continue
                for i, seg in enumerate(value.split("\n")):
                    if i and cur is not None:
                        paras.append(cur)
                        cur = None
                    if seg:
                        if cur is None:
                            cur = _ExpPara(kind=para_kind())
                        run = styled(seg)
                        if cur.runs and cur.runs[-1].same_style(run):
                            cur.runs[-1].text += seg
                        else:
                            cur.runs.append(run)
        if cur is not None:
            paras.append(cur)
    return paras


def _strip_note_marker(paras: list[_ExpPara]) -> tuple[str, str]:
    """Remove the leading footnote marker from a note body's paragraphs.
    Returns (anchor id, marker text) — either may be empty."""
    if not paras or not paras[0].runs:
        return "", ""
    runs = paras[0].runs
    for j, r in enumerate(runs[:2]):
        if r.fndef:
            disp = r.text.strip()
            del runs[j]
            if j < len(runs):
                runs[j].text = runs[j].text.lstrip()
            return r.fndef, disp
    t = runs[0].text
    m = _FN_BODY_MARK_RE.match(t)
    if m:
        runs[0].text = t[m.end():].lstrip()
        return "", (m.group(1) or m.group(2) or "").strip()
    return "", ""


def _section_body_ranges(
    sec_start: str, sec_end: str, note_regions: list
) -> list[tuple[str, str]]:
    """The section's text ranges with its footnote-body regions cut out."""
    ranges: list[tuple[str, str]] = []
    cur = sec_start
    for rs, rend, _num, _page in note_regions:
        if _tk_ix(rs) > _tk_ix(cur):
            ranges.append((cur, rs))
        if _tk_ix(rend) > _tk_ix(cur):
            cur = rend
    if _tk_ix(sec_end) > _tk_ix(cur):
        ranges.append((cur, sec_end))
    return ranges


# LaTeX escapes: specials, plus TeX idioms for the typographic characters
# opinions use constantly (quotes, dashes, §, ¶) so every engine renders
# them with proper kerning.
_LATEX_CHAR_MAP = {
    "\\": r"\textbackslash{}", "{": r"\{", "}": r"\}", "$": r"\$",
    "&": r"\&", "#": r"\#", "_": r"\_", "%": r"\%",
    "~": r"\textasciitilde{}", "^": r"\textasciicircum{}",
    " ": "~",  # non-breaking space
    "‘": "`", "’": "'",
    "“": "``", "”": "''",
    "–": "--", "—": "---",
    "…": r"\ldots{}", "•": r"\textbullet{}",
    "§": r"\S{}", "¶": r"\P{}",
    "†": r"\dag{}", "‡": r"\ddag{}",
}


def _latex_escape(s: str) -> str:
    return "".join(_LATEX_CHAR_MAP.get(ch, ch) for ch in s)


def _latex_run(
    run: _ExpRun,
    notes: Optional[dict[str, tuple[str, str]]],
    used: Optional[set[str]],
    marks: bool = True,
    has_prior_text: bool = False,
) -> str:
    """One styled run as LaTeX.  `notes` maps footnote anchor ids to
    (label, body-latex); the first reference to a note replaces its marker
    with a real \\footnote so it lands at the bottom of that page."""
    if run.pagenum:
        text = run.text
        lead = text[: len(text) - len(text.lstrip())]
        trail = text[len(text.rstrip()):]
        disp = _latex_escape(text.strip())
        m = re.search(r"\d+", text)
        if marks and m:
            return (
                lead
                + "\\starpage{%s}{%s}{%d}"
                % (m.group(0), disp, 1 if has_prior_text else 0)
                + trail
            )
        return lead + "\\starpagetext{%s}" % disp + trail
    if run.fnref:
        if notes and used is not None and run.fnref in notes \
                and run.fnref not in used:
            used.add(run.fnref)
            label, body = notes[run.fnref]
            return "\\opfootnote{%s}{%s}" % (_latex_escape(label), body)
        # A repeated reference (or one whose body wasn't found): keep the
        # superscript marker.
        return "\\textsuperscript{%s}" % _latex_escape(run.text.strip())
    if run.fndef:
        return "\\textsuperscript{%s}" % _latex_escape(run.text.strip())
    out = _latex_escape(run.text)
    if run.underline:
        out = "\\opuline{" + out + "}"
    if run.bold:
        out = "\\textbf{" + out + "}"
    if run.italic:
        out = "\\textit{" + out + "}"
    if run.sup:
        out = "\\textsuperscript{" + out + "}"
    elif run.small:
        # \small{} rather than "\small ": a control word would eat the
        # run's own leading space ("…an {\small  inside}" → "aninside").
        out = "{\\small{}" + out + "}"
    return out


# A section marker standing alone on its own (usually centered) line — the
# way Supreme Court opinions head their parts: a bare roman numeral ("II"),
# capital letter ("A") or number ("1"), optionally compounded ("II-B"), with
# an optional trailing period.  Star-pagination markers may precede it.
_BARE_HEADING_RE = re.compile(
    r"^(?:[IVXLCDM]+|[A-Z]|\d{1,2})"
    r"(?:\s*[-–—.]\s*(?:[IVXLCDM]+|[A-Z]|\d{1,2}))?\s*\.?$"
)


def _para_plain_text(p: _ExpPara) -> str:
    """The paragraph's prose, page markers and footnote anchors dropped."""
    return "".join(
        r.text for r in p.runs if not (r.pagenum or r.fnref or r.fndef)
    ).strip()


def _heading_like(p: _ExpPara) -> bool:
    """True when the paragraph acts as a section heading for page-break
    purposes: an explicit heading block, or a centered section marker —
    Supreme Court opinions head their parts with a bare "II" / "A" / "1" on
    its own centered line, other courts with a short bold centered title."""
    if p.kind == "heading":
        return True
    if p.kind != "center":
        return False
    text = re.sub(r"\s+", " ", _para_plain_text(p))
    text = re.sub(r"^(?:\*\d+\s*)+", "", text).strip()  # leading page markers
    if not text:
        return False
    if _BARE_HEADING_RE.match(text):
        return True
    # A short, all-bold centered line mid-opinion is an explanatory heading
    # ("I. BACKGROUND").  " v. " keeps centered case-caption lines out.
    styled = [r for r in p.runs
              if r.text.strip() and not (r.pagenum or r.fnref or r.fndef)]
    return (len(text) <= 80 and " v. " not in text
            and bool(styled) and all(r.bold for r in styled))


def _latex_paragraphs(
    paras: list[_ExpPara],
    notes: Optional[dict[str, tuple[str, str]]] = None,
    used: Optional[set[str]] = None,
    title_block: bool = False,
) -> str:
    """Paragraphs as LaTeX body text.  With `title_block`, a leading centered
    paragraph (the case caption) is set large and bold.

    Headings — explicit heading blocks and the bare centered section markers
    Supreme Court opinions use ("II", "A", "1") — are glued to what follows
    with \\nopagebreak so a heading (or a run of headings) can never be left
    at the bottom of a page while its text starts on the next: TeX then has
    no legal break point between the heading and the second line of the
    following paragraph (\\clubpenalty already guards the first), so the
    whole group moves to the fresh page instead."""
    # Pass 1: typeset each paragraph, in order (footnote placement depends on
    # first-reference order), remembering which ones behave as headings.
    items: list[tuple[_ExpPara, str, bool]] = []  # (para, latex, is_title)
    title_pending = title_block
    for p in paras:
        if p.kind == "fnhead":
            continue  # "Footnotes" label: notes are typeset at page bottoms
        pieces: list[str] = []
        seen_text = False
        for r in p.runs:
            pieces.append(_latex_run(
                r, notes, used, has_prior_text=seen_text))
            if (not r.pagenum and not r.fnref and not r.fndef
                    and r.text.strip()):
                seen_text = True
        body = "".join(pieces).strip()
        if not body:
            continue
        items.append((p, body, title_pending and p.kind == "center"))
        title_pending = False

    # Pass 2: assemble, keeping headings attached to their following text.
    out: list[str] = []
    quote_open = False

    def close_quote() -> None:
        nonlocal quote_open
        if quote_open:
            out.append("\\end{quote}\n\n")
            quote_open = False

    for i, (p, body, is_title) in enumerate(items):
        if p.kind == "blockquote":
            if not quote_open:
                out.append("\\begin{quote}\n")
                quote_open = True
            out.append(body + "\n\n")
            continue
        close_quote()
        # \nopagebreak only helps when something follows in this section.
        heading = not is_title and _heading_like(p)
        glue = "\\nopagebreak" if heading and i + 1 < len(items) else ""
        if p.kind == "center":
            if is_title:
                body = "{\\large\\bfseries " + body + "}"
            if heading:
                # Not a center environment: its surrounding list glue would
                # reopen a legal page-break point after the \nopagebreak.
                out.append("\\medskip{\\centering " + body + "\\par}"
                           + glue + "\\medskip\n\n")
            else:
                out.append("\\begin{center}\n" + body + "\n\\end{center}\n\n")
        elif p.kind == "heading":
            out.append("\\medskip\\noindent\\textbf{" + body + "}\\par"
                       + glue + "\n\n")
        else:
            out.append(body + "\n\n")
    close_quote()
    return "".join(out)


def _latex_footnote_body(paras: list[_ExpPara]) -> str:
    """A footnote's paragraphs as the argument of \\footnote (page markers
    inside a note stay visible but don't feed the running head)."""
    chunks: list[str] = []
    for p in paras:
        if p.kind == "fnhead":
            continue
        body = "".join(
            _latex_run(r, None, None, marks=False) for r in p.runs
        ).strip()
        if body:
            chunks.append(body)
    return "\\par ".join(chunks)


# Single column, justified (LaTeX's default), first-line paragraph indents,
# widow/orphan control — the layout of a printed reporter.  The face is
# Century Schoolbook (the Supreme Court's own), falling back to Palatino,
# then Times, then Computer Modern, whichever the local TeX carries.
_LATEX_PREAMBLE = r"""\documentclass[11pt]{article}
\usepackage{iftex}
\ifPDFTeX
  \usepackage[T1]{fontenc}
  \usepackage[utf8]{inputenc}
  \usepackage{textcomp}
  \IfFileExists{tgschola.sty}{\usepackage{tgschola}}{%
    \IfFileExists{tgpagella.sty}{\usepackage{tgpagella}}{%
      \IfFileExists{mathptmx.sty}{\usepackage{mathptmx}}{}}}
\else
  \usepackage{fontspec}
  \IfFontExistsTF{TeX Gyre Schola}{\setmainfont{TeX Gyre Schola}}{%
    \IfFontExistsTF{TeX Gyre Pagella}{\setmainfont{TeX Gyre Pagella}}{}}
\fi
\IfFileExists{microtype.sty}{\usepackage{microtype}}{}
\usepackage{graphicx}
\IfFileExists{ulem.sty}{\usepackage[normalem]{ulem}%
  \newcommand{\opuline}[1]{\uline{#1}}}{%
  \newcommand{\opuline}[1]{\underline{#1}}}
\usepackage[letterpaper,margin=1.1in,headheight=15pt,footskip=0.55in]{geometry}
\usepackage{fancyhdr}
\setlength{\parindent}{1.4em}
\setlength{\parskip}{0pt plus 1pt}
\setlength{\emergencystretch}{1.5em}
\clubpenalty=10000 \widowpenalty=10000
\raggedbottom
\setlength{\footnotesep}{0.85\baselineskip}
\addtolength{\skip\footins}{0.4\baselineskip}
% Star pagination: an inline reporter page marker that also feeds the
% running head's page range through TeX's mark mechanism.  Marks store
% {last reporter page visible}{first reporter page visible}.  A marker
% such as *23 marks the break from reporter page 22 to 23, so a sheet that
% contains actual text before that marker reports 22--23.  If the marker is
% the first actual text on the sheet, the sheet begins on 23.
\newcount\opfirstreporterpage
\opfirstreporterpage=1
\newcommand{\opfirstvisiblepage}[1]{%
  \ifnum#1>\opfirstreporterpage
    \number\numexpr#1-1\relax
  \else
    #1%
  \fi}
\newcommand{\starpage}[3]{%
  \ifnum#3>0
    \markboth{#1}{\opfirstvisiblepage{#1}}%
  \else
    \ifdim\pagetotal>0pt
      \markboth{#1}{\opfirstvisiblepage{#1}}%
    \else
      \markboth{#1}{#1}%
    \fi
  \fi
  {\bfseries\footnotesize #2}}
\newcommand{\starpagetext}[1]{{\bfseries\footnotesize #1}}
% A footnote carrying the reporter's own number (or symbol).
\newcommand{\opfootnote}[2]{\begingroup
  \renewcommand{\thefootnote}{#1}\footnote{#2}\endgroup}
% Reporter pages visible on the current sheet: first mark--last mark.
\makeatletter
\newcommand{\reporterrange}{%
  \begingroup
  \edef\rr@first{\rightmark}\edef\rr@last{\leftmark}%
  \ifx\rr@first\rr@last \rr@first\else\rr@first--\rr@last\fi
  \endgroup}
\makeatother
\newcommand{\opinionhead}{}
\newcommand{\reporterhead}{}
\newlength{\opheadavail}
% The header line is assembled by hand so the case line on the left can
% never collide with the reporter page range on the right: both are
% measured, and when the case line would intrude on the range (kept at
% its natural width) it is scaled down just enough to fit beside it.
\newcommand{\opheadline}{%
  \sbox0{\small\opinionhead}%
  \sbox1{\small\reporterhead}%
  \setlength{\opheadavail}{\headwidth}%
  \ifdim\wd1>0pt
    \addtolength{\opheadavail}{-\wd1}%
    \addtolength{\opheadavail}{-1.5em}%
  \fi
  \ifdim\wd0>\opheadavail
    \resizebox{\opheadavail}{!}{\usebox0}%
  \else
    \usebox0%
  \fi
  \hfill\usebox1}
\pagestyle{fancy}
\fancyhf{}
\fancyhead[L]{\opheadline}
\fancyfoot[C]{\small\thepage}
\renewcommand{\headrulewidth}{0.4pt}
\fancypagestyle{plain}{\fancyhf{}\fancyfoot[C]{\small\thepage}%
  \renewcommand{\headrulewidth}{0pt}}
"""


_LATEX_ENGINE_NAMES = ("pdflatex", "xelatex", "lualatex", "tectonic")


def _is_executable_file(path: str) -> bool:
    return bool(path and os.path.isfile(path) and os.access(path, os.X_OK))


def _mac_latex_search_paths() -> list[str]:
    """Common TeX/MiKTeX locations missed by GUI-launched macOS apps."""
    if sys.platform != "darwin":
        return []
    home = str(Path.home())
    paths = [
        "/Library/TeX/texbin",
        "/usr/local/bin",
        "/opt/homebrew/bin",
        "/usr/texbin",
        os.path.join(home, "bin"),
        os.path.join(home, ".local", "bin"),
        "/Applications/MiKTeX Console.app/Contents/bin",
        "/Applications/MiKTeX Console.app/Contents/MacOS",
        os.path.join(home, "Applications", "MiKTeX Console.app",
                     "Contents", "bin"),
        os.path.join(home, "Applications", "MiKTeX Console.app",
                     "Contents", "MacOS"),
    ]
    for parent in (
        Path(home) / "Library" / "Application Support" / "MiKTeX"
        / "texmfs" / "install" / "miktex" / "bin",
        Path("/Library/Application Support/MiKTeX/texmfs/install/miktex/bin"),
    ):
        try:
            paths.extend(str(p) for p in parent.glob("*-darwin") if p.is_dir())
        except OSError:
            pass
    return paths


def _which_from_login_shell(name: str) -> Optional[str]:
    """Find TeX binaries from the user's login shell on macOS."""
    if sys.platform != "darwin":
        return None
    shells = [
        os.environ.get("SHELL", ""),
        "/bin/zsh",
        "/bin/bash",
    ]
    seen: set[str] = set()
    for shell in shells:
        if not shell or shell in seen or not os.path.exists(shell):
            continue
        seen.add(shell)
        try:
            proc = subprocess.run(
                [shell, "-lc", f"command -v {name}"],
                capture_output=True, text=True, errors="replace", timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        for line in (proc.stdout or "").splitlines():
            path = line.strip()
            if os.path.isabs(path) and _is_executable_file(path):
                return path
    return None


def _find_latex_engine() -> Optional[tuple[str, str]]:
    """The first available LaTeX→PDF engine, as (name, executable path)."""
    extra_path = os.pathsep.join(_mac_latex_search_paths())
    for name in _LATEX_ENGINE_NAMES:
        exe = shutil.which(name)
        if exe:
            return name, exe
        if extra_path:
            exe = shutil.which(name, path=extra_path)
            if exe:
                return name, exe
        exe = _which_from_login_shell(name)
        if exe:
            return name, exe
    return None


def _compile_latex(
    tex: str, engine: tuple[str, str], out_pdf: str
) -> tuple[bool, str]:
    """Typeset *tex* with *engine* in a scratch directory and copy the PDF
    to *out_pdf*.  Returns (ok, log tail when not ok)."""
    name, exe = engine
    with tempfile.TemporaryDirectory(prefix="opinion_tex_") as td:
        src = os.path.join(td, "opinion.tex")
        with open(src, "w", encoding="utf-8") as f:
            f.write(tex)
        if name == "tectonic":
            cmd = [exe, "--outdir", td, src]
        else:
            # nonstopmode without -halt-on-error: a stray glyph the font
            # lacks shouldn't sink the whole export.
            cmd = [exe, "-interaction=nonstopmode", "opinion.tex"]
        try:
            proc = subprocess.run(
                cmd, cwd=td, capture_output=True, text=True,
                errors="replace", timeout=300,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return False, str(exc)
        pdf = os.path.join(td, "opinion.pdf")
        if os.path.exists(pdf):
            try:
                shutil.copyfile(pdf, out_pdf)
            except OSError as exc:
                return False, str(exc)
            return True, ""
        log = ""
        log_path = os.path.join(td, "opinion.log")
        if os.path.exists(log_path):
            try:
                with open(log_path, encoding="utf-8", errors="replace") as f:
                    log = f.read()
            except OSError:
                pass
        if not log:
            log = (proc.stdout or "") + (proc.stderr or "")
        lines = [ln for ln in log.splitlines() if ln.strip()]
        return False, "\n".join(lines[-15:])


# Markdown escapes: emphasis/link specials always; list/quote/heading
# markers only where they'd start a block.
_MD_SPECIAL_RE = re.compile(r"([\\`*_\[\]])")
_MD_LINE_START_RE = re.compile(r"^(\s*)([#>+\-])")
_MD_ORDERED_RE = re.compile(r"^(\s*\d+)\.")


def _md_escape(s: str) -> str:
    return _MD_SPECIAL_RE.sub(r"\\\1", s)


def _md_block_guard(s: str) -> str:
    s = _MD_LINE_START_RE.sub(r"\1\\\2", s)
    return _MD_ORDERED_RE.sub(r"\1\\.", s)


def _md_run(run: _ExpRun, ref_ids: dict[str, str]) -> str:
    """One styled run as Markdown.  `ref_ids` maps footnote anchor ids to
    Markdown footnote ids ([^id])."""
    if run.pagenum:
        text = run.text
        lead = text[: len(text) - len(text.lstrip())]
        trail = text[len(text.rstrip()):]
        return lead + "**" + _md_escape(text.strip()) + "**" + trail
    if run.fnref:
        rid = ref_ids.get(run.fnref)
        if rid:
            return f"[^{rid}]"
        return "<sup>" + _md_escape(run.text.strip()) + "</sup>"
    if run.fndef:
        return "**" + _md_escape(run.text.strip()) + "**"
    text = run.text
    core = text.strip()
    if not core or not (run.bold or run.italic or run.sup):
        return _md_escape(text)
    lead = text[: len(text) - len(text.lstrip())]
    trail = text[len(text.rstrip()):]
    core = _md_escape(core)
    if run.bold and run.italic:
        core = f"***{core}***"
    elif run.bold:
        core = f"**{core}**"
    elif run.italic:
        core = f"*{core}*"
    if run.sup:
        core = f"<sup>{core}</sup>"
    return lead + core + trail


def _md_paragraphs(
    paras: list[_ExpPara], ref_ids: Optional[dict[str, str]] = None
) -> list[str]:
    """Paragraphs as Markdown blocks (no trailing newlines)."""
    ref_ids = ref_ids or {}
    blocks: list[str] = []
    for p in paras:
        if p.kind == "fnhead":
            continue
        body = "".join(_md_run(r, ref_ids) for r in p.runs).strip()
        if not body:
            continue
        body = _md_block_guard(body)
        if p.kind == "blockquote":
            blocks.append("> " + body)
        elif p.kind == "heading":
            blocks.append("#### " + body)
        else:
            blocks.append(body)
    return blocks


def _copy_rich_clipboard(widget: tk.Misc, rtf: str, plain: str) -> str:
    """
    Put *rtf* on the system clipboard (with *plain* as fallback where the
    platform allows both).  Returns a short description of what was copied.
    """
    rtf_bytes = rtf.encode("ascii", "replace")
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            user32 = ctypes.windll.user32
            kernel32 = ctypes.windll.kernel32
            kernel32.GlobalAlloc.restype = ctypes.c_void_p
            kernel32.GlobalAlloc.argtypes = [wintypes.UINT, ctypes.c_size_t]
            kernel32.GlobalLock.restype = ctypes.c_void_p
            kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
            kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
            user32.OpenClipboard.argtypes = [ctypes.c_void_p]
            user32.SetClipboardData.restype = ctypes.c_void_p
            user32.SetClipboardData.argtypes = [wintypes.UINT, ctypes.c_void_p]

            CF_UNICODETEXT = 13
            GMEM_MOVEABLE = 0x0002
            cf_rtf = user32.RegisterClipboardFormatW("Rich Text Format")

            def set_data(fmt: int, data: bytes) -> None:
                handle = kernel32.GlobalAlloc(GMEM_MOVEABLE, len(data))
                ptr = kernel32.GlobalLock(handle)
                ctypes.memmove(ptr, data, len(data))
                kernel32.GlobalUnlock(handle)
                user32.SetClipboardData(fmt, handle)

            if not user32.OpenClipboard(None):
                raise OSError("OpenClipboard failed")
            try:
                user32.EmptyClipboard()
                set_data(cf_rtf, rtf_bytes + b"\x00")
                set_data(CF_UNICODETEXT, plain.encode("utf-16-le") + b"\x00\x00")
            finally:
                user32.CloseClipboard()
            return "formatted text (RTF)"
        except Exception as exc:
            print(f"[copy] Windows RTF clipboard failed: {exc}")
    elif sys.platform == "darwin":
        try:
            subprocess.run(
                ["pbcopy", "-Prefer", "rtf"], input=rtf_bytes, check=True, timeout=10
            )
            return "formatted text (RTF)"
        except Exception as exc:
            print(f"[copy] pbcopy RTF failed: {exc}")
    else:
        candidates = []
        if os.environ.get("WAYLAND_DISPLAY"):
            candidates.append(["wl-copy", "--type", "text/rtf"])
        candidates.append(["xclip", "-selection", "clipboard", "-t", "text/rtf"])
        for cmd in candidates:
            try:
                subprocess.run(cmd, input=rtf_bytes, check=True, timeout=10)
                return "formatted text (RTF)"
            except Exception as exc:
                print(f"[copy] {cmd[0]} RTF failed: {exc}")
    widget.clipboard_clear()
    widget.clipboard_append(plain)
    return "plain text (no RTF clipboard tool available)"


# Minimum word-shingle containment between the Scholar candidate and the
# CourtListener text to accept them as the same opinion.  Containment is
# stricter than an intuitive "percent similar": same-opinion pairs score
# well above this even across OCR/edition differences, while different
# opinions score near zero.
_SCHOLAR_MATCH_THRESHOLD = 0.60

# Opinion-text font size (pt).  Remembered across windows *and* app restarts on
# this computer (persisted to config), so a reader's A+/A− choice carries over
# to the next case they open — this run or a future one.
_OPINION_FONT_MIN = 7
_OPINION_FONT_MAX = 24


def _clamp_opinion_font_pt(pt) -> int:
    """Coerce a stored/parsed value to an int within the allowed reader range."""
    try:
        pt = int(pt)
    except (TypeError, ValueError):
        pt = 11
    return max(_OPINION_FONT_MIN, min(_OPINION_FONT_MAX, pt))


def _save_opinion_font_pt(pt: int) -> None:
    """Persist the reader text size so the next opinion/statute window — this
    run or a future one — opens at the size last chosen on this computer."""
    data = _load_config()
    if data.get("opinion_font_pt") != pt:
        data["opinion_font_pt"] = pt
        _save_config(data)


_OPINION_FONT_PT = _clamp_opinion_font_pt(
    _load_config().get("opinion_font_pt", 11))

# Side (details) panel text zoom — a point offset the user sets with the
# panel's own A−/A+ control.  Persisted to config so it carries across cases
# and app launches.  This sidesteps auto-DPI detection, which can't see a
# Retina Mac's true resolution (Tk reports logical points and 72 dpi there),
# leaving the panel small until the user nudges it.
_DETAILS_FONT_MIN_DELTA = -4
_DETAILS_FONT_MAX_DELTA = 16
_details_font_delta: Optional[int] = None


def _clamp_details_delta(delta: int) -> int:
    return max(_DETAILS_FONT_MIN_DELTA, min(_DETAILS_FONT_MAX_DELTA, delta))


def _get_details_font_delta() -> int:
    """The saved side-panel zoom offset (loaded once, then cached)."""
    global _details_font_delta
    if _details_font_delta is None:
        raw = _load_config().get("details_font_delta", 0)
        _details_font_delta = _clamp_details_delta(
            int(raw) if isinstance(raw, (int, float)) else 0)
    return _details_font_delta


def _set_details_font_delta(delta: int) -> None:
    """Update and persist the side-panel zoom offset."""
    global _details_font_delta
    _details_font_delta = _clamp_details_delta(delta)
    data = _load_config()
    data["details_font_delta"] = _details_font_delta
    _save_config(data)


def _screen_text_scale(widget: tk.Misc) -> float:
    """Conservative type nudge for dense/high-resolution displays.

    Tk point sizes normally follow OS scaling, but GUI apps launched outside a
    shell on macOS can still land with very small side-panel text on 4K/5K
    screens.  Keep the main reader user-controlled and scale only supporting
    panels that otherwise have fixed 9 pt labels.
    """
    scale = 1.0
    try:
        width = int(widget.winfo_screenwidth())
        height = int(widget.winfo_screenheight())
        long_side, short_side = max(width, height), min(width, height)
        if long_side >= 5000 or short_side >= 2800:
            scale = 1.45
        elif long_side >= 3800 or short_side >= 2100:
            scale = 1.33
        elif long_side >= 3000 or short_side >= 1700:
            scale = 1.22
        elif long_side >= 2500 or short_side >= 1400:
            scale = 1.12
    except Exception:
        pass
    try:
        dpi = float(widget.winfo_fpixels("1i"))
        if dpi >= 180:
            scale = max(scale, 1.22)
        elif dpi >= 135:
            scale = max(scale, 1.12)
    except Exception:
        pass
    return min(scale, 1.45)


def _scaled_panel_font_size(widget: tk.Misc, base_size: int) -> int:
    return max(base_size, min(base_size + 5,
                              int(round(base_size * _screen_text_scale(widget)))))


def _find_scholar_for_item(
    client: Optional[CourtListenerClient],
    fetcher: "GoogleScholarFetcher",
    item: dict,
    status,
) -> tuple[Optional[tuple[str, str]], Optional[str], str]:
    """
    Locate this CourtListener case's opinion on Google Scholar, verifying
    candidates against the CourtListener text before accepting them.

    Search stages, in order:
      1. the primary citation (walking down the results list),
      2. alternate reporter citations from the CourtListener cluster,
      3. the case name (with variants such as United States ↔ US).

    Returns (result, cl_text, note): *result* is (url, opinion_html) or
    None if no candidate was similar enough; *cl_text* is the assembled
    CourtListener text (so the viewer's toggle is instant); *note*
    describes the verification outcome.
    """
    cluster_id = item.get("cluster_id") or item.get("id")
    vkey = f"verified:cluster:{cluster_id}" if cluster_id else ""
    if vkey:
        cached = fetcher.get_cached(vkey)
        if cached:
            return cached, None, "verified match (cached)"

    cl_text: Optional[str] = None
    if client is not None and cluster_id:
        status("Fetching CourtListener text for comparison…")
        try:
            cl_text = _assemble_case_text(client, item)
        except Exception as exc:
            print(f"[verify] CourtListener text unavailable: {exc}")

    tried: set[str] = set()
    best_sim = 0.0

    def try_url(url: str) -> Optional[tuple[str, str]]:
        nonlocal best_sim
        if url in tried:
            return None
        tried.add(url)
        res = fetcher.fetch_by_url(url)
        if not res:
            return None
        if cl_text is None:
            return res  # nothing to verify against; accept the first hit
        sim = text_similarity(blocks_to_text(parse_opinion_blocks(res[1])), cl_text)
        print(f"[verify] similarity {sim:.2f} for {url}")
        best_sim = max(best_sim, sim)
        return res if sim >= _SCHOLAR_MATCH_THRESHOLD else None

    # --- assemble the search stages ---
    primary = _pick_citation(item.get("citation", []))
    alt_cites: list[str] = []
    raw = item.get("citation")
    if isinstance(raw, list):
        alt_cites += [c for c in raw if c and c != primary]
    if client is not None and cluster_id:
        try:
            rec = client.get_cluster(int(cluster_id), fields="citations")
            for c in _cluster_citations_to_strings(rec.get("citations")):
                if c != primary and c not in alt_cites:
                    alt_cites.append(c)
        except Exception as exc:
            print(f"[verify] cluster citations fetch failed: {exc}")
    alt_cites = [c for c in alt_cites if not _NOISE_CITE_RE.search(c)][:4]

    case_name = re.sub(
        r"<[^>]+>", "", item.get("caseName") or item.get("case_name") or ""
    ).strip()
    date_filed = item.get("dateFiled") or item.get("date_filed") or ""
    year = date_filed[:4] if len(date_filed) >= 4 else ""
    name_variants: list[str] = []
    if case_name:
        name_variants.append(case_name)
        v = re.sub(r"\bUnited States\b", "US", case_name)
        if v not in name_variants:
            name_variants.append(v)
        v = re.sub(r"\bU\.? ?S\.?\b", "United States", case_name)
        if v not in name_variants:
            name_variants.append(v)

    stages: list[tuple[str, int, str]] = []  # (query, results to try, description)
    if primary:
        stages.append((f'"{primary}"', 4, f"citation {primary}"))
    for c in alt_cites:
        stages.append((f'"{c}"', 2, f"alternate citation {c}"))
    for nm in name_variants:
        q = f"{nm} {year}".strip()
        stages.append((q, 3, f"case name {nm!r}"))

    fetches = 0
    _MAX_FETCHES = 10
    for q, take, desc in stages:
        if fetches >= _MAX_FETCHES:
            break
        status(f"Searching Scholar by {desc}…")
        results = fetcher.search_cases(q, limit=take)
        for r in results[:take]:
            if fetches >= _MAX_FETCHES:
                break
            fetches += 1
            status(f"Comparing candidate: {r.title[:60]}…")
            hit = try_url(r.url)
            if hit:
                if cl_text is not None and vkey:
                    fetcher.put_cached(vkey, *hit)
                note = (
                    "verified against CourtListener"
                    if cl_text is not None
                    else "unverified (no CourtListener text to compare)"
                )
                return hit, cl_text, note

    print(f"[verify] gave up; best similarity {best_sim:.2f}")
    return None, cl_text, f"best candidate similarity {best_sim:.0%}"


class _TextFinder:
    """Ctrl-F find bar for a Text widget: highlights every match, steps
    through them with Enter / Shift+Enter (also F3 / Shift+F3), and
    closes with Escape.  Case-insensitive plain-text search."""

    def __init__(self, win: tk.Misc, txt: tk.Text,
                 before_widget: tk.Misc) -> None:
        self._win, self._txt = win, txt
        self._before = before_widget
        self._visible = False
        self._matches: list[tuple[str, str]] = []
        self._cur = -1
        self._pending: Optional[str] = None  # debounce timer id

        txt.tag_configure("findmatch", background="#fff3b0")
        txt.tag_configure("findcur", background="#ffb347")
        bar = self._bar = ttk.Frame(win)
        ttk.Label(bar, text="Find:").pack(side="left", padx=(8, 4))
        self._var = tk.StringVar()
        self._entry = ttk.Entry(bar, textvariable=self._var, width=28)
        self._entry.pack(side="left")
        ttk.Button(bar, text="▼", width=2,
                   command=lambda: self.step(+1)).pack(side="left", padx=2)
        ttk.Button(bar, text="▲", width=2,
                   command=lambda: self.step(-1)).pack(side="left")
        self._count_var = tk.StringVar()
        ttk.Label(bar, textvariable=self._count_var,
                  foreground="gray").pack(side="left", padx=8)
        ttk.Button(bar, text="✕", width=2,
                   command=self.close).pack(side="right", padx=(0, 4))

        self._entry.bind("<Return>", lambda _e: self.step(+1))
        self._entry.bind("<Shift-Return>", lambda _e: self.step(-1))
        self._entry.bind("<KeyRelease>", self._on_key)
        self._entry.bind("<Escape>", lambda _e: self.close())
        win.bind("<Control-f>", lambda _e: self.open() or "break")
        win.bind("<F3>", lambda _e: self.step(+1))
        win.bind("<Shift-F3>", lambda _e: self.step(-1))
        win.bind("<Escape>", lambda _e: self.close() if self._visible
                 else None)

    def open(self) -> None:
        if not self._visible:
            # The find bar anchors above the text view.  When that view is hidden
            # — e.g. the opinion reader is showing its PDF instead, so the text
            # frame is pack_forget'd — there's nothing to find here, and packing
            # "before" an unpacked widget raises TclError.  Bail out quietly.
            try:
                if not self._before.winfo_ismapped():
                    return
                self._bar.pack(fill="x", padx=8, pady=(4, 0),
                               before=self._before)
            except tk.TclError:
                return
            self._visible = True
        self._entry.focus_set()
        self._entry.select_range(0, "end")
        if self._var.get():
            self.refresh()

    def close(self) -> None:
        if not self._visible:
            return
        self._bar.pack_forget()
        self._visible = False
        self._clear_tags()
        self._count_var.set("")
        self._txt.focus_set()

    def _clear_tags(self) -> None:
        self._txt.tag_remove("findmatch", "1.0", "end")
        self._txt.tag_remove("findcur", "1.0", "end")

    def _on_key(self, event) -> None:
        if event.keysym in ("Return", "Escape", "F3"):
            return
        if self._pending:
            self._win.after_cancel(self._pending)
        self._pending = self._win.after(250, self.refresh)

    def refresh(self) -> None:
        """Re-run the search (also called after the text is re-rendered)."""
        self._pending = None
        if not self._visible:
            return
        self._clear_tags()
        self._matches, self._cur = [], -1
        needle = self._var.get()
        if not needle:
            self._count_var.set("")
            return
        txt = self._txt
        idx = "1.0"
        n = tk.IntVar()
        while True:
            idx = txt.search(needle, idx, stopindex="end", nocase=True,
                             count=n)
            if not idx or not n.get():
                break
            end = f"{idx}+{n.get()}c"
            self._matches.append((idx, end))
            txt.tag_add("findmatch", idx, end)
            idx = end
        if self._matches:
            # start at the first match at or below the current view
            top = txt.index("@0,0")
            first = next(
                (i for i, (s, _e) in enumerate(self._matches)
                 if txt.compare(s, ">=", top)), 0,
            )
            self._goto(first)
        else:
            self._count_var.set("no matches")

    def step(self, delta: int) -> None:
        if not self._visible:
            self.open()
            return
        if not self._matches:
            self.refresh()
            return
        self._goto((self._cur + delta) % len(self._matches))

    def _goto(self, i: int) -> None:
        self._cur = i
        start, end = self._matches[i]
        txt = self._txt
        txt.tag_remove("findcur", "1.0", "end")
        txt.tag_add("findcur", start, end)
        txt.see(start)
        self._count_var.set(f"{i + 1} of {len(self._matches)}")


# ---------------------------------------------------------------------------
# Lossless PDF cropping (keeps the selectable text layer)
# ---------------------------------------------------------------------------
#
# The viewer can also export a *raster* crop (``_PdfPane.export_pdf``) which
# normalizes every page to one size with even margins — good for scans, but it
# rasterizes the text.  These helpers instead crop by tightening each page's
# MediaBox/CropBox to its content: a born-digital PDF keeps its real,
# selectable/searchable text because nothing is re-rendered — only the visible
# page boundary moves.  PDFium (via pypdfium2, already a dependency) both
# detects the content and writes the cropped file, so no new package is needed.


# PDFium (the C library behind pypdfium2) is NOT thread-safe — concurrent calls,
# even on *different* documents, can crash the interpreter.  The PDF pane renders
# on the main thread while the citation-linking worker (_detect_pdf_citation_links)
# reads text/char-boxes on a background thread, so every stretch of PDFium work is
# serialized through the one process-wide lock in ``pdfium_lock`` — shared with
# ``us_reports_pdf`` (opinion carving) and ``brief_reader`` (brief text), whose
# worker-thread PDFium calls used to run unserialized against the pane's render
# and crash the app when a case link was clicked from the PDF brief viewer.
_PDFIUM_LOCK = pdfium_lock.PDFIUM_LOCK
_PDF_HEADER_RE = re.compile(br"%PDF-\d")
_PDF_LINK_ATTR_RE = re.compile(
    r"""(?is)\b(?:href|src|data|data-url)=["']([^"']+)["']"""
)
_ABS_URL_RE = re.compile(r"""https?://[^\s"'<>]+""", re.IGNORECASE)


def _normalize_pdf_bytes(data: bytes) -> Optional[bytes]:
    """Return PDF bytes, trimming a small non-PDF prefix when present.

    Some court storage endpoints prepend whitespace, a byte-order mark, or a
    short transport artifact before the real ``%PDF-`` header.  Treat those as
    PDFs so they still open in the in-app viewer; otherwise return ``None``.
    """
    if not data:
        return None
    m = _PDF_HEADER_RE.search(data[:1024 * 1024])
    if not m:
        return None
    return data[m.start():] if m.start() else data


def _is_courtlistener_url(url: str) -> bool:
    host = (urllib.parse.urlparse(url or "").netloc or "").lower()
    return host == "courtlistener.com" or host.endswith(".courtlistener.com")


def _looks_like_pdf_url(url: str) -> bool:
    p = urllib.parse.urlparse(url or "")
    host = (p.netloc or "").lower()
    path_q = (p.path + "?" + p.query).lower()
    return (
        path_q.endswith(".pdf")
        or ".pdf?" in path_q
        or host == "storage.courtlistener.com"
        or (host.endswith(".courtlistener.com") and "/pdf" in path_q)
    )


def _pdf_link_candidates_from_html(data: bytes, base_url: str) -> list[str]:
    """PDF-looking links found in a non-PDF response body."""
    try:
        text = data[:300_000].decode("utf-8", errors="replace")
    except Exception:
        return []
    found: list[str] = []

    def add(raw: str) -> None:
        raw = _html.unescape((raw or "").strip())
        if not raw or raw.startswith(("javascript:", "mailto:", "#")):
            return
        url = urllib.parse.urljoin(base_url, raw)
        if _looks_like_pdf_url(url) and url not in found:
            found.append(url)

    for raw in _PDF_LINK_ATTR_RE.findall(text):
        add(raw)
    for raw in _ABS_URL_RE.findall(text):
        add(raw.rstrip(").,;"))
    return found


def _pdf_get(url: str, client=None, timeout: int = 30):
    """GET *url* using the CourtListener session only for CourtListener hosts."""
    session = (
        client._session
        if client is not None and _is_courtlistener_url(url)
        else _anon_session
    )
    headers = dict(_anon_session.headers)
    headers["Accept"] = "application/pdf,text/html,*/*;q=0.8"
    # storage.courtlistener.com can return PDFs with Content-Encoding: br when
    # br is advertised.  If the local requests stack lacks Brotli support, the
    # body remains compressed and no longer starts with %PDF.  Ask for the file
    # bytes as-is so sniffing and pdfium both see a real PDF.
    headers["Accept-Encoding"] = "identity"
    if session is not _anon_session:
        auth = getattr(session, "headers", {}).get("Authorization")
        if auth:
            headers["Authorization"] = auth
    return session.get(url, timeout=timeout, allow_redirects=True, headers=headers)


def _maybe_decode_pdf_response(data: bytes, encoding: str) -> bytes:
    """Best-effort decode when a server ignores our identity encoding request."""
    enc = (encoding or "").lower()
    if "br" not in enc or _normalize_pdf_bytes(data) is not None:
        return data
    for mod_name in ("brotli", "brotlicffi"):
        try:
            mod = __import__(mod_name)
            return mod.decompress(data)
        except Exception:
            continue
    return data


def _fetch_pdf_bytes(
    url: str,
    *,
    client=None,
    timeout: int = 30,
    max_hops: int = 3,
) -> "Optional[tuple[bytes, str]]":
    """Fetch *url* as a PDF, following a small HTML wrapper if necessary.
    file:// URLs (opinions extracted from local US Reports volumes) are read
    straight from disk."""
    queue = [url]
    seen: set[str] = set()
    while queue and len(seen) < max_hops:
        cur = queue.pop(0)
        if not cur or cur in seen:
            continue
        seen.add(cur)
        if cur.startswith("file:"):
            from urllib.request import url2pathname
            try:
                local = Path(url2pathname(urllib.parse.urlparse(cur).path))
                data = _normalize_pdf_bytes(local.read_bytes())
            except Exception as exc:
                print(f"[pdf] local file read failed ({exc}): {cur}")
                continue
            if data is not None:
                return data, cur
            continue
        resp = _pdf_get(cur, client=client, timeout=timeout)
        resp.raise_for_status()
        final_url = getattr(resp, "url", None) or cur
        content = _maybe_decode_pdf_response(
            resp.content, resp.headers.get("Content-Encoding", ""))
        data = _normalize_pdf_bytes(content)
        if data is not None:
            return data, final_url
        for nxt in _pdf_link_candidates_from_html(content, final_url):
            if nxt not in seen and nxt not in queue:
                queue.append(nxt)
    return None


def _page_has_scan_background(page) -> bool:
    """True when a page appears to be an OCR layer over a full-page scan."""
    import pypdfium2.raw as C

    try:
        text_objs = 0
        invisible_text_objs = 0
        image_objs = 0
        mb = tuple(page.get_mediabox())
        page_w, page_h = mb[2] - mb[0], mb[3] - mb[1]
        page_area = max(1.0, page_w * page_h)
        invisible_mode = getattr(C, "FPDF_TEXTRENDERMODE_INVISIBLE", 3)
        for obj in page.get_objects():
            try:
                obj_type = C.FPDFPageObj_GetType(obj.raw)
            except Exception:
                obj_type = None
            if obj_type == C.FPDF_PAGEOBJ_TEXT:
                text_objs += 1
                try:
                    if C.FPDFTextObj_GetTextRenderMode(obj.raw) == invisible_mode:
                        invisible_text_objs += 1
                except Exception:
                    pass
                continue
            if obj_type == C.FPDF_PAGEOBJ_IMAGE:
                image_objs += 1
            elif obj_type is not None:
                continue
            try:
                ol, ob, orr, ot = obj.get_pos()
            except Exception:
                continue
            ow, oh = orr - ol, ot - ob
            if ow <= 0 or oh <= 0:
                continue
            if (ow * oh >= 0.80 * page_area
                    and ow >= 0.70 * page_w
                    and oh >= 0.70 * page_h):
                return True
        if text_objs >= 10 and invisible_text_objs / text_objs >= 0.75:
            return True
        if image_objs and text_objs >= 10 and invisible_text_objs / text_objs >= 0.5:
            return True
    except Exception:
        return False
    return False


def _pdf_ocr_scan_pages(pdf_bytes: bytes) -> set[int]:
    """Page indexes whose text layer likely comes from OCR over a scan."""
    import pypdfium2 as pdfium

    pages: set[int] = set()
    with _PDFIUM_LOCK:
        doc = pdfium.PdfDocument(pdf_bytes)
        try:
            for i in range(len(doc)):
                page = doc[i]
                try:
                    if _page_has_scan_background(page):
                        pages.add(i)
                finally:
                    page.close()
        finally:
            doc.close()
    return pages


def _page_content_box_pts(page) -> Optional[tuple]:
    """The bounding box of a page's actual content in PDF points
    ``(left, bottom, right, top)``, or ``None`` when it can't be determined
    (e.g. a bare scanned image with no text or vector objects).

    Built from the page's own objects so it is exact for born-digital PDFs:
    the union of every glyph box (the selectable text) plus any image/vector
    object that does not span almost the whole page — a near-full-page image is
    treated as a scan background and left to the caller's raster fallback."""
    import ctypes
    import math
    import pypdfium2.raw as C

    l = b = math.inf
    r = t = -math.inf

    tp = None
    try:
        tp = page.get_textpage()
    except Exception:
        tp = None
    if tp is not None:
        try:
            for k in range(tp.count_chars()):
                cl, cr, cb, ct = (ctypes.c_double() for _ in range(4))
                if not C.FPDFText_GetCharBox(
                    tp.raw, k, ctypes.byref(cl), ctypes.byref(cr),
                    ctypes.byref(cb), ctypes.byref(ct),
                ):
                    continue
                if cr.value <= cl.value or ct.value <= cb.value:
                    continue  # empty / whitespace glyph box
                l, r = min(l, cl.value), max(r, cr.value)
                b, t = min(b, cb.value), max(t, ct.value)
        except Exception:
            pass
        finally:
            try:
                tp.close()
            except Exception:
                pass

    # Include drawn objects (images, vector graphics) so a figure isn't clipped,
    # but skip any single object covering ~the whole page — that is the scan
    # image itself, whose own margins are exactly what we want to crop away.
    try:
        mb = tuple(page.get_mediabox())
        page_area = max(1.0, (mb[2] - mb[0]) * (mb[3] - mb[1]))
        for obj in page.get_objects():
            try:
                ol, ob, orr, ot = obj.get_pos()
            except Exception:
                continue
            if (orr - ol) * (ot - ob) >= 0.9 * page_area:
                continue
            l, r = min(l, ol), max(r, orr)
            b, t = min(b, ob), max(t, ot)
    except Exception:
        pass

    if l == math.inf or r <= l or t <= b:
        return None
    return (l, b, r, t)


def _frac_box_to_points(frac: tuple, mb: tuple) -> Optional[tuple]:
    """Convert a viewer content fraction ``(l, t, r, b)`` (0..1, top-left
    origin — what ``_PdfPane._content_frac`` detects from a low-res render) into
    a PDF-point box ``(left, bottom, right, top)`` within media box *mb*.
    Returns ``None`` for the whole-page fraction (nothing to crop)."""
    fl, ft, fr, fb = frac
    if (fl, ft, fr, fb) == (0.0, 0.0, 1.0, 1.0):
        return None
    w, h = mb[2] - mb[0], mb[3] - mb[1]
    return (mb[0] + fl * w, mb[3] - fb * h, mb[0] + fr * w, mb[3] - ft * h)


def _crop_pdf_to_content(
    pdf_bytes: bytes,
    frac_boxes: Optional[list] = None,
    margin_pt: float = 7.0,
) -> bytes:
    """Return *pdf_bytes* cropped to each page's content by tightening the page
    boxes — a lossless crop that removes the wide blank borders while leaving
    the original (selectable) text untouched.

    Content is detected from the page objects (:func:`_page_content_box_pts`);
    for a page where that fails — a bare scan — the matching entry in
    *frac_boxes* (the viewer's ink-detected fraction) is used, so scanned pages
    crop too.  A small *margin_pt* is left around the content and the result is
    clamped to the original media box so a page is never enlarged."""
    import io

    import pypdfium2 as pdfium

    with _PDFIUM_LOCK:
        doc = pdfium.PdfDocument(pdf_bytes)
        try:
            for i in range(len(doc)):
                page = doc[i]
                try:
                    mb = tuple(page.get_mediabox())
                    box = _page_content_box_pts(page)
                    if box is None and frac_boxes and i < len(frac_boxes):
                        box = _frac_box_to_points(frac_boxes[i], mb)
                    if box is None:
                        continue  # leave this page at full size
                    l, b, r, t = box
                    l, b = max(mb[0], l - margin_pt), max(mb[1], b - margin_pt)
                    r, t = min(mb[2], r + margin_pt), min(mb[3], t + margin_pt)
                    if r - l < 1 or t - b < 1:
                        continue  # implausibly tight — skip rather than clip
                    page.set_mediabox(l, b, r, t)
                    page.set_cropbox(l, b, r, t)
                finally:
                    page.close()
            buf = io.BytesIO()
            doc.save(buf)
            return buf.getvalue()
        finally:
            doc.close()


# ---------------------------------------------------------------------------
# Citation detection over a PDF's text layer (for the "link citations" feature)
# ---------------------------------------------------------------------------


def _union_line_runs(boxes: list) -> list:
    """Group glyph boxes (in reading order, PDF points ``(l, b, r, t)``) into
    one union rectangle per line: a new run starts whenever a box no longer
    shares a vertical band with the current one (i.e. the citation wrapped to
    the next line).  So a cite split across lines yields one rectangle per line
    that the overlay can highlight separately."""
    runs: list = []
    cur: list = []
    for bx in boxes:
        if cur and (bx[3] <= cur[-1][1] or bx[1] >= cur[-1][3]):
            runs.append(cur)
            cur = [bx]
        else:
            cur.append(bx)
    if cur:
        runs.append(cur)
    out: list = []
    for run in runs:
        out.append((
            min(b[0] for b in run), min(b[1] for b in run),
            max(b[2] for b in run), max(b[3] for b in run),
        ))
    return out


def _extract_pdf_text_pages(pdf_bytes: bytes) -> list:
    """Extract a PDF's text layer as ``[[(char, box_or_None), …], …]`` — one
    list per page, each a parallel run of characters and their glyph boxes
    ``(left, bottom, right, top)`` in PDF points (``None`` for a char with no
    usable box, e.g. a line break).

    Runs on a background thread: it loads its *own* pdfium document (never the
    pane's), touches no tk objects, and returns plain data.  Every PDFium call is
    taken under :data:`_PDFIUM_LOCK`, per page, so it interleaves safely with the
    main thread's page rendering instead of racing it.  Shared by the citation
    linker and the find bar so the text is extracted only once."""
    import pypdfium2 as pdfium

    with _PDFIUM_LOCK:
        doc = pdfium.PdfDocument(pdf_bytes)
    try:
        pages: list = []
        try:
            with _PDFIUM_LOCK:
                n_pages = len(doc)
        except Exception:
            return []
        for pi in range(n_pages):
            chars: list = []
            with _PDFIUM_LOCK:
                page = doc[pi]
                try:
                    tp = page.get_textpage()
                    try:
                        n = tp.count_chars()
                        whole = tp.get_text_range()
                        aligned = len(whole) == n
                        for i in range(n):
                            ch = whole[i] if aligned else tp.get_text_range(i, 1)
                            if not ch:
                                ch = " "
                            try:
                                bx = tp.get_charbox(i)
                                if not (bx and bx[2] > bx[0] and bx[3] > bx[1]):
                                    bx = None
                            except Exception:
                                bx = None
                            chars.append((ch, bx))
                    finally:
                        tp.close()
                except Exception:
                    chars = []
                finally:
                    page.close()
            pages.append(chars)
        return pages
    finally:
        with _PDFIUM_LOCK:
            doc.close()


def _citation_links_from_pages(pages: list) -> dict:
    """Build the per-page clickable-citation rectangles from extracted page char
    data (see :func:`_extract_pdf_text_pages`).  Pure — no PDFium, no tk — so it
    is cheap to run on the worker right after extraction:

        {page_index: [(rect_pts, action, snippet), …]}

    Detection runs over the whole document at once (so "Id." and short forms
    resolve against citations anywhere in it); a per-character offset map ties
    each detected span back to its glyph boxes, and a citation that wraps across
    lines (or pages) becomes one rectangle per line."""
    parts: list = []               # global text pieces
    gmap: list = []                # global index -> (page, local) or (None, None)
    for pi, chars in enumerate(pages):
        for li, (ch, _bx) in enumerate(chars):
            parts.append(ch)
            gmap.append((pi, li))
        parts.append("\n")          # page separator (keeps words from fusing)
        gmap.append((None, None))
    text = "".join(parts)
    try:
        links = detect_brief_links(text)
    except Exception:
        return {}

    result: dict = {}
    for start, end, action in links:
        per_page: dict = {}
        for g in range(start, end):
            pi, li = gmap[g]
            if pi is None:
                continue
            bx = pages[pi][li][1]
            if bx is not None:
                per_page.setdefault(pi, []).append(bx)
        snippet = re.sub(r"\s+", " ", text[start:end]).strip()
        for pi, boxes in per_page.items():
            for rect in _union_line_runs(boxes):
                result.setdefault(pi, []).append((rect, action, snippet))
    return result


def _detect_pdf_citation_links(pdf_bytes: bytes) -> dict:
    """Convenience wrapper: extract a PDF's text and return its citation
    rectangles (see :func:`_citation_links_from_pages`)."""
    return _citation_links_from_pages(_extract_pdf_text_pages(pdf_bytes))


def _citation_links_from_visible_pdf_text(
    pdf_bytes: bytes, pages: list,
) -> "tuple[dict, set[int]]":
    """Citation rectangles for the whole document, plus the page indexes
    whose text layer is OCR over a scan.  OCR glyph boxes land imprecisely,
    so the pane renders those pages' links *quietly* — no tint, no
    recoloring — while keeping them clickable (see
    :meth:`_PdfPane.set_citation_links`)."""
    try:
        ocr_pages = _pdf_ocr_scan_pages(pdf_bytes)
    except Exception as exc:
        print(f"[pdf-links] OCR scan check failed: {exc}")
        ocr_pages = set()
    return _citation_links_from_pages(pages), ocr_pages


# ---------------------------------------------------------------------------
# Main-thread-only Tk image lifecycle.
#
# Tcl/Tk may only be touched from the thread running the Tcl interpreter —
# the main thread.  The panes below honor that (workers hand results to the
# main thread via ``after``), but Python's *cyclic* garbage collector runs
# finalizers on whichever thread happens to trigger a collection, and
# ``ImageTk.PhotoImage.__del__`` calls Tcl's ``image delete``.  So a page
# image that dies inside a reference cycle (a closed viewer window, say) can
# fire a Tcl call from a PDF-scanning worker thread.  That call blocks the
# worker — while it holds ``_PDFIUM_LOCK`` — until the main loop services it;
# if the main thread is itself waiting on ``_PDFIUM_LOCK`` to render a page,
# the app deadlocks, and the tangle ends in
# ``Fatal Python error: PyEval_RestoreThread``.
#
# Two rules keep it safe:
#   • page images are :func:`_make_safe_photo` instances, whose finalizer
#     never calls Tcl off the main thread — it just parks the Tcl image name
#     in :data:`_TK_IMAGE_GRAVEYARD` (a plain list append) for the main
#     thread to delete later;
#   • the pane frees images deterministically on the main thread (page
#     scrolled away, re-render, pane destroyed) via :func:`_dispose_photo`,
#     so images normally never reach the garbage collector at all.
# ``main()`` additionally disables automatic GC and collects on a main-thread
# timer, which protects every other Tcl-calling finalizer (``Variable``,
# ``font.Font``, …) the same way.
# ---------------------------------------------------------------------------

_TK_IMAGE_GRAVEYARD: list = []   # [(tkapp, image_name), …] pending `image delete`
_SAFE_PHOTO_CLS = None           # built lazily — PIL may not be installed


def _flush_tk_image_graveyard() -> None:
    """Delete the Tcl images parked by finalizers that ran off the main
    thread.  Main thread only; cheap when nothing is parked."""
    while _TK_IMAGE_GRAVEYARD:
        try:
            tkapp, name = _TK_IMAGE_GRAVEYARD.pop()
        except IndexError:
            break
        try:
            tkapp.call("image", "delete", name)
        except Exception:
            pass


def _make_safe_photo(img):
    """An ``ImageTk.PhotoImage`` of ``img`` whose finalizer is safe on any
    thread (see the lifecycle note above)."""
    global _SAFE_PHOTO_CLS
    if _SAFE_PHOTO_CLS is None:
        from PIL import ImageTk

        class _SafePhotoImage(ImageTk.PhotoImage):
            def __del__(self) -> None:
                if threading.current_thread() is threading.main_thread():
                    super().__del__()
                    return
                # Off the main thread (cyclic GC): no Tcl calls allowed.
                # Park the name for the main thread and disarm the inner
                # tkinter.PhotoImage's own Tcl-calling __del__.
                inner = getattr(self, "_PhotoImage__photo", None)
                name = getattr(inner, "name", None)
                tkapp = getattr(inner, "tk", None)
                if inner is not None:
                    inner.name = None
                if name and tkapp is not None:
                    _TK_IMAGE_GRAVEYARD.append((tkapp, name))

        _SAFE_PHOTO_CLS = _SafePhotoImage
    return _SAFE_PHOTO_CLS(img)


def _dispose_photo(photo) -> None:
    """Free a page PhotoImage now — main thread only — and disarm its
    finalizers, so whenever (and on whatever thread) the garbage collector
    eventually finds the husk there is nothing left to do."""
    if photo is None:
        return
    inner = getattr(photo, "_PhotoImage__photo", None)
    try:
        # PIL's PhotoImage.__del__ starts with `self.__photo.name`; with the
        # attribute gone it returns immediately, calling no Tcl.
        del photo._PhotoImage__photo
    except AttributeError:
        pass
    if inner is None:
        return
    name = getattr(inner, "name", None)
    inner.name = None               # tkinter.Image.__del__ → no-op
    if name:
        try:
            inner.tk.call("image", "delete", name)
        except Exception:
            pass


def _region_mostly_black(img, box, frac: float = 0.90) -> bool:
    """True when at least `frac` of the pixels inside `box` are near-black —
    the full-resolution confirmation that a small candidate really is a solid
    redaction rectangle and not a bold word (whose inter-letter white always
    drops it well below the bar)."""
    l, t, r, b = box
    area = (r - l) * (b - t)
    if area <= 0:
        return False
    hist = img.crop(box).convert("L").histogram()
    return sum(hist[:100]) >= frac * area


def _whiten_pdf_redactions(img, _ds: int = 4) -> list:
    """Erase the solid black redaction rectangles from a rendered page image
    (in place), returning the erased boxes as ``[(l, t, r, b), …]`` in image
    pixels.

    The static.case.law scans black out still-copyrighted West material
    (headnotes, syllabus, key numbers) with filled rectangles baked into the
    page bitmap; printed, they drink ink.  The image is box-averaged down 4×
    so solid black stays near 0 while text and halftones rise, and
    near-black blocks are grown into connected components.  Two shapes
    qualify: the big blocks (wide — ≥8% of the page — at least a text line
    tall, and ≥88% solid, which body text and headings never are), and the
    small one-line boxes West's key-number cites leave at paragraph heads —
    those are additionally verified at full resolution, where anything made
    of glyphs shows its white."""
    from PIL import Image
    W, H = img.size
    w, h = max(1, W // _ds), max(1, H // _ds)
    small = img.convert("L").resize((w, h), Image.BOX)
    black = [v <= 48 for v in small.tobytes()]
    seen = bytearray(w * h)
    min_w = max(6, int(0.08 * w))
    min_h = max(3, int(0.005 * h))
    max_key_h = max(4, int(0.035 * h))   # a key-cite box is ~one text line
    boxes: list[tuple[int, int, int, int]] = []
    for start in range(w * h):
        if not black[start] or seen[start]:
            continue
        seen[start] = 1
        stack = [start]
        n = 0
        lo_x = hi_x = start % w
        lo_y = hi_y = start // w
        while stack:
            p = stack.pop()
            n += 1
            x, y = p % w, p // w
            if x < lo_x:
                lo_x = x
            elif x > hi_x:
                hi_x = x
            if y < lo_y:
                lo_y = y
            elif y > hi_y:
                hi_y = y
            for q in (p - 1 if x else -1,
                      p + 1 if x < w - 1 else -1,
                      p - w, p + w if y < h - 1 else -1):
                if q >= 0 and black[q] and not seen[q]:
                    seen[q] = 1
                    stack.append(q)
        bw, bh = hi_x - lo_x + 1, hi_y - lo_y + 1
        keep = bw >= min_w and bh >= min_h and n >= 0.88 * bw * bh
        if (not keep and 2 <= bh <= max_key_h and 2 <= bw <= int(0.3 * w)
                and n >= 0.72 * bw * bh):
            keep = _region_mostly_black(
                img, (lo_x * _ds, lo_y * _ds,
                      (hi_x + 1) * _ds, (hi_y + 1) * _ds))
        if keep:
            boxes.append((lo_x, lo_y, hi_x, hi_y))
    fill = 255 if img.mode == "L" else (255,) * len(img.getbands())
    out: list = []
    for lo_x, lo_y, hi_x, hi_y in boxes:
        # One reduced pixel of outward pad erases the anti-aliased fringe.
        box = (max(0, (lo_x - 1) * _ds), max(0, (lo_y - 1) * _ds),
               min(W, (hi_x + 2) * _ds), min(H, (hi_y + 2) * _ds))
        img.paste(fill, box)
        out.append(box)
    return out


def _page_fully_redacted(text: str, boxes: list, size: tuple) -> bool:
    """True when redaction left the page with nothing worth printing:
    erased boxes covering a good share of it and a text layer holding no
    real words — just the page number, since case.law strips the text under
    its redactions.  That's a headnotes page of a fully-redacted West
    opinion (all boxes, "286" at the top); the print omits it entirely.
    Every content page keeps dozens of words, so the margin is wide."""
    if not boxes:
        return False
    W, H = size
    area = sum((r - l) * (b - t) for l, t, r, b in boxes)
    if area < 0.15 * W * H:
        return False
    return len(re.findall(r"[A-Za-z]{2,}", text or "")) < 3


class _PdfPane(ttk.Frame):
    """A scrollable, lazily-rendered view of a PDF, embedded in the opinion
    window (pypdfium2 + Pillow).

    Pages are rendered to images only as they scroll near the viewport, and
    pages that scroll far away are released again, so even a long opinion stays
    light on memory.  Construction raises ImportError when pypdfium2/Pillow are
    not installed — the caller then offers to open the PDF in a browser.
    """

    _PAD = 12        # vertical gap between pages (px)
    _SCROLL_PX = 60  # wheel-notch scroll distance (px); canvas uses 1px units
    _MARGIN = 18     # small even margin drawn around the cropped page (px)
    _BBOX_SCALE = 0.6   # low-res render scale used to detect the content box
    _INK_THRESH = 185   # grayscale < this counts as "ink" (ignores scan bg)
    _PROFILE_MIN = 2    # min avg ink (0-255) for a row/col to count as content
    _PAD_FRAC = 0.006   # tiny expansion of the detected box so glyphs aren't clipped
    _ZOOM_STEP = 1.25   # render-width multiplier per zoom notch
    _ZOOM_MIN_W = 240   # narrowest page render width (px)
    _ZOOM_MAX_W = 3200  # widest page render width (px)

    #: Glyph color used for citation links in ``link_style="recolor"`` mode —
    #: a light, readable blue standing in for the black ink.
    _RECOLOR_RGB = (47, 111, 214)

    def __init__(self, parent: tk.Misc, pdf_bytes: bytes, width: int = 800,
                 margin: Optional[int] = None,
                 link_style: str = "tint",
                 uniform_crop: bool = False) -> None:
        super().__init__(parent)
        self._disposed = False
        import pypdfium2 as pdfium
        from PIL import ImageTk  # noqa: F401  (availability check at construct)
        _flush_tk_image_graveyard()

        self._pdf_bytes = pdf_bytes   # kept for the lossless text-preserving export
        # How citation links are shown: "tint" draws translucent highlight
        # boxes (the brief viewer); "recolor" repaints the citation's own
        # glyphs light blue and adds no box at all — the slip-opinion viewer,
        # where highlights would be intrusive.  Clicks in recolor mode are
        # hit-tested at the canvas level, since there is no overlay item.
        self._link_style = link_style
        self._doc = pdfium.PdfDocument(pdf_bytes)
        # White margin drawn around each cropped page; US Reports scans get a
        # roomier margin (their official typography sits in a small block).
        self._margin = self._MARGIN if margin is None else max(0, int(margin))
        self._base_w = max(240, int(width))    # the fit-to-window width (zoom 1.0)
        self._target_w = self._base_w          # current render width (zoom applied)
        self._page_x = self._PAD               # left x of each page; centered later
        self._photos: dict[int, object] = {}   # page → PhotoImage (kept alive)
        self._img_ids: dict[int, int] = {}      # page → canvas image id
        # Optional citation-link overlay (set via set_citation_links): per-page
        # clickable rectangles drawn on top of the rendered pages.
        self._cite_links: dict[int, list] = {}  # page → [(rect_pts, action, snippet)]
        self._overlay_ids: dict[int, list] = {}  # page → [canvas rectangle ids]
        self._cite_on_left = None
        self._cite_on_right = None
        # Pages whose links are *quiet*: clickable but drawn with no tint and
        # no recoloring (OCR-over-scan text, whose glyph boxes are imprecise).
        self._quiet_pages: set[int] = set()
        # Optional text find (enabled via enable_find): per-page char data, the
        # current match list, and the page→highlight-ids map.
        self._search_pages: Optional[list] = None
        self._search_matches: list = []   # [(page_index, [rect_pts, …]), …]
        self._search_cur: int = -1
        self._search_ids: dict[int, list] = {}
        self._find_bar = None
        # Mouse text selection (enabled with the text layer, see enable_find):
        # the anchor and range are (page_index, char_index) pairs.
        self._sel_bound = False
        self._sel_anchor: Optional[tuple[int, int]] = None
        self._sel_range: Optional[tuple] = None
        self._sel_ids: dict[int, list] = {}

        # The canvas lives in a body frame so a find bar can sit above it.
        body = ttk.Frame(self)
        body.pack(side="top", fill="both", expand=True)
        self._body = body
        canvas = tk.Canvas(body, bg="#d9d9d9", highlightthickness=0,
                           yscrollincrement=1)
        vsb = ttk.Scrollbar(body, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=self._on_yview)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        self._canvas, self._vsb = canvas, vsb

        # Detect each page's content box once with a quick low-resolution render
        # (independent of zoom), so the wide blank margins of court PDFs are
        # cropped to a small, even margin.  The layout is (re)built from this
        # cached metadata whenever the window resizes or the zoom changes.
        self._meta: list[tuple] = []  # (w_pt, h_pt, frac_box)
        with _PDFIUM_LOCK:
            for i in range(len(self._doc)):
                page = self._doc[i]
                try:
                    w_pt, h_pt = page.get_size()
                    try:
                        lo = page.render(scale=self._BBOX_SCALE).to_pil()
                        frac = self._content_frac(lo)
                    except Exception:
                        frac = (0.0, 0.0, 1.0, 1.0)
                finally:
                    page.close()
                self._meta.append((w_pt, h_pt, frac))
        if uniform_crop:
            self._apply_uniform_crop()

        self._rect_ids: list[int] = []
        self._slots: list[tuple] = []  # (y, slot_h, frac_box, render_scale)
        self._content_h = self._PAD
        self._layout()

        canvas.bind("<Configure>", lambda _e: self._on_configure())
        canvas.bind("<MouseWheel>", self._on_wheel)            # Windows / macOS
        canvas.bind("<Button-4>", lambda _e: self._wheel(-1))  # X11 wheel up
        canvas.bind("<Button-5>", lambda _e: self._wheel(1))   # X11 wheel down
        # Ctrl + wheel zooms the page (matches the reader's Ctrl-wheel binding).
        canvas.bind("<Control-MouseWheel>",
                    lambda e: self.zoom(1 if e.delta > 0 else -1) or "break")
        canvas.bind("<Control-Button-4>", lambda _e: self.zoom(1) or "break")
        canvas.bind("<Control-Button-5>", lambda _e: self.zoom(-1) or "break")
        # Take keyboard focus only when the reader intentionally clicks in the
        # PDF.  Focusing on hover can make some platforms raise this window just
        # because the pointer crossed it.
        canvas.bind("<Button-1>", lambda _e: canvas.focus_set(), add="+")
        # Release the page images/document even when the toplevel dies at the
        # Tcl level (window-manager close), where Python-side destroy() never
        # cascades down to this frame.
        self.bind("<Destroy>", lambda _e: self._dispose(), add="+")
        self.after(60, self._render_visible)

    # ------------------------------------------------------------------
    # Layout / zoom
    # ------------------------------------------------------------------

    def zoom_percent(self) -> int:
        """Current zoom level as a whole-number percentage of fit-to-window."""
        return int(round(100 * self._target_w / self._base_w))

    def zoom(self, delta: int) -> None:
        """Zoom in (delta > 0), out (delta < 0), or reset to fit (delta == 0).
        Re-lays out from cached page metadata, so only the render width changes
        — the (expensive) content-box detection is not repeated."""
        if delta == 0:
            new_w = self._base_w
        else:
            factor = self._ZOOM_STEP if delta > 0 else 1 / self._ZOOM_STEP
            new_w = int(round(self._target_w * factor))
        new_w = max(self._ZOOM_MIN_W, min(self._ZOOM_MAX_W, new_w))
        if new_w == self._target_w:
            return
        try:
            top_frac = self._canvas.yview()[0]
        except tk.TclError:
            top_frac = 0.0
        self._target_w = new_w
        self._layout()
        try:
            self._canvas.yview_moveto(top_frac)  # keep the same place in the doc
        except tk.TclError:
            pass
        self._render_visible()

    def _page_left(self) -> int:
        """Left x for each page so it's centred when the canvas is wider than
        the page, and flush at the gutter otherwise."""
        try:
            view_w = self._canvas.winfo_width()
        except tk.TclError:
            view_w = 0
        full_w = self._target_w + 2 * self._PAD
        if view_w > full_w:
            return (view_w - self._target_w) // 2
        return self._PAD

    def _update_scrollregion(self) -> None:
        try:
            view_w = self._canvas.winfo_width()
        except tk.TclError:
            view_w = 0
        # Span the viewport when the page is narrower than it, so the centred
        # page stays put with no spurious horizontal scrolling.
        width = max(view_w, self._target_w + 2 * self._PAD)
        self._canvas.configure(scrollregion=(0, 0, width, self._content_h))

    def _layout(self) -> None:
        """(Re)build one white slot per page at the current zoom, centred."""
        c = self._canvas
        c.delete("all")
        self._rect_ids = []
        self._slots = []
        for photo in self._photos.values():
            _dispose_photo(photo)
        self._photos.clear()
        self._img_ids.clear()
        self._overlay_ids.clear()
        self._search_ids.clear()
        self._sel_ids.clear()
        self._inner_w = max(1, self._target_w - 2 * self._margin)
        self._page_x = self._page_left()
        y = self._PAD
        for (w_pt, h_pt, frac) in self._meta:
            fl, ft, fr, fb = frac
            cw_pt = max(1.0, (fr - fl) * w_pt)
            ch_pt = max(1.0, (fb - ft) * h_pt)
            render_scale = self._inner_w / cw_pt
            slot_h = int(round(ch_pt * render_scale)) + 2 * self._margin
            rid = c.create_rectangle(
                self._page_x, y, self._page_x + self._target_w, y + slot_h,
                fill="white", outline="#b8b8b8")
            self._rect_ids.append(rid)
            self._slots.append((y, slot_h, frac, render_scale))
            y += slot_h + self._PAD
        self._content_h = y
        self._update_scrollregion()

    def _on_configure(self) -> None:
        """Recentre on resize without a full re-render when the zoom is steady."""
        new_x = self._page_left()
        if new_x != self._page_x:
            dx = new_x - self._page_x
            self._page_x = new_x
            for rid in self._rect_ids:
                self._canvas.move(rid, dx, 0)
            for iid in self._img_ids.values():
                self._canvas.move(iid, dx, 0)
            for ids in self._overlay_ids.values():
                for oid in ids:
                    self._canvas.move(oid, dx, 0)
            for ids in self._search_ids.values():
                for oid in ids:
                    self._canvas.move(oid, dx, 0)
            for ids in self._sel_ids.values():
                for oid in ids:
                    self._canvas.move(oid, dx, 0)
        self._update_scrollregion()
        self._render_visible()

    def _content_frac(self, img) -> tuple:
        """Fractional content box (l, t, r, b in 0..1) of `img` — the area
        holding actual text/figures, found from row/column ink projections so
        scanner speckle in the margins doesn't defeat the crop.  Returns the
        full page when nothing plausible is found."""
        from PIL import Image
        full = (0.0, 0.0, 1.0, 1.0)
        W, H = img.size
        if W < 8 or H < 8:
            return full
        mask = img.convert("L").point(
            lambda p: 255 if p < self._INK_THRESH else 0)
        cols = mask.resize((W, 1), Image.BOX).tobytes()  # avg ink per column
        rows = mask.resize((1, H), Image.BOX).tobytes()  # avg ink per row

        def span(profile, n):
            idx = [k for k, v in enumerate(profile) if v > self._PROFILE_MIN]
            return (idx[0], idx[-1] + 1) if idx else (0, n)

        l, r = span(cols, W)
        t, b = span(rows, H)
        fl, ft = l / W - self._PAD_FRAC, t / H - self._PAD_FRAC
        fr, fb = r / W + self._PAD_FRAC, b / H + self._PAD_FRAC
        fl, ft = max(0.0, fl), max(0.0, ft)
        fr, fb = min(1.0, fr), min(1.0, fb)
        # Ignore implausible crops (blank page, or so tight it's likely noise).
        if (fr - fl) < 0.15 or (fb - ft) < 0.15:
            return full
        # Redacted case.law scans can contain full-height/full-width black bars
        # plus sparse text at the opposite edge.  Projection cropping may then
        # look confident while cutting off edge characters. Treat those one-side
        # crops as uncertain and leave the page whole.
        if (ft <= self._PAD_FRAC and fb >= 1.0 - self._PAD_FRAC
                and (fl > 0.03 or fr < 0.97)):
            return full
        if (fl <= self._PAD_FRAC and fr >= 1.0 - self._PAD_FRAC
                and (ft > 0.03 or fb < 0.97)):
            return full
        return (fl, ft, fr, fb)

    def _apply_uniform_crop(self) -> None:
        """Use one shared crop box for confident same-template opinion pages."""
        if len(self._meta) < 2:
            return
        widths = [m[0] for m in self._meta]
        heights = [m[1] for m in self._meta]
        max_w, max_h = max(widths), max(heights)
        if max_w <= 0 or max_h <= 0:
            return
        if ((max_w - min(widths)) / max_w > 0.03
                or (max_h - min(heights)) / max_h > 0.03):
            return
        full = (0.0, 0.0, 1.0, 1.0)
        boxes = [m[2] for m in self._meta if m[2] != full]
        if not boxes:
            return
        fl = max(0.0, min(b[0] for b in boxes))
        ft = max(0.0, min(b[1] for b in boxes))
        fr = min(1.0, max(b[2] for b in boxes))
        fb = min(1.0, max(b[3] for b in boxes))
        if (fr - fl) < 0.15 or (fb - ft) < 0.15:
            return
        common = (fl, ft, fr, fb)
        # A full-page box means detection was not confident for that page
        # (blank, noisy, or real content spanning the page). Do not force the
        # shared crop onto those pages; that can cut off text on case.law scans.
        self._meta = [
            (w_pt, h_pt, common if frac != full else full)
            for w_pt, h_pt, frac in self._meta
        ]

    def _on_yview(self, first: str, last: str) -> None:
        self._vsb.set(first, last)
        self._render_visible()

    def _on_wheel(self, e) -> None:
        self._wheel(-1 if e.delta > 0 else 1)

    def _wheel(self, direction: int) -> None:
        self._canvas.yview_scroll(direction * self._SCROLL_PX, "units")

    def _render_visible(self) -> None:
        _flush_tk_image_graveyard()
        c = self._canvas
        try:
            top = c.canvasy(0)
            view_h = c.winfo_height()
        except tk.TclError:
            return
        lo, hi = top - view_h, top + 2 * view_h   # ~one screen of buffer
        for i, (y, slot_h, _frac, _scale) in enumerate(self._slots):
            near = (y + slot_h) >= lo and y <= hi
            if near and i not in self._img_ids:
                self._render_page(i)
            elif not near and i in self._img_ids:
                c.delete(self._img_ids.pop(i))
                _dispose_photo(self._photos.pop(i, None))
                for oid in self._overlay_ids.pop(i, []):
                    c.delete(oid)
                for oid in self._search_ids.pop(i, []):
                    c.delete(oid)
                for oid in self._sel_ids.pop(i, []):
                    c.delete(oid)

    def _render_page(self, i: int) -> None:
        from PIL import Image
        y, slot_h, frac, scale = self._slots[i]
        with _PDFIUM_LOCK:
            page = self._doc[i]
            try:
                full = page.render(scale=scale).to_pil()
            finally:
                page.close()
        fl, ft, fr, fb = frac
        W, H = full.size
        content = full.crop((int(fl * W), int(ft * H),
                             int(round(fr * W)), int(round(fb * H))))
        if self._link_style == "recolor" and self._cite_links.get(i):
            content = self._recolor_citations(content, i, scale,
                                              int(fl * W), int(ft * H))
        # Snap to the exact content box so every page lines up with a uniform
        # margin, then mount it on a white page of the slot's size.
        inner_h = max(1, slot_h - 2 * self._margin)
        if content.size != (self._inner_w, inner_h):
            content = content.resize((self._inner_w, inner_h), Image.LANCZOS)
        canvas_img = Image.new("RGB", (self._target_w, slot_h), "white")
        canvas_img.paste(content, (self._margin, self._margin))
        photo = _make_safe_photo(canvas_img)
        self._photos[i] = photo
        self._img_ids[i] = self._canvas.create_image(
            self._page_x, y, anchor="nw", image=photo)
        # Search highlights and the text selection sit just above the page
        # image; citation overlays are drawn last so they stay clickable on
        # top of any overlapping highlight.
        self._draw_search(i)
        self._draw_selection(i)
        self._draw_overlays(i)

    # ------------------------------------------------------------------
    # Citation-link overlay
    # ------------------------------------------------------------------

    def set_citation_links(self, page_links: dict,
                           on_left=None, on_right=None,
                           quiet_pages: "Optional[set[int]]" = None) -> None:
        """Attach clickable citation overlays.  ``page_links`` maps a page index
        to ``[(rect_pts, action, snippet), …]`` (see
        :func:`_detect_pdf_citation_links`).  ``on_left(action, snippet)`` fires
        on a left click, ``on_right(action, snippet)`` on a right click.
        Links on a page in ``quiet_pages`` (OCR-over-scan text, whose glyph
        boxes are imprecise) are drawn with no tint or recoloring at all but
        stay clickable, dispatched at the canvas level.  Redraws the overlays
        on every page currently on screen (in recolor mode the pages
        themselves are re-rendered, since the link color is baked into the
        page image)."""
        self._cite_links = page_links or {}
        self._cite_on_left = on_left
        self._cite_on_right = on_right
        self._quiet_pages = set(quiet_pages or ())
        if self._link_style == "recolor":
            self._bind_recolor_events()
            c = self._canvas
            for i in list(self._img_ids):   # drop stale renders; re-render blue
                c.delete(self._img_ids.pop(i))
                _dispose_photo(self._photos.pop(i, None))
            self._render_visible()
            return
        if self._quiet_pages:
            self._bind_recolor_events()   # canvas-level clicks for quiet links
        for i in list(self._img_ids):
            self._draw_overlays(i)

    def _recolor_citations(self, content, i: int, scale: float,
                           crop_x: int, crop_y: int):
        """Repaint the ink of page *i*'s citation spans light blue on the
        cropped page image.  Only dark (glyph) pixels inside each citation
        rectangle are recolored, so the shapes stay crisp and the paper stays
        white — the text simply *is* blue, with no highlight box."""
        from PIL import Image
        if i in self._quiet_pages:
            return content   # OCR page: leave the scan untouched (quiet links)
        _w_pt, h_pt, _frac = self._meta[i]
        if content.mode != "RGB":
            content = content.convert("RGB")
        W, H = content.size
        blue = Image.new("RGB", (1, 1), self._RECOLOR_RGB)
        for rect, _action, _snippet in self._cite_links.get(i, ()):
            l, b, r, t = rect
            x0 = int(l * scale) - crop_x - 1
            x1 = int(round(r * scale)) - crop_x + 1
            y0 = int((h_pt - t) * scale) - crop_y - 1
            y1 = int(round((h_pt - b) * scale)) - crop_y + 1
            x0, y0 = max(0, x0), max(0, y0)
            x1, y1 = min(W, x1), min(H, y1)
            if x1 <= x0 or y1 <= y0:
                continue
            box = (x0, y0, x1, y1)
            region = content.crop(box)
            mask = region.convert("L").point(
                lambda p: 255 if p < 160 else 0)
            content.paste(blue.resize(region.size), box, mask)
        return content

    def _bind_recolor_events(self) -> None:
        """Canvas-level hover/click dispatch for recolor mode, where the links
        have no overlay items to bind to."""
        if getattr(self, "_recolor_bound", False):
            return
        self._recolor_bound = True
        c = self._canvas
        c.bind("<Motion>", self._on_recolor_motion, add="+")
        c.bind("<Button-1>", lambda e: self._on_recolor_click(e, left=True),
               add="+")
        c.bind("<Button-3>", lambda e: self._on_recolor_click(e, left=False),
               add="+")

    def _link_at(self, event) -> "Optional[tuple]":
        """The (action, snippet) under a mouse event, or None.  In tint mode
        only the *quiet* pages' links are canvas-dispatched — the tinted
        rectangles have their own item bindings."""
        try:
            x = self._canvas.canvasx(event.x)
            y = self._canvas.canvasy(event.y)
        except tk.TclError:
            return None
        for i in list(self._img_ids):
            if self._link_style != "recolor" and i not in self._quiet_pages:
                continue
            for rect, action, snippet in self._cite_links.get(i, ()):
                x0, y0, x1, y1 = self._rect_to_canvas(i, rect)
                if x0 - 1 <= x <= x1 + 1 and y0 - 1 <= y <= y1 + 1:
                    return action, snippet
        return None

    def _over_overlay_item(self) -> bool:
        """True when the pointer sits on a tinted overlay rectangle (whose
        own item bindings handle the cursor and the click)."""
        try:
            return "cite_ov" in self._canvas.gettags("current")
        except tk.TclError:
            return False

    def _on_recolor_motion(self, event) -> None:
        if self._over_overlay_item():
            return
        try:
            self._canvas.config(
                cursor="hand2" if self._link_at(event) else "")
        except tk.TclError:
            pass

    def _on_recolor_click(self, event, left: bool):
        if self._over_overlay_item():
            return None
        hit = self._link_at(event)
        if not hit:
            return None
        cb = self._cite_on_left if left else self._cite_on_right
        if cb is not None:
            cb(*hit)
        return "break"

    def scroll_to_page(self, i: int) -> None:
        """Scroll so page *i* starts at the top of the view."""
        if not (0 <= i < len(self._slots)) or self._content_h <= 0:
            return
        try:
            self._canvas.yview_moveto(
                max(0.0, (self._slots[i][0] - self._PAD) / self._content_h))
        except tk.TclError:
            pass
        self._render_visible()

    def _rect_to_canvas(self, i: int, rect: tuple) -> tuple:
        """Map a PDF-point box ``(l, b, r, t)`` on page *i* to canvas coordinates
        ``(x0, y0, x1, y1)`` using the same scale/crop the page image used."""
        y, _slot_h, frac, scale = self._slots[i]
        fl, ft, _fr, _fb = frac
        w_pt, h_pt, _ = self._meta[i]
        x_off = self._page_x + self._margin - scale * fl * w_pt
        y_off = y + self._margin + scale * h_pt * (1.0 - ft)
        l, b, r, t = rect
        return (x_off + scale * l, y_off - scale * t,
                x_off + scale * r, y_off - scale * b)

    def _draw_overlays(self, i: int) -> None:
        """(Re)draw the clickable citation rectangles for page *i* on top of its
        rendered image.  Recolor mode draws nothing — the links are painted
        into the page image and clicks are canvas-level."""
        if self._link_style == "recolor":
            return
        c = self._canvas
        for oid in self._overlay_ids.pop(i, []):
            c.delete(oid)
        links = self._cite_links.get(i)
        if not links or i >= len(self._slots):
            return
        if i in self._quiet_pages:
            return   # quiet page: no tint; clicks are canvas-dispatched
        ids: list = []
        for rect, action, snippet in links:
            x0, y0, x1, y1 = self._rect_to_canvas(i, rect)
            cat = _brief_action_category(action[0])
            oid = c.create_rectangle(
                x0, y0, x1, y1, width=0, tags=("cite_ov",),
                fill=_BRIEF_TINTS.get(cat, "#cfe2ff"), stipple="gray50",
                activefill=_BRIEF_TINTS.get(cat, "#cfe2ff"), activestipple="gray25",
            )
            c.tag_bind(oid, "<Enter>",
                       lambda _e: c.config(cursor="hand2"))
            c.tag_bind(oid, "<Leave>", lambda _e: c.config(cursor=""))
            c.tag_bind(oid, "<Button-1>",
                       lambda _e, a=action, s=snippet: self._fire_cite(
                           self._cite_on_left, a, s))
            c.tag_bind(oid, "<Button-3>",
                       lambda _e, a=action, s=snippet: self._fire_cite(
                           self._cite_on_right, a, s))
            ids.append(oid)
        self._overlay_ids[i] = ids

    @staticmethod
    def _fire_cite(cb, action, snippet):
        if cb is not None:
            cb(action, snippet)
        return "break"

    # ------------------------------------------------------------------
    # Text find (search the PDF's text layer; highlight matches on the page)
    # ------------------------------------------------------------------

    _SEARCH_FILL = "#fff15a"        # all matches: pale yellow
    _SEARCH_FILL_CUR = "#ff9632"    # current match: orange

    def enable_find(self, pages: list, bind_keys: bool = True) -> None:
        """Turn on the text layer using ``pages`` (the extracted per-page
        char data from :func:`_extract_pdf_text_pages`): Ctrl-F text search
        and mouse text selection (drag to select, Ctrl-C to copy).  A no-op
        when ``pages`` is empty (a scan with no text layer).

        ``bind_keys=False`` skips binding Ctrl-F/F3 on the containing window
        — for panes embedded in a window whose find keys already belong to
        its text view (the opinion reader) — while selection, which binds
        only on the pane's own canvas, stays on."""
        if not pages:
            return
        self._search_pages = pages
        self._enable_selection()
        if not bind_keys:
            return
        top = self.winfo_toplevel()
        top.bind("<Control-f>", lambda _e: self._open_find())
        top.bind("<F3>", lambda _e: self._find_step(1))
        top.bind("<Shift-F3>", lambda _e: self._find_step(-1))

    def has_find(self) -> bool:
        return bool(self._search_pages)

    def _open_find(self) -> str:
        if not self._search_pages:
            return "break"
        if self._find_bar is None:
            bar = ttk.Frame(self, padding=(6, 3))
            ttk.Label(bar, text="Find:").pack(side="left")
            self._find_var = tk.StringVar()
            ent = ttk.Entry(bar, textvariable=self._find_var, width=30)
            ent.pack(side="left", padx=4)
            ent.bind("<Return>", lambda _e: self._find_step(1))
            ent.bind("<Shift-Return>", lambda _e: self._find_step(-1))
            ent.bind("<Escape>", lambda _e: self._close_find())
            ent.bind("<KeyRelease>", self._on_find_key)
            self._find_entry = ent
            ttk.Button(bar, text="▲", width=2,
                       command=lambda: self._find_step(-1)).pack(side="left")
            ttk.Button(bar, text="▼", width=2,
                       command=lambda: self._find_step(1)).pack(side="left",
                                                                padx=(2, 6))
            self._find_count = tk.StringVar(value="")
            ttk.Label(bar, textvariable=self._find_count,
                      foreground="gray").pack(side="left")
            ttk.Button(bar, text="✕", width=2,
                       command=self._close_find).pack(side="right")
            self._find_bar = bar
        self._find_bar.pack(side="top", fill="x", before=self._body)
        self._find_entry.focus_set()
        self._find_entry.selection_range(0, "end")
        return "break"

    def _close_find(self) -> str:
        self._search_matches = []
        self._search_cur = -1
        self._redraw_search()
        if self._find_bar is not None:
            self._find_bar.pack_forget()
        try:
            self._canvas.focus_set()
        except tk.TclError:
            pass
        return "break"

    def _on_find_key(self, event: tk.Event) -> None:
        if event.keysym in ("Return", "Escape", "Up", "Down",
                             "Shift_L", "Shift_R"):
            return
        self._run_find()

    def _run_find(self) -> None:
        query = self._find_var.get() if self._find_bar is not None else ""
        self._search_matches = self._compute_matches(query)
        self._search_cur = 0 if self._search_matches else -1
        self._update_find_count()
        self._redraw_search()
        if self._search_cur >= 0:
            self._scroll_to_match(self._search_cur)

    def _compute_matches(self, query: str) -> list:
        """Whitespace-flexible, case-insensitive search of every page's text;
        returns ``[(page_index, [rect_pts, …]), …]`` in document order.  A match
        that wraps across a line becomes one rectangle per line."""
        qn = re.sub(r"\s+", " ", query or "").strip().lower()
        if not qn or not self._search_pages:
            return []
        matches: list = []
        for pi, chars in enumerate(self._search_pages):
            norm: list = []        # normalized chars
            idxmap: list = []      # normalized index -> original char index
            prev_ws = False
            for li, (ch, _bx) in enumerate(chars):
                if ch and ch.isspace():
                    if not prev_ws:
                        norm.append(" ")
                        idxmap.append(li)
                    prev_ws = True
                elif ch:
                    norm.append(ch)
                    idxmap.append(li)
                    prev_ws = False
            ntext = "".join(norm).lower()
            start = 0
            while True:
                k = ntext.find(qn, start)
                if k < 0:
                    break
                boxes = [chars[idxmap[j]][1] for j in range(k, k + len(qn))
                         if chars[idxmap[j]][1] is not None]
                rects = _union_line_runs(boxes) if boxes else []
                if rects:
                    matches.append((pi, rects))
                start = k + len(qn)
        return matches

    def _draw_search(self, i: int) -> None:
        """(Re)draw the search-match highlights for page *i*; the current match
        is drawn in a stronger colour.  Citation overlays are raised back on top
        so they stay clickable."""
        c = self._canvas
        for oid in self._search_ids.pop(i, []):
            c.delete(oid)
        if not self._search_matches or i >= len(self._slots):
            return
        ids: list = []
        for mi, (pi, rects) in enumerate(self._search_matches):
            if pi != i:
                continue
            fill = self._SEARCH_FILL_CUR if mi == self._search_cur \
                else self._SEARCH_FILL
            for rect in rects:
                x0, y0, x1, y1 = self._rect_to_canvas(i, rect)
                ids.append(c.create_rectangle(
                    x0, y0, x1, y1, width=0, fill=fill, stipple="gray50"))
        self._search_ids[i] = ids
        c.tag_raise("cite_ov")

    def _redraw_search(self) -> None:
        for i in list(self._img_ids):
            self._draw_search(i)

    def _update_find_count(self) -> None:
        if not hasattr(self, "_find_count"):
            return
        n = len(self._search_matches)
        if not n:
            q = self._find_var.get().strip() if self._find_bar is not None else ""
            self._find_count.set("No matches" if q else "")
        else:
            self._find_count.set(f"{self._search_cur + 1} of {n}")

    def _find_step(self, direction: int) -> str:
        if not self._search_matches:
            return "break"
        self._search_cur = (self._search_cur + direction) % len(self._search_matches)
        self._update_find_count()
        self._scroll_to_match(self._search_cur)
        self._redraw_search()
        return "break"

    def _scroll_to_match(self, mi: int) -> None:
        """Scroll the current match into view (about a third down the viewport),
        then render and re-highlight the now-visible pages."""
        if not (0 <= mi < len(self._search_matches)):
            return
        pi, rects = self._search_matches[mi]
        if pi >= len(self._slots) or not rects:
            return
        _x0, y0, _x1, _y1 = self._rect_to_canvas(pi, rects[0])
        try:
            view_h = self._canvas.winfo_height() or 1
        except tk.TclError:
            return
        target = max(0.0, y0 - view_h / 3.0)
        self._canvas.yview_moveto(min(1.0, target / max(1, self._content_h)))
        self._render_visible()
        self._redraw_search()

    # ------------------------------------------------------------------
    # Text selection (drag over the page to select; Ctrl-C copies) — uses
    # the same extracted char boxes as the find bar, so it's available
    # whenever the PDF has a text layer.
    # ------------------------------------------------------------------

    _SEL_FILL = "#4a90e2"   # translucent (stippled) selection blue

    def _enable_selection(self) -> None:
        if self._sel_bound:
            return
        self._sel_bound = True
        c = self._canvas
        c.bind("<Button-1>", self._on_sel_press, add="+")
        c.bind("<B1-Motion>", self._on_sel_drag, add="+")
        c.bind("<Control-c>", self._copy_selection, add="+")

    def _on_sel_press(self, event) -> None:
        # Take keyboard focus so Ctrl-C reaches the canvas binding.
        try:
            self._canvas.focus_set()
        except tk.TclError:
            pass
        if self._sel_range is not None:
            self._sel_range = None
            self._redraw_selection()
        # A press on a citation link is a click, not a selection start.
        self._sel_anchor = None if self._link_at(event) else self._char_at(event)

    def _on_sel_drag(self, event) -> None:
        if self._sel_anchor is None:
            return
        cur = self._char_at(event)
        if cur is None:
            return
        a, b = self._sel_anchor, cur
        self._sel_range = (a, b) if a <= b else (b, a)
        self._redraw_selection()

    def _char_at(self, event) -> Optional[tuple[int, int]]:
        """The (page index, char index) nearest a mouse event — same line
        preferred, then nearest horizontally — or None without a text layer."""
        pages = self._search_pages
        if not pages or not self._slots:
            return None
        try:
            x = self._canvas.canvasx(event.x)
            y = self._canvas.canvasy(event.y)
        except tk.TclError:
            return None
        pi, best_d = 0, None
        for i, (sy, slot_h, _frac, _scale) in enumerate(self._slots):
            if i >= len(pages):
                break
            if sy <= y <= sy + slot_h:
                pi = i
                break
            d = min(abs(y - sy), abs(y - (sy + slot_h)))
            if best_d is None or d < best_d:
                pi, best_d = i, d
        if pi >= len(pages):
            return None
        # Canvas point -> PDF points on page `pi` (inverse of _rect_to_canvas)
        sy, _slot_h, frac, scale = self._slots[pi]
        fl, ft, _fr, _fb = frac
        w_pt, h_pt, _ = self._meta[pi]
        x_off = self._page_x + self._margin - scale * fl * w_pt
        y_off = sy + self._margin + scale * h_pt * (1.0 - ft)
        x_pt = (x - x_off) / scale
        y_pt = (y_off - y) / scale
        best = None
        best_key = None
        for idx, (ch, box) in enumerate(pages[pi]):
            if box is None or (ch and ch in "\r\n"):
                continue
            l, b, r, t = box
            dy = 0.0 if b <= y_pt <= t else min(abs(y_pt - b), abs(y_pt - t))
            dx = 0.0 if l <= x_pt <= r else min(abs(x_pt - l), abs(x_pt - r))
            key = (dy, dx)
            if best_key is None or key < best_key:
                best_key, best = key, (pi, idx)
        return best

    def _draw_selection(self, i: int) -> None:
        c = self._canvas
        for oid in self._sel_ids.pop(i, []):
            c.delete(oid)
        rng = self._sel_range
        if rng is None or i >= len(self._slots) or not self._search_pages:
            return
        (p0, i0), (p1, i1) = rng
        if not (p0 <= i <= p1) or i >= len(self._search_pages):
            return
        chars = self._search_pages[i]
        lo = i0 if i == p0 else 0
        hi = i1 if i == p1 else len(chars) - 1
        boxes = [b for ch, b in chars[lo:hi + 1]
                 if b is not None and (not ch or ch not in "\r\n")]
        ids: list = []
        for rect in _union_line_runs(boxes):
            x0, y0, x1, y1 = self._rect_to_canvas(i, rect)
            ids.append(c.create_rectangle(
                x0, y0, x1, y1, width=0, fill=self._SEL_FILL,
                stipple="gray50"))
        if ids:
            self._sel_ids[i] = ids
        c.tag_raise("cite_ov")

    def _redraw_selection(self) -> None:
        for i in list(self._img_ids):
            self._draw_selection(i)

    def selected_text(self) -> str:
        """The currently selected text, page breaks as newlines; "" without
        a selection."""
        rng = self._sel_range
        if rng is None or not self._search_pages:
            return ""
        (p0, i0), (p1, i1) = rng
        parts: list[str] = []
        for pi in range(p0, min(p1, len(self._search_pages) - 1) + 1):
            chars = self._search_pages[pi]
            lo = i0 if pi == p0 else 0
            hi = i1 if pi == p1 else len(chars) - 1
            parts.append("".join(ch for ch, _b in chars[lo:hi + 1] if ch))
        text = "\n".join(parts).replace("\r\n", "\n").replace("\r", "\n")
        return text

    def _copy_selection(self, _event=None):
        text = self.selected_text()
        if not text.strip():
            return None   # nothing selected: let the window's Ctrl-C run
        try:
            self.clipboard_clear()
            self.clipboard_append(text)
        except tk.TclError:
            return None
        return "break"

    def export_pdf(self, path: str, dpi: int = 150,
                   whiten_redactions: bool = False,
                   header_cite: str = "") -> int:
        """Write a PDF that matches what's shown — each page cropped to its
        content box and re-centered on a clean white page with the viewer's
        uniform margin — rather than the original scan with its wide, uneven
        borders.  Rendered as images at `dpi` (text becomes raster).

        ``whiten_redactions=True`` additionally erases the solid black
        redaction rectangles the case.law scans carry (see
        :func:`_whiten_pdf_redactions`) — for printing, where they'd waste
        ink.  ``header_cite`` is then drawn centered on each page's whitened
        running-head line (whose redaction took the reporter citation with
        it), at one size for the whole document: the erased bars vary a
        little page to page, so each page's line is measured and every page
        gets the largest fit any of them would choose.  A page the redaction
        left with nothing but boxes and its page number (see
        :func:`_page_fully_redacted`) is omitted from the output altogether.
        Returns how many rectangles were erased (0 without the flag)."""
        from PIL import Image
        scale = dpi / 72.0
        # Keep the same margin-to-content proportion the viewer displays.
        margin_ratio = self._margin / max(1, self._inner_w)
        whitened = 0
        omitted = 0
        pages: list = []
        try:
            # Pass 1 — render and whiten, keeping each page's header line.
            rendered: list = []   # [(full image, frac box, header box), …]
            for i, (w_pt, h_pt, frac) in enumerate(self._meta):
                with _PDFIUM_LOCK:
                    page = self._doc[i]
                    try:
                        full = page.render(scale=scale).to_pil().convert("RGB")
                        page_text = ""
                        if whiten_redactions:
                            try:
                                tp = page.get_textpage()
                                try:
                                    page_text = tp.get_text_range()
                                finally:
                                    tp.close()
                            except Exception:
                                page_text = ""
                    finally:
                        page.close()
                hbox = None
                if whiten_redactions:
                    boxes = _whiten_pdf_redactions(full)
                    whitened += len(boxes)
                    if _page_fully_redacted(page_text, boxes, full.size):
                        omitted += 1
                        continue
                    if header_cite:
                        hbox = _header_redaction_box(boxes, full.size)
                rendered.append((full, frac, hbox))
            # One size fits all: the largest (fullest text on ties) any
            # page's own line would choose.
            if header_cite:
                fits = [_header_citation_fit(header_cite, hbox)
                        for _full, _frac, hbox in rendered if hbox]
                if fits:
                    text, size = max(fits, key=lambda fs: (fs[1], len(fs[0])))
                    font = _print_header_font(size)
                    for full, _frac, hbox in rendered:
                        if hbox:
                            _draw_header_citation(full, hbox, text, font)
            # Pass 2 — crop each page to its content box and mount it.
            for idx in range(len(rendered)):
                full, frac, _hbox = rendered[idx]
                rendered[idx] = None   # free the full render as we go
                fl, ft, fr, fb = frac
                W, H = full.size
                content = full.crop((int(fl * W), int(ft * H),
                                     int(round(fr * W)), int(round(fb * H))))
                m = max(1, int(round(content.width * margin_ratio)))
                sheet = Image.new(
                    "RGB", (content.width + 2 * m, content.height + 2 * m),
                    "white")
                sheet.paste(content, (m, m))
                pages.append(sheet)
            if not pages:
                raise ValueError("the PDF has no pages")
            pages[0].save(path, "PDF", resolution=float(dpi),
                          save_all=True, append_images=pages[1:])
            if whitened:
                print(f"[pdf] whitened {whitened} redaction boxes for printing")
            if omitted:
                print(f"[pdf] omitted {omitted} fully-redacted page(s)")
            return whitened
        finally:
            for p in pages:
                try:
                    p.close()
                except Exception:
                    pass

    def has_text_layer(self) -> bool:
        """True when the PDF carries a real selectable text layer (born-digital,
        not a bare scan) — the case where a lossless, text-preserving crop is
        worth doing instead of the raster export."""
        any_text = False
        try:
            with _PDFIUM_LOCK:
                for i in range(len(self._doc)):
                    page = self._doc[i]
                    try:
                        if _page_has_scan_background(page):
                            return False
                        tp = page.get_textpage()
                        try:
                            if tp.count_chars() > 0:
                                any_text = True
                        finally:
                            tp.close()
                    finally:
                        page.close()
        except Exception:
            return False
        return any_text

    def export_cropped_pdf(self, path: str) -> None:
        """Write a PDF cropped to each page's content with the selectable text
        layer preserved (see :func:`_crop_pdf_to_content`).  The viewer's
        ink-detected boxes are passed as the per-page fallback so scanned pages
        still crop."""
        out = _crop_pdf_to_content(
            self._pdf_bytes, frac_boxes=[m[2] for m in self._meta]
        )
        with open(path, "wb") as fh:
            fh.write(out)

    def export_best(self, path: str) -> None:
        """Save the cropped PDF the user expects: a lossless crop that keeps the
        text selectable when the source has a text layer, otherwise the raster
        crop (which normalizes a scan's page sizes and margins).  Either way the
        wide blank borders are removed."""
        if self.has_text_layer():
            try:
                self.export_cropped_pdf(path)
                return
            except Exception as exc:
                print(f"[pdf] lossless crop failed ({exc}); using raster export")
        self.export_pdf(path)

    def _dispose(self) -> None:
        """Free the page images and the PDFium document.  Idempotent; main
        thread only.  Runs from destroy() and from the <Destroy> binding, so
        the images are released deterministically instead of waiting — full
        of live Tcl handles — for the garbage collector (see the Tk-image
        lifecycle note above)."""
        if getattr(self, "_disposed", True):
            return
        self._disposed = True
        photos = getattr(self, "_photos", None)  # absent if __init__ raised
        if photos:
            for photo in photos.values():
                _dispose_photo(photo)
            photos.clear()
        _flush_tk_image_graveyard()
        # Under the PDFium lock: closing the document is a PDFium call and may
        # overlap the citation-scan worker (which holds its own document) if the
        # window is closed mid-scan.
        doc = getattr(self, "_doc", None)
        if doc is not None:
            try:
                with _PDFIUM_LOCK:
                    doc.close()
            except Exception:
                pass

    def destroy(self) -> None:
        self._dispose()
        super().destroy()


# US Reports scans (LOC CDN "usrepNNN…" and GovInfo "USREPORTS-…") have a small
# type block centred on a large page; a roomier margin reads better than the
# default.  Detected from the resolved PDF URL.
def _is_us_reports_pdf(url: str) -> bool:
    u = (url or "").lower()
    return "usrep" in u or "usreports-" in u


# The Harvard CAP scans on static.case.law redact still-copyrighted West
# material with big solid black rectangles baked into the page images; the
# print path whitens those (see _whiten_pdf_redactions).  Detected from the
# resolved PDF URL so official court/RECAP/CommonLII scans are never touched.
def _is_redacted_case_pdf(url: "Optional[str]") -> bool:
    return "case.law" in (url or "").lower()


# ---------------------------------------------------------------------------
# Printed running head for redacted case.law scans
# ---------------------------------------------------------------------------
# The header redaction takes the reporter's running head — and with it the
# citation — off every page, so the print export re-letters that line with
# the Bluebook citation for *this* reporter.  The pieces come from the PDF
# itself (its first-page caption prints the parties, court, and decision
# date), from the static.case.law URL (which encodes reporter, volume, and
# first page), and from whatever richer metadata the calling window has.

_CASE_LAW_URL_RE = re.compile(
    r"static\.case\.law/([^/]+)/(\d+)/case-pdfs/0*(\d+)-\d+\.pdf", re.I)

#: Reverse of _slugify_reporter for the reporters case.law hosts — federal,
#: regional, and the states' official series — so a bare URL still yields a
#: citation when neither the metadata JSON nor any caller supplied one.
_CASE_LAW_SLUG_REPORTERS = {_slugify_reporter(_r): _r for _r in (
    # Federal
    "U.S.", "S. Ct.", "L. Ed.", "L. Ed. 2d", "F.", "F.2d", "F.3d", "F.4th",
    "F. App'x", "F. Supp.", "F. Supp. 2d", "F. Supp. 3d", "F.R.D.",
    "Fed. Cl.", "B.R.", "U.S. App. D.C.",
    # Regional
    "A.", "A.2d", "A.3d", "N.E.", "N.E.2d", "N.E.3d", "N.W.", "N.W.2d",
    "P.", "P.2d", "P.3d", "S.E.", "S.E.2d", "S.W.", "S.W.2d", "S.W.3d",
    "So.", "So. 2d", "So. 3d",
    # State-focused West series
    "Cal. Rptr.", "Cal. Rptr. 2d", "Cal. Rptr. 3d", "N.Y.S.", "N.Y.S.2d",
    "N.Y.S.3d", "Ill. Dec.",
    # State official reporters
    "Ala.", "Alaska", "Ariz.", "Ariz. App.", "Ark.", "Cal.", "Cal. 2d",
    "Cal. 3d", "Cal. 4th", "Cal. 5th", "Cal. App.", "Cal. App. 2d",
    "Cal. App. 3d", "Cal. App. 4th", "Cal. App. 5th", "Colo.", "Conn.",
    "Conn. App.", "D.C.", "Del.", "Del. Ch.", "Fla.", "Ga.", "Ga. App.",
    "Haw.", "Idaho", "Ill.", "Ill. 2d", "Ill. App.", "Ill. App. 2d",
    "Ill. App. 3d", "Ind.", "Ind. App.", "Iowa", "Kan.", "Kan. App. 2d",
    "Ky.", "La.", "Mass.", "Mass. App. Ct.", "Md.", "Md. App.", "Me.",
    "Mich.", "Mich. App.", "Minn.", "Miss.", "Mo.", "Mont.", "N.C.",
    "N.C. App.", "N.D.", "N.H.", "N.J.", "N.J. Super.", "N.M.", "N.Y.",
    "N.Y.2d", "N.Y.3d", "A.D.", "A.D.2d", "A.D.3d", "Misc.", "Misc. 2d",
    "Misc. 3d", "Neb.", "Nev.", "Ohio St.", "Ohio St. 2d", "Ohio St. 3d",
    "Ohio App.", "Okla.", "Okla. Crim.", "Or.", "Or. App.", "Pa.",
    "Pa. Super.", "Pa. Commw.", "R.I.", "S.C.", "S.D.", "Tenn.", "Tex.",
    "Tex. Crim.", "Utah", "Utah 2d", "Va.", "Va. App.", "Vt.", "W. Va.",
    "Wash.", "Wash. 2d", "Wash. App.", "Wis.", "Wis. 2d", "Wyo.")}

_ANY_CITE_RE = re.compile(
    r"\b(\d{1,4})\s+([A-Z][A-Za-z0-9.'’ ]{0,24}?)\s+(\d{1,5})\b")
_CAPTION_DATE_RE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)[a-z]*\.?\s+"
    r"\d{1,2},\s+(\d{4})")
_CAPTION_BREAK_RE = re.compile(
    r"^(?:ORDER|OPINION|PER CURIAM|Syllabus|Background|West Headnotes)\b",
    re.I)
_ROLE_LABEL_RE = re.compile(
    r",?\s*(?:Cross-)?(?:Appell(?:ant|ee)|Plaintiff|Defendant|Petitioner|"
    r"Respondent|Relator|Intervenor)s?"
    r"(?:\s*[-–—]\s*(?:Cross-)?(?:Appell(?:ant|ee)|Plaintiff|Defendant|"
    r"Petitioner|Respondent|Relator|Intervenor)s?)*\.?(?=\s|,|$)")
_ORDINAL_CIRCUITS = {
    "first": "1st", "second": "2d", "third": "3d", "fourth": "4th",
    "fifth": "5th", "sixth": "6th", "seventh": "7th", "eighth": "8th",
    "ninth": "9th", "tenth": "10th", "eleventh": "11th",
}
_NAME_KEEP_CAPS = {"LLC", "LLP", "PLLC", "USA", "II", "III", "IV", "U.S.",
                   "FBI", "SEC", "IRS", "EPA", "NLRB", "DC"}
_NAME_SMALL_WORDS = {"of", "the", "and", "in", "on", "for", "at", "de", "la",
                     "an", "a", "to", "by"}


def _fed_court_abbr(line: str) -> "Optional[str]":
    """Bluebook abbreviation when `line` names a federal court ("United
    States Court of Appeals, Fourth Circuit." → "4th Cir."), "" for the
    Supreme Court (never named in a parenthetical), None when the line is
    not a federal court at all."""
    low = " ".join((line or "").lower().split())
    if "circuit" in low and ("court of appeals" in low
                             or "united states" in low):
        if "district of columbia circuit" in low:
            return "D.C. Cir."
        if "federal circuit" in low:
            return "Fed. Cir."
        m = re.search(r"\b(first|second|third|fourth|fifth|sixth|seventh|"
                      r"eighth|ninth|tenth|eleventh)\s+circuit", low)
        if m:
            return f"{_ORDINAL_CIRCUITS[m.group(1)]} Cir."
        m = re.search(r"\b(\d{1,2})(?:st|n?d|th)\s+circuit", low)
        if m:
            n = int(m.group(1))
            suffix = {1: "st", 2: "d", 3: "d"}.get(n, "th")
            return f"{n}{suffix} Cir."
    if "district court" in low and ("united states" in low or "u.s." in low):
        m = re.search(r"\b([nsewc])\.?\s?d\.\s+([a-z. ]+?)[.,]?$", low)
        if m:
            for sname in sorted(_STATE_BLUEBOOK, key=len, reverse=True):
                if sname in m.group(2):
                    return f"{m.group(1).upper()}.D. {_STATE_BLUEBOOK[sname]}"
        m = re.search(r"\bdistrict of\s+([a-z. ]+?)[.,]?$", low)
        if m:
            for sname in sorted(_STATE_BLUEBOOK, key=len, reverse=True):
                if sname in m.group(1):
                    return f"D. {_STATE_BLUEBOOK[sname]}"
    if "supreme court" in low and "united states" in low:
        return ""
    return None


def _titlecase_caps(s: str) -> str:
    """Title-case the ALL-CAPS words of a reporter caption ("BOARD OF ZONING
    APPEALS" → "Board of Zoning Appeals"), leaving mixed-case words and
    initialisms (LLC, U.S.) alone."""
    return normal_case_caption(s)


def _caption_fields(text: str) -> tuple:
    """(case name, court abbr or None, year, citation lines) read from a
    case.law first page.  The caption is the surviving text above the body:
    parties (with their role labels), docket line, court line, dates —
    exactly the pieces the redacted running head needs back."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()][:14]
    name_parts: list = []
    court = None
    year = ""
    cite_lines: list = []
    in_name = True
    for ln in lines:
        if _CAPTION_BREAK_RE.match(ln):
            break
        if re.fullmatch(r"\d{1,5}", ln):        # the bare page number
            continue
        if re.match(r"^\d{1,4}\s+\S", ln) and _ANY_CITE_RE.search(ln):
            cite_lines.append(ln)               # "171 A.3d 204"
            continue
        fed = _fed_court_abbr(ln)
        if fed is not None:
            court = fed
            in_name = False
            continue
        st = _bluebook_court_from_name(ln)
        if st:
            court = st
            in_name = False
            continue
        if _CAPTION_DATE_RE.search(ln):
            year = _CAPTION_DATE_RE.search(ln).group(1)  # last date wins
            in_name = False
            continue
        if (re.match(r"^(?:Nos?\.|Case No\.)", ln) or "Term" in ln
                or re.fullmatch(r"[\dA-Z][\w:().-]*", ln)
                or re.match(r"^(?:Argued|Decided|Submitted|Filed|Heard)\b",
                            ln)):
            in_name = False
            continue
        if in_name:
            name_parts.append(ln)
    raw = " ".join(name_parts)
    raw = re.sub(r"\s*\([^)]*\)\s*$", "", raw)   # "(ATTORNEY NO. …)"
    raw = _ROLE_LABEL_RE.sub("", raw)
    raw = re.sub(r"\s+", " ", raw).strip(" ,.")
    raw = re.sub(r",?\s+v\.?\s+", " v. ", raw, flags=re.I)
    raw = re.sub(r",\s*an?\s+[A-Za-z' .]+$", "", raw)  # ", an Attorney at Law"
    return _titlecase_caps(raw), court, year, cite_lines


def _case_law_case_json(url: str, timeout: int = 8) -> "Optional[dict]":
    """The metadata JSON case.law publishes beside each scan —
    …/{rep}/{vol}/case-pdfs/NNNN-01.pdf ↔ …/{rep}/{vol}/cases/NNNN-01.json —
    holding the clean case name, official citation, court, and decision
    date; None when the fetch fails.  This is the running head's first
    source: the scans' own text is OCR, and a Fed. App'x case often isn't
    on Scholar or CourtListener at all."""
    json_url = re.sub(r"/case-pdfs/(\d+-\d+)\.pdf$", r"/cases/\1.json",
                      url or "", flags=re.I)
    if not json_url or json_url == url:
        return None
    try:
        resp = _anon_session.get(json_url, timeout=timeout)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


_GEO_TAIL_WORDS = ("county", "cnty", "parish", "borough", "township",
                   "municipality", "village")


def _trim_geographic_tails(name: str) -> str:
    """Bluebook rule 10.2.1(f): drop a geographic designation following a
    comma at the end of a party name ("Bd. of Zoning Appeals of Evansville,
    Vanderburgh County" → "… of Evansville").  Doing it here also unblocks
    the abbreviator, which leaves a party alone when it can't parse the
    tail; corporate designators (", Inc.") are not geographic and stay."""
    def trim(party: str) -> str:
        while True:
            m = re.search(r",\s*([^,]+?)\s*$", party)
            if not m:
                return party
            tail = m.group(1).strip(" .").lower()
            words = tail.split()
            if words and (words[-1].strip(".") in _GEO_TAIL_WORDS
                          or tail in _STATE_BLUEBOOK):
                party = party[:m.start()]
                continue
            return party
    return " v. ".join(trim(p) for p in re.split(r"\s+v\.\s+", name or ""))


def _case_law_print_citation(pdf_bytes: bytes, url: str, title: str = "",
                             item: "Optional[dict]" = None,
                             cite_hint: str = "", client=None) -> str:
    """The Bluebook citation for *this* reporter's scan, to re-letter the
    whitened running-head line: "Name, 855 N.E.2d 286 (Ind. Ct. App. 2006)".

    Sources, best first: the scan's parallel case.law metadata JSON (see
    :func:`_case_law_case_json`), then whatever the calling window knows
    (CourtListener item, window title), then a CourtListener citation
    lookup for pieces still missing, and only then the scan's own
    first-page caption — that text is OCR, and a Fed. App'x PDF's first
    page often *starts* inside the previous case.  The case name is set in
    the app's Bluebook form (:func:`abbreviate_case_name`, after a rule-
    10.2.1(f) trim of geographic comma-tails), and the court parenthetical
    follows rule 10.4 via :func:`_court_for_paren` (omitted or trimmed when
    the reporter already conveys it).  Returns "" when even the reporter
    citation can't be established."""
    m = _CASE_LAW_URL_RE.search(url or "")
    if not m:
        return ""
    slug, vol, first_page = m.group(1), m.group(2), str(int(m.group(3)))

    meta = _case_law_case_json(url) or {}
    meta_court = meta.get("court") if isinstance(meta.get("court"), dict) \
        else {}
    m_name = str(meta.get("name_abbreviation") or "").strip()
    m_court = str(meta_court.get("name_abbreviation")
                  or meta_court.get("name") or "").strip()
    m_year = str(meta.get("decision_date") or "")[:4]
    if not m_year.isdigit():
        m_year = ""
    m_cites = [str(c.get("cite") or "").strip()
               for c in (meta.get("citations") or []) if isinstance(c, dict)]

    it = item or {}
    it_name = str(it.get("caseName") or it.get("case_name") or "").strip()
    t_name = ""
    if " — " in (title or ""):
        cand = title.split(" — ", 1)[0].strip()
        if " v. " in cand or re.match(
                r"(?i)^(?:in\s+re|ex\s+parte|(?:in\s+the\s+)?matter\s+of)\b",
                cand):
            t_name = cand
    it_date = str(it.get("dateFiled") or it.get("date_filed") or "")
    it_year = it_date[:4] if it_date[:4].isdigit() else ""
    court_id = str(it.get("court_id") or "").strip().lower()
    it_court = str(it.get("court") or "").strip()

    def _extract_caption() -> tuple:
        text = ""
        try:
            import pypdfium2 as pdfium
            with _PDFIUM_LOCK:
                doc = pdfium.PdfDocument(pdf_bytes)
                try:
                    tp = doc[0].get_textpage()
                    try:
                        text = tp.get_text_range()
                    finally:
                        tp.close()
                finally:
                    doc.close()
        except Exception:
            text = ""
        return _caption_fields(text)

    # The citation for this reporter: a candidate counts only when it maps
    # back to this exact PDF's URL.  The scan's caption is scanned for one
    # only when the better sources and the slug table all came up empty.
    caption = None

    def _pick_cite(sources) -> str:
        for source in sources:
            for cm in _ANY_CITE_RE.finditer(source or ""):
                c = re.sub(r"\s+", " ", cm.group(0)).strip()
                if (_static_case_law_url(c) or "").lower() \
                        == (url or "").lower():
                    return c
        return ""

    cite = _pick_cite(m_cites + [cite_hint, title])
    if not cite:
        rep = _CASE_LAW_SLUG_REPORTERS.get(slug)
        if rep:
            cite = f"{vol} {rep} {first_page}"
        else:
            caption = _extract_caption()
            cite = _pick_cite(caption[3])
            if not cite:
                return ""

    name = m_name or it_name or t_name
    year = m_year or it_year
    have_court = bool(m_court or court_id or it_court)

    # CourtListener fills what the JSON and the window couldn't.
    cl_court_id = ""
    cl_court = ""
    if client is not None and not (name and year and have_court):
        try:
            cl = _cl_item_for_citation(client, cite, name=name) or {}
        except Exception as exc:
            print(f"[pdf] header CL lookup failed for {cite!r}: {exc}")
            cl = {}
        name = name or str(cl.get("caseName") or cl.get("case_name")
                           or "").strip()
        cl_date = str(cl.get("dateFiled") or cl.get("date_filed") or "")
        year = year or (cl_date[:4] if cl_date[:4].isdigit() else "")
        cl_court_id = str(cl.get("court_id") or "").strip().lower()
        cl_court = str(cl.get("court") or "").strip()
        have_court = have_court or bool(cl_court_id or cl_court)

    # The scan's own caption is the last resort.
    if not (name and year and have_court):
        if caption is None:
            caption = _extract_caption()
        cap_name, cap_court, cap_year = caption[0], caption[1], caption[2]
        name = name or cap_name
        year = year or cap_year
    else:
        cap_court = ""

    if name:
        name = re.sub(r"<[^>]+>", "", name).strip(" ,")
        name = _trim_geographic_tails(name)
        try:
            name = abbreviate_case_name(name)   # the app's Bluebook form
        except Exception:
            pass

    if m_court:
        court = _court_for_paren(cite, "", m_court)
    else:
        court = _court_for_paren(
            cite, court_id or cl_court_id,
            it_court or cl_court or (cap_court or ""))

    paren = " ".join(p for p in (court, year) if p)
    out = ", ".join(p for p in (name, cite) if p)
    return f"{out} ({paren})" if paren else out


def _client_for_window(app) -> "Optional[object]":
    """The window's CourtListener client for the running-head lookup, or
    None when the app (or its token) isn't available.  Main thread only —
    the token lives in a tk variable."""
    try:
        if app is not None and app._token_var.get().strip():
            return app._get_client()
    except Exception:
        pass
    return None


def _print_header_font(px: int):
    """A serif font at `px` pixels for the re-lettered running head, from
    the usual system font homes; Pillow's scalable built-in is the floor."""
    from PIL import ImageFont
    for cand in ("/System/Library/Fonts/Supplemental/Times New Roman.ttf",
                 "/Library/Fonts/Times New Roman.ttf",
                 "times.ttf",
                 "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                 "DejaVuSerif.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(cand, px)
        except Exception:
            continue
    try:
        return ImageFont.load_default(px)
    except TypeError:            # Pillow < 10.1: fixed-size bitmap font
        return ImageFont.load_default()


def _header_redaction_box(boxes: list, size: tuple) -> "Optional[tuple]":
    """The page's whitened running-head line: the topmost erased box that is
    header-shaped (in the top 12% of the page and at least 15% of its
    width); None when the page has no such box."""
    W, H = size
    heads = [b for b in boxes
             if b[1] < 0.12 * H and (b[2] - b[0]) >= 0.15 * W]
    return min(heads, key=lambda bx: bx[1]) if heads else None


def _header_citation_fit(citation: str, box: tuple) -> tuple:
    """The ``(text, font size)`` this page's running-head line would get on
    its own: the fullest form that fits the box — the whole citation, else
    the bare reporter cite — at the largest size that fits, floored at 9 px.
    The caller compares fits across pages and letters every page alike."""
    l, t, r, b = box
    box_w, box_h = r - l, b - t
    forms = [citation]
    m = re.search(r"\b\d{1,4}\s+[A-Z]\S*", citation)
    if m and m.start() > 0:
        forms.append(citation[m.start():])      # "855 N.E.2d 286 (… 2006)"
    for fi, form in enumerate(forms):
        size = max(9, int(box_h * 0.72))
        while size > 9 and _print_header_font(size).getlength(
                form) > box_w * 1.04:
            size -= 1
        if (_print_header_font(size).getlength(form) <= box_w * 1.04
                or fi == len(forms) - 1):
            return form, size
    return citation, 9


def _draw_header_citation(img, box: tuple, text: str, font) -> None:
    """Draw `text` centered on the whitened running-head line, nudged
    inward when a size chosen from a wider page's line would run off this
    page's edge."""
    from PIL import ImageDraw
    l, t, r, b = box
    draw = ImageDraw.Draw(img)
    half = draw.textlength(text, font=font) / 2
    cx = max(int(half) + 8, min(img.width - int(half) - 8, (l + r) // 2))
    draw.text((cx, (t + b) // 2), text, font=font, fill=(0, 0, 0),
              anchor="mm")


class _ScholarTextWindow:
    """
    Rich viewer for a Google Scholar opinion.

    Renders the opinion with its original formatting (paragraphs, centering,
    italics, footnote markers), highlights the reporter star-pagination
    markers, makes case citations clickable (fetching the cited case from
    Scholar in a new window), and offers:
      • Copy + Cite — copies selection (or all) with formatting and appends
        a Bluebook citation pin-cited from the star pagination,
      • Export ▾ — RTF (two-column), PDF typeset with LaTeX (single column,
        bottom-of-page footnotes, reporter page range in the running head),
        or Markdown, each named after the Bluebook caption,
      • View PDF — the official opinion PDF, shown in-app (Download PDF there),
      • a toggle to the CourtListener version of the text.
    """

    _PAGENUM_COLOR = "#8e44ad"   # muted purple — visible but not loud
    _LINK_COLOR = "#1a56b0"
    _DISSENT_COLOR = "#a31515"   # dark red — top-of-window label & RTF headings
    _CONCUR_COLOR = "#1a7a3c"    # dark green
    _DISSENT_BG = "#fbeeee"      # very light red — full-view box behind a dissent
    _CONCUR_BG = "#eef7f0"       # very light green — box behind a concurrence
    _SEPARATE_COLOR = "#59636f"  # neutral slate: role is not reliably known
    _SEPARATE_BG = "#f1f3f5"     # neutral gray: neither assent nor disagreement
    # In the full-opinion view the region behind a concurrence/dissent gets a
    # light background tint; the body text itself stays black and the active
    # part is named, in color, at the top of the window.
    _MAJORITY_COLOR = "#1a3e72"  # dark blue — the main opinion on the part map
    _PART_BOX_TAGS = {
        "dissent": "box-dissent", "concurrence": "box-concurrence",
        "separate": "box-separate",
    }
    _PART_LABEL_COLORS = {
        "dissent": _DISSENT_COLOR, "concurrence": _CONCUR_COLOR,
        "separate": _SEPARATE_COLOR,
    }
    # The part map (right strip) also points to the main opinion.
    _PARTMAP_COLORS = {
        "dissent": _DISSENT_COLOR, "concurrence": _CONCUR_COLOR,
        "separate": _SEPARATE_COLOR, "majority": _MAJORITY_COLOR,
    }
    _PAGECOL_W = 48     # left gutter: reporter page numbers (px)
    _PARTMAP_W = 104    # right strip: map of the opinion's parts (px)
    # Approx. on-screen width of the right "Case details" panel (a 38-char Text
    # plus its scrollbar and padding).  Used to widen the window for SCOTUS
    # cases, where the panel opens by default, so the opinion text keeps its
    # full width instead of shrinking to make room.
    _DETAILS_PANEL_W = 300
    _JUSTIFY_HARD_BREAK_EXTRA_SPACES = 4
    _JUSTIFY_PAD_TAG = "justify-pad"
    _JUSTIFY_HIDE_TAG = "justify-hide"
    _HYPHEN_MIN_WORD = 7
    _HYPHEN_MIN_PREFIX = 2
    _HYPHEN_MIN_SUFFIX = 3
    _HYPHEN_SAFETY_PX = 3
    _VOWELS = frozenset("aeiouy")

    def __init__(
        self,
        parent: tk.Misc,
        app: "CourtListenerGUI",
        url: str,
        opinion_html: str,
        item: Optional[dict] = None,
        cl_text: Optional[str] = None,
        note: str = "",
        cl_parts: Optional[list] = None,
        cl_blocks: Optional[list] = None,
        prefetch_pdf: bool = True,
        primary_source_label: str = "CourtListener",
        primary_source_url: str = "",
        primary_source_kind: str = "courtlistener",
        history_pdf_url: str = "",
        initial_pdf: "Optional[tuple[bytes, str]]" = None,
    ) -> None:
        self._app = app
        self._item = item or {}
        self._scholar_url = url
        self._note = note
        self._primary_source_label = (
            primary_source_label.strip() or "CourtListener"
        )
        self._primary_source_url = primary_source_url.strip()
        self._primary_source_kind = primary_source_kind
        self._history_pdf_url = history_pdf_url.strip()
        self._header_cites: list[str] = []  # parallel reporter cites (PDF resolve)
        self._cl_primary = cl_parts is not None and not opinion_html
        if self._cl_primary:
            # Opened as a CourtListener-primary window (Scholar failed).
            self._blocks = cl_blocks or []
            try:
                self._scholar_text = blocks_to_text(self._blocks)
            except Exception:
                self._scholar_text = cl_text or ""
            self._parts = cl_parts or []
            self._cl_parts = cl_parts or []
            self._cl_blocks = cl_blocks or []
        else:
            self._blocks = parse_opinion_blocks(opinion_html)
            self._scholar_text = (
                blocks_to_text(self._blocks) or _strip_html(opinion_html)
            )
            self._parts = segment_blocks(self._blocks)
            self._cl_parts = None
            self._cl_blocks = None
        # Kept for the History dropdown: reopening replays this constructor
        # with the original ingredients (no refetch).
        self._history_html = "" if self._cl_primary else opinion_html
        self._current_part: Optional[int] = None  # None = full opinion
        # Page in effect at the start of each part, for pin cites when a
        # single part is displayed (no preceding star marker on screen).
        self._part_start_pages: list[Optional[int]] = []
        page: Optional[int] = None
        for part in self._parts:
            self._part_start_pages.append(page)
            for b in part.blocks:
                for s in b.spans:
                    if s.pagenum:
                        m = re.search(r"\d+", s.text)
                        if m:
                            page = int(m.group(0))
        self._cl_text: Optional[str] = cl_text
        self._mode = "courtlistener" if self._cl_primary else "scholar"
        self._pdf_pane: Optional[_PdfPane] = None  # set while viewing the PDF
        self._pdf_holder: Optional[ttk.Frame] = None  # pane + parts strip
        self._pdf_url: Optional[str] = None
        self._pdf_bytes: Optional[bytes] = None
        # Background-prefetched PDF (data, url) so "View PDF" is instant; set by
        # _prefetch_pdf, consumed by _view_pdf.
        self._pdf_prefetch: Optional[tuple[bytes, str]] = None
        self._case_law_pdf_choices: list[_CaseLawPdfChoice] = []
        self._pdf_prefetch_started = False
        # CourtListener text view: whether a PDF was located anywhere (None =
        # still looking, True/False = found/not), gating its "View PDF" button.
        self._pdf_located: Optional[bool] = None
        self._pdf_locate_started = False
        if initial_pdf is not None:
            self._pdf_prefetch = initial_pdf
            self._pdf_bytes, self._pdf_url = initial_pdf
            self._pdf_located = True
        elif self._history_pdf_url:
            # A PDF-first view already knows the exact numbered CAP file even
            # if its bytes are still arriving in the other window.
            self._pdf_url = self._history_pdf_url
            self._pdf_located = True
        self._prefetch_ok = prefetch_pdf
        self._pre_pdf_mode = "scholar"  # text view to return to from the PDF
        self._link_actions: dict[str, tuple[str, str]] = {}
        self._link_n = 0
        self._part_region_marks: list[str] = []
        # (volume, reporter) → first pages, so short forms ("410 U.S. at 152")
        # link back to the full citation's case.  Rebuilt on each render.
        self._short_cite_index: dict[tuple[str, str], list[int]] = {}
        # WL/LEXIS cite → RECAP spec, built over the whole opinion so a docket
        # printed once also powers links on later short-form occurrences.
        self._recap_spec_index: dict[str, str] = {}
        # The most recently emitted citation's action — a case ("cite", base),
        # a statute ("usc"/"cfr"/"rule"/"const"/"statpdf"/…), etc. — so an "Id."
        # links back to whatever was last cited.  Reset per render.
        self._last_cite_action: Optional[tuple[str, str]] = None
        # A bare "Id." whose pin ("at N") sits in a following span — (link tag,
        # base cite) — so the pin can be attached when that span arrives.
        self._pending_id: Optional[tuple[str, str]] = None
        # Amendment numbers already linked in this render, so a repeated bare
        # prose mention ("the First Amendment guarantees …") isn't re-linked.
        self._const_linked: set[int] = set()
        self._fonts: dict[str, tkfont.Font] = {}
        self._fn_text: dict[str, str] = {}  # footnote id → body text (for hover tips)
        self._fn_tip: Optional[tk.Toplevel] = None
        self._is_scotus = False  # set by _compute_bluebook_parts
        self._bb = self._compute_bluebook_parts()
        self._citation_override_keys = citation_identity_keys(
            self._item,
            self._bb.get("cite", ""),
            self._header_cites,
            self._scholar_url,
        )
        self._base_citation_override = find_override(
            _load_config().get("citation_overrides", {}),
            self._citation_override_keys,
        )
        if not self._cl_primary:
            self._refine_part_labels(self._parts)
        # Whether Google Scholar actually carries the opinion text (it usually
        # doesn't for Federal Appendix cases — they're scans only).  Gates the
        # "Google Scholar Text" toggle while the PDF is showing.
        self._scholar_has_text = (
            not self._cl_primary
            and len(re.sub(r"\s+", "", self._scholar_text or "")) >= 500
        )
        self._fed_appx = (not self._cl_primary) and self._is_fed_appx()

        self._win = _case_view_host(parent, app)
        _ensure_modern_ttk_styles(self._win)
        self._win.title(
            self._title_citation() or (
                f"{self._primary_source_label} Opinion Text" if self._cl_primary
                else "Google Scholar Opinion Text"
            )
        )
        self._history_menubar = _install_history_menubar(self._app, self._win)
        # SCOTUS cases open the Oyez "Case details" panel by default (wired up
        # in _build_ui); widen the window by the panel's width so the opinion
        # text keeps its usual room with the panel added to the right of it.
        self._details_panel_w = int(
            round(self._DETAILS_PANEL_W * _screen_text_scale(self._win))
        )
        win_w = 860 + (self._details_panel_w if self._is_scotus else 0)
        self._win.geometry(
            _fit_toplevel_geometry(
                self._win, win_w, 680, min_width=430, min_height=300
            )
        )
        self._win.minsize(430, 300)
        self._build_ui()
        if self._cl_primary:
            self._render_cl_blocks()
        else:
            self._render_scholar()
            # Warm the official PDF in the background so "View PDF" is instant.
            # Suppressed when opened from the PDF brief viewer: a second PDF
            # being resolved/downloaded while that big pdfium view is live could
            # hang the app, so those cases open straight to the Scholar text and
            # leave the PDF to the on-demand "View PDF" button.
            if prefetch_pdf:
                self._prefetch_pdf()
                # Federal Appendix cases are scans, not text — open straight on
                # the PDF (Scholar rarely has the opinion text for them).
                if self._fed_appx:
                    self._win.after(0, self._view_pdf)
        # Fill in whatever the opinion text can't give us for a proper Bluebook
        # citation — chiefly the federal-district / lower-court parenthetical and
        # a missing year — from CourtListener in the background, then refresh the
        # title.  The window opens immediately on the best-effort citation.
        self._enrich_citation()
        self._record_history()

    # ------------------------------------------------------------------
    # Case-view history
    # ------------------------------------------------------------------

    def _history_key(self) -> str:
        """Identity of this case for the History dropdown (a re-view of the
        same case replaces its entry instead of duplicating it)."""
        if self._primary_source_kind == "case_law" and self._history_pdf_url:
            return f"pdf:{self._history_pdf_url}"
        if self._scholar_url:
            return f"scholar:{self._scholar_url}"
        cluster = self._item.get("cluster_id") or self._item.get("id")
        if cluster:
            return f"cl:{cluster}"
        return f"case:{self._history_label()}"

    def _history_label(self) -> str:
        try:
            return self._title_citation() or self._win.title()
        except Exception:
            return self._win.title()

    def _record_history(self) -> None:
        """Register this view in the app-wide History dropdown.  The reopen
        callable replays this constructor with the stored ingredients, so a
        pick from the menu is instant (no refetch)."""
        app = self._app
        if app is None or not hasattr(app, "record_case_view"):
            return
        item = dict(self._item)
        if self._cl_primary and self._primary_source_kind == "case_law":
            parts, blocks = self._cl_parts, self._cl_blocks
            text = self._cl_text
            prefetch = self._prefetch_ok
            source_label = self._primary_source_label
            source_url = self._primary_source_url
            pdf_url = self._history_pdf_url
            initial_pdf = self._pdf_prefetch
            payload = {
                "type": "pdf",
                "url": pdf_url,
                "title": self._history_label(),
            }

            def reopen(
                app=app, item=item, parts=parts, blocks=blocks, text=text,
                prefetch=prefetch, source_label=source_label,
                source_url=source_url, pdf_url=pdf_url,
                initial_pdf=initial_pdf,
            ) -> None:
                _ScholarTextWindow(
                    app.root, app, "", "", item=item, cl_text=text,
                    cl_parts=parts, cl_blocks=blocks, prefetch_pdf=prefetch,
                    primary_source_label=source_label,
                    primary_source_url=source_url,
                    primary_source_kind="case_law",
                    history_pdf_url=pdf_url, initial_pdf=initial_pdf,
                )
        elif self._cl_primary:
            parts, blocks = self._cl_parts, self._cl_blocks
            text = self._cl_text
            prefetch = self._prefetch_ok
            payload = {
                "type": "cl",
                "item": item,
                "prefetch_pdf": bool(prefetch),
            }

            def reopen(app=app, item=item, parts=parts, blocks=blocks,
                       text=text, prefetch=prefetch) -> None:
                _ScholarTextWindow(app.root, app, "", "", item=item,
                                   cl_text=text, cl_parts=parts,
                                   cl_blocks=blocks, prefetch_pdf=prefetch)
        else:
            url, html = self._scholar_url, self._history_html
            prefetch = self._prefetch_ok
            payload = {
                "type": "scholar",
                "url": url,
                "item": item,
                "cite": self._bb.get("cite", ""),
                "prefetch_pdf": bool(prefetch),
            }

            def reopen(app=app, url=url, html=html, item=item,
                       prefetch=prefetch) -> None:
                _ScholarTextWindow(app.root, app, url, html, item=item,
                                   prefetch_pdf=prefetch)

        key = self._history_key()
        label = self._history_label()
        app.record_case_view(key, label, reopen, payload=payload)
        if hasattr(app, "register_case_window"):
            app.register_case_window(self, self._win, key, label, reopen)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        win = self._win
        lbl_style = "Modern.TLabel" if _CTK_AVAILABLE else "TLabel"
        muted_style = "ModernMuted.TLabel" if _CTK_AVAILABLE else "TLabel"
        entry_style = "Modern.TEntry" if _CTK_AVAILABLE else "TEntry"
        combo_style = "Modern.TCombobox" if _CTK_AVAILABLE else "TCombobox"

        url_frame = _ui_frame(win)
        url_frame.pack(fill="x", padx=12, pady=(12, 0))
        ttk.Label(url_frame, text="Source", style=muted_style).pack(side="left")
        initial_source = (
            self._primary_source_url or self._primary_source_label
            if self._cl_primary else self._scholar_url
        )
        self._source_var = tk.StringVar(value=initial_source)
        ttk.Entry(url_frame, textvariable=self._source_var, state="readonly",
                  style=entry_style).pack(
            side="left", fill="x", expand=True, padx=(8, 0)
        )

        # Part navigation: what you're viewing, and a selector to filter
        view_frame = _ui_frame(win)
        view_frame.pack(fill="x", padx=12, pady=(8, 0))
        ttk.Label(view_frame, text="Viewing", style=muted_style).pack(side="left")
        self._view_label_var = tk.StringVar(value="Full opinion")
        self._view_label = ttk.Label(
            view_frame,
            textvariable=self._view_label_var,
            style=lbl_style,
            font=("TkDefaultFont", 11, "bold"),
        )
        self._view_label.pack(side="left", padx=(6, 12))
        part_values = ["Full opinion"] + [
            f"{i + 1}. {p.label}" for i, p in enumerate(self._parts)
        ]
        self._part_combo = ttk.Combobox(
            view_frame, state="readonly", width=44, values=part_values,
            style=combo_style,
        )
        self._part_combo.current(0)
        self._part_combo.pack(side="right")
        self._part_combo.bind("<<ComboboxSelected>>", self._on_part_selected)
        if len(self._parts) <= 1:
            self._part_combo.config(state="disabled")

        text_frame = ttk.Frame(win)
        text_frame.pack(fill="both", expand=True, padx=8, pady=4)
        base = tkfont.Font(family=self._opinion_font_family(), size=_OPINION_FONT_PT)
        self._fonts["base"] = base
        self._family = base.actual("family")
        self._base_size = base.actual("size")
        txt = tk.Text(text_frame, wrap="word", font=base, padx=14, pady=10)
        self._text = txt
        vsb = ttk.Scrollbar(text_frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=self._on_yscroll)
        # Left gutter: reporter page numbers in bold black, scrolling with the
        # text — a separate Canvas, so it can't be selected/copied (the page is
        # already marked inline in purple).  Right strip: a colour-coded map of
        # where each concurrence/dissent begins (full-opinion view only).
        self._pagecol_font = tkfont.Font(
            family="Georgia", size=max(self._base_size - 2, 7), weight="bold")
        self._partmap_font = tkfont.Font(
            family="TkDefaultFont", size=max(self._base_size - 3, 7))
        self._pagecol = tk.Canvas(text_frame, width=self._PAGECOL_W, bg="white",
                                  highlightthickness=0, takefocus=0)
        self._partmap = tk.Canvas(text_frame, width=0, bg="white",
                                  highlightthickness=0, takefocus=0)
        self._partmap_rows: list[tuple[float, float, int]] = []
        self._pagecol.pack(side="left", fill="y")
        vsb.pack(side="right", fill="y")
        self._partmap.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)
        txt.bind("<Configure>", lambda _e: self._on_text_configure())
        self._partmap.bind("<Button-1>", self._on_partmap_click)
        self._partmap.bind("<Enter>", lambda _e: self._partmap.config(cursor="hand2"))
        self._partmap.bind("<Leave>", lambda _e: self._partmap.config(cursor=""))
        self._text_frame, self._vsb = text_frame, vsb
        self._details_frame: Optional[ttk.Frame] = None
        self._details_loaded = False
        self._details_case: Optional[tuple] = None  # cached (title, lines)
        self._related_loaded = False
        self._details_related: Optional[tuple] = None  # cached (title, lines)
        self._recent_loaded = False
        self._details_recent: Optional[tuple] = None  # cached (title, lines)

        txt.tag_configure("center", justify="center")
        txt.tag_configure("blockquote", lmargin1=36, lmargin2=36, rmargin=36)
        txt.tag_configure("heading", spacing1=6, spacing3=4)
        txt.tag_configure("underline", underline=True)
        # Full-view part boxes: a light background tint, kept at the bottom of
        # the tag stack so the selection highlight, citation links and page
        # markers all show above it.  Part text is no longer colored — only
        # this subtle box and the top-of-window label distinguish the parts.
        txt.tag_configure("box-dissent", background=self._DISSENT_BG)
        txt.tag_configure("box-concurrence", background=self._CONCUR_BG)
        txt.tag_configure("box-separate", background=self._SEPARATE_BG)
        txt.tag_lower("box-dissent")
        txt.tag_lower("box-concurrence")
        txt.tag_lower("box-separate")
        fnhead_font = tkfont.Font(
            family=self._family, size=max(self._base_size - 2, 8), weight="bold"
        )
        self._fonts["fnhead"] = fnhead_font
        txt.tag_configure(
            "fnhead", font=fnhead_font, foreground="#666666", spacing1=10
        )
        # Star-pagination markers are reporter page references the app
        # interleaves into the text, not the Court's prose, so they stay in
        # the default serif even when a SCOTUS body switches to Century
        # Schoolbook.
        pagenum_font = tkfont.Font(
            family="Georgia", size=max(self._base_size - 1, 8), weight="bold"
        )
        self._fonts["pagenum"] = pagenum_font
        txt.tag_configure(
            "pagenum", font=pagenum_font, foreground=self._PAGENUM_COLOR
        )
        txt.tag_configure("citelink", foreground=self._LINK_COLOR)
        txt.tag_bind("citelink", "<Enter>", lambda _e: txt.config(cursor="hand2"))
        txt.tag_bind("citelink", "<Leave>", lambda _e: txt.config(cursor=""))
        txt.tag_configure("jumpflash", background="#fff2a8")
        txt.tag_configure(self._JUSTIFY_PAD_TAG)
        txt.tag_configure(self._JUSTIFY_HIDE_TAG, elide=True)
        self._finder = _TextFinder(win, txt, text_frame)

        btn_frame = _ui_frame(win)
        btn_frame.pack(fill="x", padx=12, pady=(2, 10))
        self._btn_frame = btn_frame  # PDF/text panes pack just above this
        # (Copy-with-citation lives on Ctrl-C / Cmd-C; no button needed.)
        # In text view this drops the export menu (RTF / PDF via LaTeX /
        # Markdown); in PDF view it becomes "Download PDF".
        self._export_btn = _ui_button(
            btn_frame, "Export ▾", command=self._post_export_menu, width=104
        )
        self._export_btn.pack(side="right", padx=(6, 0))
        # Print: only meaningful in the PDF view, so it's packed there and
        # hidden again in the text view.
        self._print_btn = _ui_button(
            btn_frame, "Print…", command=self._print_pdf, width=86
        )
        self._toggle_btn = _ui_button(
            btn_frame, "CourtListener Text", command=self._toggle_source,
            primary=True, width=150,
        )
        self._toggle_btn.pack(side="right", padx=(6, 0))
        # The CourtListener text view gets its own "View PDF" button (the
        # Scholar view reuses the toggle for that).  Packed in _render_cl_blocks
        # / _show_courtlistener, hidden elsewhere; enabled once a PDF is located.
        self._pdf_btn = _ui_button(
            btn_frame, "View PDF", command=self._view_pdf, width=84
        )
        try:
            self._pdf_btn.configure(state="disabled")
        except tk.TclError:
            pass
        # The Scholar text view's switch back to the CourtListener opinion (the
        # mirror of CL's "Google Scholar Text" button).  Packed in
        # _render_scholar, hidden in the CL and PDF views.
        self._cl_btn = _ui_button(
            btn_frame, "CourtListener Text", command=self._toggle_source,
            width=150,
        )

        # Size controls: text size in the reader, PDF zoom in the PDF view
        # (also Ctrl +/−/0 and Ctrl+mouse wheel).
        self._zoom_out_btn = _ui_button(
            btn_frame, "A−", command=lambda: self._zoom(-1), width=42
        )
        self._zoom_out_btn.pack(side="left")
        self._zoom_in_btn = _ui_button(
            btn_frame, "A+", command=lambda: self._zoom(+1), width=42
        )
        self._zoom_in_btn.pack(side="left", padx=(6, 10))
        # On by default for Supreme Court cases (Oyez fills the panel); the
        # window was widened to fit it.  _toggle_details is fired below.
        self._details_var = tk.BooleanVar(value=self._is_scotus)
        _ui_checkbox(
            btn_frame, "Side panel", self._details_var, self._toggle_details,
        ).pack(side="left", padx=(0, 10))
        # When checked (default), Ctrl-C appends the Bluebook citation (with the
        # pin cite) to the copied text; unchecked, it copies the selection alone.
        self._copy_with_cite = tk.BooleanVar(value=True)
        _ui_checkbox(
            btn_frame, "Copy with citation", self._copy_with_cite,
        ).pack(side="left", padx=(0, 10))
        self._edit_citation_btn = _ui_button(
            btn_frame, "Edit citation…", command=self._edit_base_citation,
            width=108,
        )
        self._edit_citation_btn.pack(side="left", padx=(0, 10))
        self._justify_text = tk.BooleanVar(value=False)
        _ui_checkbox(
            btn_frame, "Justify Opinion Text.", self._justify_text,
            self._on_justify_toggle,
        ).pack(side="left", padx=(0, 10))
        for seq in ("<Control-plus>", "<Control-equal>", "<Control-KP_Add>"):
            win.bind(seq, lambda _e: self._zoom(+1))
        for seq in ("<Control-minus>", "<Control-KP_Subtract>"):
            win.bind(seq, lambda _e: self._zoom(-1))
        win.bind("<Control-0>", lambda _e: self._zoom(0))
        txt.bind(
            "<Control-MouseWheel>",
            lambda e: self._zoom(+1 if e.delta > 0 else -1) or "break",
        )
        txt.bind("<Control-Button-4>", lambda _e: self._zoom(+1) or "break")
        txt.bind("<Control-Button-5>", lambda _e: self._zoom(-1) or "break")
        # Ctrl-C copies with the Bluebook citation appended when the "Copy with
        # citation" box is checked, else the selection alone (the plain default
        # copy is suppressed either way); the find bar's entry keeps native
        # copy since this is bound to the text widget only.
        for seq in ("<Control-c>", "<Command-c>"):
            try:
                txt.bind(seq, lambda _e: self._copy_formatted() or "break")
            except tk.TclError:
                pass  # modifier not supported on this platform

        self._status_var = tk.StringVar()
        _install_status_label(btn_frame, self._status_var)
        self._button_bar_compact: Optional[bool] = None
        btn_frame.bind("<Configure>", self._on_button_bar_configure)

        # Supreme Court cases: open the Oyez case-details panel from the start
        # (the checkbox above defaults on and the window is sized to fit it).
        if self._is_scotus:
            self._toggle_details()

    def _automatic_base_citation(self) -> str:
        """The app-computed citation without a pinpoint or final period."""
        bb = self._bb
        name = bb.get("name", "")
        cite = bb.get("display_cite") or bb.get("cite", "")
        value = f"{name}, {cite}" if name and cite else (name or cite)
        paren = "" if bb.get("omit_parenthetical") else " ".join(
            p for p in (bb.get("court", ""), bb.get("year", "")) if p
        )
        return f"{value} ({paren})" if value and paren else value

    def _set_base_citation_override(self, value: str) -> None:
        """Persist a custom base citation, or restore the automatic one."""
        old_history_key = self._history_key()
        value = clean_base_citation(value)
        data = _load_config()
        data["citation_overrides"] = update_overrides(
            data.get("citation_overrides", {}),
            self._citation_override_keys,
            value,
        )
        _save_config(data)
        self._base_citation_override = value

        title = self._title_citation()
        if title:
            self._win.title(title)
            app = self._app
            if app is not None and hasattr(app, "retitle_case_view"):
                app.retitle_case_view(old_history_key, title)
        self._status_var.set(
            "Saved the custom citation for future use."
            if value else "Restored automatic Bluebooking for this opinion."
        )

    def _edit_base_citation(self) -> None:
        """Edit the no-pincite citation used by copy and export operations."""
        if not self._citation_override_keys:
            messagebox.showwarning(
                "Edit Citation",
                "This opinion has no stable reporter or database identifier, so a "
                "citation override cannot yet be saved for future use.",
                parent=self._win,
            )
            return

        dlg = _ui_toplevel(self._win)
        dlg.title("Edit Base Citation")
        dlg.transient(self._win)
        dlg.resizable(True, False)
        frame = _ui_frame(dlg)
        frame.pack(fill="both", expand=True, padx=16, pady=14)
        ttk.Label(
            frame,
            text=("Edit the citation without a pinpoint page. Pinpoints and opinion "
                  "writer parentheticals will still be added automatically when you copy."),
            wraplength=650,
            justify="left",
        ).pack(fill="x", pady=(0, 10))
        value = tk.StringVar(
            value=self._base_citation_override or self._automatic_base_citation()
        )
        entry = ttk.Entry(frame, textvariable=value, width=90)
        entry.pack(fill="x")
        entry.select_range(0, "end")
        entry.focus_set()

        buttons = _ui_frame(frame)
        buttons.pack(fill="x", pady=(14, 0))

        def save() -> None:
            edited = clean_base_citation(value.get())
            if not edited:
                messagebox.showwarning(
                    "Edit Citation", "Enter a base citation, or choose Use Automatic.",
                    parent=dlg,
                )
                return
            self._set_base_citation_override(edited)
            dlg.destroy()

        _ui_button(buttons, "Save", command=save, primary=True, width=86).pack(side="right")
        _ui_button(buttons, "Cancel", command=dlg.destroy, width=86).pack(
            side="right", padx=(0, 8)
        )
        _ui_button(
            buttons, "Use Automatic",
            command=lambda: (self._set_base_citation_override(""), dlg.destroy()),
            width=116,
        ).pack(side="left")
        dlg.bind("<Return>", lambda _e: save())
        dlg.bind("<Escape>", lambda _e: dlg.destroy())
        dlg.grab_set()

    def _on_button_bar_configure(self, event) -> None:
        compact = event.width < 760
        if compact == self._button_bar_compact:
            return
        self._button_bar_compact = compact
        self._apply_button_bar_compact()

    def _button_text(self, button) -> str:
        try:
            return str(button.cget("text") or "")
        except Exception:
            return ""

    def _source_or_pdf_widths(self, button) -> tuple[int, int]:
        text = self._button_text(button)
        if text.startswith("View PDF"):
            return (96, 84) if "▾" in text else (84, 70)
        if text == "No PDF":
            return 84, 70
        if text.startswith("Finding PDF"):
            return 104, 90
        if text == "Back to Text":
            return 106, 88
        return 150, 118

    def _export_widths(self) -> tuple[int, int]:
        text = self._button_text(self._export_btn)
        if "Download" in text:
            return 112, 92
        return 104, 82

    def _apply_button_bar_compact(self) -> None:
        compact = bool(getattr(self, "_button_bar_compact", False))
        toggle_normal, toggle_small = self._source_or_pdf_widths(self._toggle_btn)
        pdf_normal, pdf_small = self._source_or_pdf_widths(self._pdf_btn)
        export_normal, export_small = self._export_widths()
        widths = (
            (self._export_btn, export_normal, export_small),
            (self._print_btn, 86, 64),
            (self._toggle_btn, toggle_normal, toggle_small),
            (self._pdf_btn, pdf_normal, pdf_small),
            (self._cl_btn, 150, 118),
            (self._zoom_out_btn, 42, 34),
            (self._zoom_in_btn, 42, 34),
            (self._edit_citation_btn, 108, 88),
        )
        for btn, normal, small in widths:
            _set_ui_button_width(btn, small if compact else normal)
            if _CTK_AVAILABLE:
                try:
                    btn.configure(height=30 if compact else 34)
                except tk.TclError:
                    pass

    def _set_view_color(self, color: str) -> None:
        """Recolour the "Viewing" label to mark a concurrence/dissent — via the
        right option for whichever widget kind the label is (a ttk.Label styled
        with a modern theme here, so ``foreground`` applies)."""
        try:
            if isinstance(self._view_label, ttk.Label):
                self._view_label.configure(foreground=color)
            else:
                self._view_label.configure(text_color=color)
        except tk.TclError:
            pass

    def _zoom(self, delta: int) -> None:
        """In the reader, grow/shrink every font (delta 0 resets to default);
        Tk re-renders widgets when a named Font object is reconfigured, so
        resizing the shared Font instances restyles all existing text.  In the
        PDF view the same controls zoom the rendered page instead."""
        if self._mode == "pdf" and self._pdf_pane is not None:
            self._pdf_pane.zoom(delta)
            self._status_var.set(f"PDF zoom: {self._pdf_pane.zoom_percent()}%")
            return
        global _OPINION_FONT_PT
        new = 11 if delta == 0 else max(
            _OPINION_FONT_MIN, min(_OPINION_FONT_MAX, self._base_size + delta)
        )
        if new == self._base_size:
            return
        self._base_size = new
        _OPINION_FONT_PT = new
        _save_opinion_font_pt(new)
        for name, f in self._fonts.items():
            if name == "base":
                f.configure(size=new)
            elif name == "fnhead":
                f.configure(size=max(new - 2, 8))
            elif name == "pagenum":
                f.configure(size=max(new - 1, 8))
            elif name.startswith("fnt_"):
                small, sup = name[6] == "1", name[7] == "1"
                f.configure(
                    size=max(new - (3 if sup else 2 if small else 0), 7)
                )
        # Keep the side gutters in step with the body text.
        if getattr(self, "_pagecol_font", None) is not None:
            self._pagecol_font.configure(size=max(new - 2, 7))
            self._partmap_font.configure(size=max(new - 3, 7))
        self._schedule_text_justify()
        self._schedule_gutter_redraw()
        self._status_var.set(f"Text size: {new} pt")

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _font_tag(self, italic: bool, bold: bool, small: bool, sup: bool,
                  family: Optional[str] = None) -> str:
        fam = family or self._family
        prefix = "fnt_" if fam == self._family else "fna_"
        name = prefix + "".join("1" if f else "0" for f in (italic, bold, small, sup))
        if name not in self._fonts:
            size = self._base_size - (3 if sup else 2 if small else 0)
            f = tkfont.Font(
                family=fam,
                size=max(size, 7),
                slant="italic" if italic else "roman",
                weight="bold" if bold else "normal",
            )
            self._fonts[name] = f
            self._text.tag_configure(name, font=f, offset=4 if sup else 0)
        return name

    # Century Schoolbook is the Supreme Court's house typeface; prefer the
    # first installed variant for SCOTUS opinions.  Names vary by platform
    # (URW "Century Schoolbook L" on Linux, the TeX Gyre Schola clone, etc.).
    _SCOTUS_FONT_FAMILIES = (
        "Century Schoolbook",
        "New Century Schoolbook",
        "Century Schoolbook L",
        "Century Schoolbook Std",
        "TeX Gyre Schola",
        "Century",
    )
    # Non-SCOTUS opinions use Palatino Linotype; the follow-on names cover
    # common Palatino-compatible installs on macOS/Linux before falling back to
    # Georgia when no Palatino family is available.
    _NON_SCOTUS_FONT_FAMILIES = (
        "Palatino Linotype",
        "Palatino",
        "Book Antiqua",
        "URW Palladio L",
        "TeX Gyre Pagella",
        "Georgia",
    )

    def _opinion_font_family(self) -> str:
        """Body font for the opinion text.

        Supreme Court decisions use Century Schoolbook variants where present;
        all other opinions prefer Palatino Linotype for consistent justification
        metrics, with compatible fallbacks for systems that lack the Windows
        font.
        """
        available = {f.lower() for f in tkfont.families(self._win)}
        families = (
            self._SCOTUS_FONT_FAMILIES
            if self._is_scotus else self._NON_SCOTUS_FONT_FAMILIES
        )
        for fam in families:
            if fam.lower() in available:
                return fam
        return families[0]

    def _new_link(self, action: tuple[str, str]) -> str:
        self._link_n += 1
        tag = f"lnk{self._link_n}"
        self._link_actions[tag] = action
        self._text.tag_bind(
            tag, "<Button-1>", lambda _e, t=tag: self._follow_link(t)
        )
        if action[0] == "fnref":
            # Hovering an in-text footnote marker previews the note's text.
            fid = action[1]
            self._text.tag_bind(
                tag, "<Enter>", lambda e, i=fid: self._show_fn_tip(e, i), add="+"
            )
            self._text.tag_bind(
                tag, "<Leave>", lambda _e: self._hide_fn_tip(), add="+"
            )
        return tag

    # ------------------------------------------------------------------
    # Footnote hover tooltip
    # ------------------------------------------------------------------
    def _show_fn_tip(self, event, fid: str) -> None:
        """Pop up the footnote's text next to the hovered marker."""
        text = self._fn_text.get(fid)
        if not text:
            return
        self._hide_fn_tip()
        tip = tk.Toplevel(self._text)
        tip.wm_overrideredirect(True)
        try:
            tip.attributes("-topmost", True)
        except tk.TclError:
            pass
        tk.Label(
            tip, text=text, justify="left", wraplength=460,
            background="#fffbe6", foreground="#000000",
            relief="solid", borderwidth=1,
            font=(self._family, max(self._base_size - 2, 8)),
            padx=8, pady=5,
        ).pack()
        tip.wm_geometry(f"+{event.x_root + 14}+{event.y_root + 18}")
        self._fn_tip = tip

    def _hide_fn_tip(self) -> None:
        if self._fn_tip is not None:
            self._fn_tip.destroy()
            self._fn_tip = None

    def _insert_span(self, span, block_tags: tuple, neutral: bool = False,
                     link_tag: Optional[str] = None) -> None:
        txt = self._text
        tags = list(block_tags)
        if span.pagenum:
            m = re.search(r"\d+", span.text)
            if m:
                self._cur_page = int(m.group(0))
                # where each star page begins, for pin-cited link arrivals
                self._page_pos.setdefault(self._cur_page,
                                          txt.index("end-1c"))
            tags.append("pagenum")
            txt.insert("end", span.text, tuple(tags))
            return
        if span.fnref and span.fnref not in self._fnref_pages:
            # Page in effect where the footnote is referenced — that's the
            # page a Bluebook "n.N" pin cite uses.
            self._fnref_pages[span.fnref] = self._cur_page
        tags.append(self._font_tag(
            span.italic, span.bold, span.small, span.sup,
            family="Georgia" if neutral else None,
        ))
        if span.underline:
            tags.append("underline")
        if span.fnref:
            # In-text footnote marker: click jumps to the footnote body
            self._fn_ref_pos.setdefault(span.fnref, txt.index("end-1c"))
            tags += ["citelink", self._new_link(("fnref", span.fnref))]
            txt.insert("end", span.text, tuple(tags))
            return
        if span.fndef:
            # Footnote-body marker: click jumps back to the reference
            self._fn_def_pos[span.fndef] = txt.index("end-1c")
            tags += ["citelink", self._new_link(("fndef", span.fndef))]
            txt.insert("end", span.text, tuple(tags))
            return
        if span.link:
            # A Google Scholar case hyperlink.  Its click action is computed
            # from the *whole* reference (see _scholar_link_action); when the
            # link is split across several styled spans (italic case name, roman
            # reporter cite, …), _insert_block hands every span the one shared
            # *link_tag* so clicking the case name, the reporter, the year — any
            # part — does the same thing and the same CourtListener / case.law
            # fallback applies if Google Scholar fails.
            if link_tag is None:
                link_tag = self._new_link(
                    self._scholar_link_action(span.text, span.link)
                )
            tags += ["citelink", link_tag]
            txt.insert("end", span.text, tuple(tags))
            return
        # Plain text: make recognizable citations clickable
        self._insert_plain_with_links(span.text, tuple(tags))

    def _scholar_link_action(self, full_text: str, href: str) -> tuple[str, str]:
        """The click action for a Google Scholar case hyperlink whose text is
        *full_text* (the whole reference — case name through reporter cite).

        Reading the cite and case name from the full text (not a styled
        fragment) means a link split across spans still resolves, and lets a
        Federal Appendix or English Reports cite be rerouted to its own better
        source.  Records this as the last cited case so a following "Id."
        resolves to it.  A short form ("Quinn, 8 F.4th at 565") is resolved back
        to the full cite via the document index; old nominative SCOTUS cites
        ("3 Dall. 386") are captured too, so a link Google can't open still
        falls back to CourtListener / case.law instead of dead-ending."""
        ref, pin = _link_cite(full_text, self._short_cite_index)
        if ref:
            self._last_cite_action = ("cite", ref)
        # Federal Appendix cite Scholar hyperlinked (often to the wrong
        # scholar_case page) — open the official static.case.law PDF instead.
        if ref and _FED_APPX_RE.search(ref):
            self._last_cite_action = ("cite", ref)
            return ("cite", ref + (f"@{pin}" if pin else ""))
        # An English Reports cite we hold → our CommonLII scan (Scholar's copy
        # of these old English cases is usually missing or a poor scan).  Only
        # override on a real index match, so unknown E.R. cites keep the link.
        er_m = eng_rep.ER_CITE_RE.search(full_text)
        if er_m:
            er_spec = eng_rep.cite_spec(er_m)
            if eng_rep.resolve(er_spec):
                self._last_cite_action = ("engrep", er_spec)
                return ("engrep", er_spec)
        # Same for a link citing only the nominate form ("9 Exch. 341" with no
        # E.R. parallel) — resolution-gated, so U.S. cites are never claimed.
        nom = eng_rep.iter_nominate_cites(re.sub(r"<[^>]+>", "", full_text))
        if nom:
            self._last_cite_action = ("engrep", nom[0][2])
            return ("engrep", nom[0][2])
        # A CourtListener opinion link whose reporter cite we recognize: prefer
        # our own citation handling (Google Scholar first, then CourtListener /
        # case.law) over CourtListener's bare /opinion/N/ link, so where CL's
        # link and ours would conflict, ours wins.  This routes the click the
        # same way a citation we detected in plain text would go.  A cite we
        # can't parse keeps CourtListener's link, so nothing stops working.
        if ref and "courtlistener.com/opinion/" in href:
            return ("cite", f"{ref}@{pin}" if pin else ref)
        # Open the Scholar opinion, carrying the pincite (so it jumps to the
        # right page), the reporter cite, and the case name so a failed/blocked
        # fetch still locates the case on CourtListener (by cite, or by name
        # when no cite parses) / case.law.
        value = href
        if pin:
            value += f"\tpin={pin}"
        if ref:
            value += f"\tcite={ref}"
        link_name = _link_name(full_text)
        if link_name:
            value += f"\tname={link_name}"
        return ("url", value)

    def _const_link_action(self, spec: str, matched_text: str):
        """Action for a U.S. Constitution citation, or None to leave it as plain
        text.  A bare *prose* amendment reference ("the First Amendment …", no
        section/clause and not a "U.S. Const." citation) is linked only the
        first time that amendment appears in the opinion — repeated prose
        mentions of the same amendment aren't re-linked.  Formal citations
        ("U.S. Const. amend. I", or any reference carrying a § section) always
        link."""
        kind, num, sec = (spec.split(":") + ["", "", ""])[:3]
        prose = "const" not in re.sub(r"\s+", " ", matched_text).lower()
        if kind == "amend" and num.isdigit():
            n = int(num)
            if prose and not sec and n in self._const_linked:
                return None  # repeated bare prose mention — leave as plain text
            self._const_linked.add(n)
        return ("const", spec)

    def _insert_plain_with_links(self, text: str, tags: tuple) -> None:
        """Insert text, turning case citations, U.S. Code citations, and
        C.F.R. citations into clickable links (Scholar lookup / OLRC and
        eCFR statute viewers)."""
        txt = self._text
        # A bare "Id." in the previous span may have its pin ("at N") here —
        # Scholar italicizes "Id." into its own span, splitting it from the
        # page.  Link "Id. … at N" (the already-rendered "Id." retroactively,
        # plus the pin here) only when N is a page of that reporter, not a
        # record page (see _id_pin_in_range); otherwise leave it as plain text.
        pend = self._pending_id
        self._pending_id = None
        if pend:
            id_start, id_end, ref = pend
            mp = re.match(r"\s*,?\s*at\s+(\d{1,5})\b", text)
            if mp and _id_pin_in_range(ref, mp.group(1)):
                link_tag = self._new_link(("cite", f"{ref}@{mp.group(1)}"))
                txt.tag_add("citelink", id_start, id_end)
                txt.tag_add(link_tag, id_start, id_end)
                txt.insert("end", text[:mp.end()], tags + ("citelink", link_tag))
                text = text[mp.end():]
                if not text:
                    return
        matches: list[tuple[int, int, str, re.Match]] = []
        for start, end, action in _recap_citation_ranges(
                text, self._recap_spec_index):
            matches.append((start, end, "recap", action[1]))
        for m in _TEXT_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "cite", m))
        # Early-SCOTUS nominative cites, with or without the interpolated
        # parallel U.S. volume ("4 Wheat. [17 U. S.] 438", "9 Wall. (76
        # U. S.) 136", "76 U.S. (9 Wall.) 136", bare "1 Cranch 137") —
        # pre-normalized to the two-part cite the resolvers know
        # ("4 Wheat. 438"), whose modern U.S. volume the lookup paths
        # already try via _citation_search_variants.
        for pat in (_NOMINATIVE_PARALLEL_RE, _US_NOMINATIVE_PARALLEL_RE,
                    _NOMINATIVE_TEXT_CITE_RE):
            for m in pat.finditer(text):
                if any(m.start() < e and s < m.end()
                       for s, e, _k, _v in matches):
                    continue
                matches.append((m.start(), m.end(), "cite",
                                _case_match_text(m)))
        # Early lower-federal reporters ("1 Sumner, 73" -> "1 Sumn. 73",
        # "35 Fed. Rep. 665" -> "35 F. 665"), the forms CourtListener knows.
        for m in _EARLY_FED_CITE_RE.finditer(text):
            if any(m.start() < e and s < m.end()
                   for s, e, _k, _v in matches):
                continue
            matches.append((m.start(), m.end(), "cite",
                            _early_fed_cite_text(m)))
        for m in us_code.USC_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "usc", m))
        for m in ecfr.CFR_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "cfr", m))
        for m in fed_rules.RULE_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "rule", m))
        for m in constitution.CONST_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "const", m))
        # Short-form citations ("Roe, 410 U.S. at 152"): resolve to the case's
        # full citation (indexed from the opinion) so the link opens it and
        # jumps to the pin page.
        for m in _SHORT_CITE_RE.finditer(text):
            pages = self._short_cite_index.get(
                (m.group(1), _norm_reporter(m.group(2))))
            if not pages:
                continue
            pin = int(m.group(3))
            below = [p for p in pages if p <= pin]
            first = max(below) if below else pages[0]
            rep = re.sub(r"\s+", " ", m.group(2)).strip().replace("U. S.", "U.S.")
            cite = f"{m.group(1)} {rep} {first}"
            if pin != first:
                cite += f"@{pin}"
            matches.append((m.start(), m.end(), "shortcite", cite))
        # "Id. at 152" — refers to the previous citation; resolved in document
        # order in the processing loop below.
        for m in _ID_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "idcite", m))
        for c in state_statutes.iter_cites(text):
            matches.append((c.start, c.end, "statestat", c))
        for m in statutes_at_large.STAT_CITE_RE.finditer(text):
            if statutes_at_large.url_for(m):  # only link volumes GovInfo has
                matches.append((m.start(), m.end(), "stat", m))
        for m in eng_rep.ER_CITE_RE.finditer(text):
            matches.append((m.start(), m.end(), "engrep", m))
        for s, e, spec, _cases in eng_rep.iter_nominate_cites(text):
            matches.append((s, e, "engrepn", spec))
        # Federal Cases cited by case number ("Cole v. The Atlantic, Case
        # No. 2,976"; chained "The Chusan, Id. 2,717") — resolved at click
        # time via the CourtListener API from the printed name and number.
        # The span outranks the bare "Id." match at the same start.
        for s, e, spec in fed_cas.iter_cites(text):
            matches.append((s, e, "fedcas", spec))
        matches.sort(key=lambda t: (t[0], -t[1]))
        pos = 0
        for start, end, kind, m in matches:
            if start < pos:
                continue  # overlapping match — first/longest wins
            if start > pos:
                txt.insert("end", text[pos:start], tags)
            cite_base = ""  # set for case cites, to track the last citation
            if kind == "cite":
                # m is a regex match, or a pre-normalized string for the
                # nominative-SCOTUS pass above.
                if isinstance(m, str):
                    cite = m
                else:
                    cite = re.sub(r"\s+", " ", m.group(0)).replace("U. S.", "U.S.")
                    cite = cite.replace("’", "'")  # straight apostrophe for the search query
                cite_base = cite  # base for a following "Id. at N"
                # A pincite right after ("365 U.S. 167, 171") rides along
                # so the opened case can jump to that page.  A number that
                # opens a parallel cite ("556, 510 A.2d 562") is excluded
                # by the capital letter that follows it.
                pin_m = _PINCITE_AFTER_RE.match(text, end)
                if pin_m:
                    cite += "@" + pin_m.group(1)
                action = ("cite", cite)
            elif kind == "recap":
                action = ("recap", m)  # m is the pre-built JSON spec
            elif kind == "usc":
                action = ("usc", us_code.cite_spec(m))
            elif kind == "rule":
                action = ("rule", fed_rules.cite_spec(m))
            elif kind == "const":
                action = self._const_link_action(
                    constitution.cite_spec(m), m.group(0))
            elif kind == "shortcite":
                action = ("cite", m)  # m is the pre-built "vol rep page@pin"
                cite_base = m.split("@")[0]
            elif kind == "idcite":
                # "Id., at N" → the case last cited, pinned to N — but only when
                # N is plausibly a page of that reporter (within _ID_PIN_WINDOW
                # of its start), so an "Id., at 45" into the trial record isn't
                # linked to the wrong page.  A bare "Id." with no page is never
                # linked here; when its "at N" sits in the next span (Scholar
                # splits them), it's resolved by the _pending_id path above.
                la = self._last_cite_action
                pin = m.group(1)
                if pin is None or not la:
                    action = None
                elif la[0] == "cite":
                    action = (("cite", f"{la[1]}@{pin}")
                              if _id_pin_in_range(la[1], pin) else None)
                else:
                    action = la  # statute/regulation/rule → reopen (no pin page)
            elif kind == "statestat":
                # In-app for priority states (once a parser exists), else a
                # browser link-out.  `m` here is a state_statutes.Cite record.
                action = state_statutes.action_for(m)
            elif kind == "stat":
                # Statutes at Large → free GovInfo scan, shown in the in-app
                # PDF viewer (with a Download option).
                action = ("statpdf", statutes_at_large.url_for(m))
            elif kind == "engrep":
                # English Reports cite ("156 Eng. Rep. 145") → CommonLII scan.
                action = ("engrep", eng_rep.cite_spec(m))
            elif kind == "engrepn":
                # Nominate-report cite ("9 Exch. 341") → same viewer; m is the
                # pre-built, resolution-gated spec ("n:exch:9:341").
                action = ("engrep", m)
            elif kind == "fedcas":
                # Federal Cases case number → CourtListener lookup at click
                # time; m is the pre-built JSON spec ({"no", "name"}).
                action = ("fedcas", m)
            else:
                action = ("cfr", ecfr.cite_spec(m))
            if action is None:
                id_start = txt.index("end-1c")
                txt.insert("end", text[start:end], tags)  # plain text
                la = self._last_cite_action
                if (kind == "idcite" and m.group(1) is None
                        and la and la[0] == "cite"):
                    # A bare "Id." pointing at a case: render it plain for now
                    # and remember where, so it's linked from here only if its
                    # "at N" (in the next span) is in range (see _pending_id).
                    self._pending_id = (id_start, txt.index("end-1c"), la[1])
                else:
                    self._pending_id = None
            else:
                link_tag = self._new_link(action)
                txt.insert("end", text[start:end], tags + ("citelink", link_tag))
                self._pending_id = None
                # Remember this citation so a following "Id." points to it.
                if kind in ("cite", "shortcite"):
                    self._last_cite_action = ("cite", cite_base)
                elif kind != "idcite":
                    self._last_cite_action = action
            pos = end
        if pos < len(text):
            tail = text[pos:]
            txt.insert("end", tail, tags)
            # Real text after a pin-less "Id." means its pin isn't coming.
            if re.search(r"[A-Za-z0-9]", tail):
                self._pending_id = None

    def _render_footnotes(self, footnotes: list, part_tag: Optional[str]) -> None:
        """Insert a part's footnote blocks, recording each note's rendered
        region and number so copied selections can be pin-cited (page n.N)."""
        txt = self._text
        open_region: Optional[list] = None  # [start_index, note_number, page]

        def close_region() -> None:
            nonlocal open_region
            if open_region is not None:
                self._fn_regions.append(
                    (open_region[0], txt.index("end-1c"),
                     open_region[1], open_region[2])
                )
                open_region = None

        last_fid: Optional[str] = None
        for block in footnotes:
            first = block.spans[0] if block.spans else None
            num = ""
            page: Optional[int] = None
            if first is not None and first.fndef:
                num = first.text.strip().strip("[]")
                page = self._fnref_pages.get(first.fndef)
            else:
                body_text = "".join(
                    s.text for s in block.spans if not s.pagenum
                ).lstrip()
                m = _FN_BODY_MARK_RE.match(body_text)
                if m:
                    num = (m.group(1) or m.group(2) or "").strip()
            # Record the note's text, keyed by its anchor id, for hover tips.
            body = re.sub(
                r"\s+", " ",
                "".join(s.text for s in block.spans if not s.pagenum),
            ).strip()
            if first is not None and first.fndef:
                last_fid = first.fndef
                self._fn_text[last_fid] = body
            elif last_fid is not None and body:
                self._fn_text[last_fid] += " " + body
            if num:
                close_region()
                open_region = [txt.index("end-1c"), num, page]
            self._insert_block(block, part_tag)
        close_region()

    def _insert_block(self, block, part_tag: Optional[str]) -> None:
        if block.kind == "center":
            block_tags: tuple = ("center",)
        elif block.kind == "blockquote":
            block_tags = ("blockquote",)
        elif block.kind == "heading":
            block_tags = ("heading",)
        else:
            block_tags = ()
        if part_tag:
            block_tags = block_tags + (part_tag,)
        # The reporter citation lines at the top of a SCOTUS opinion ("512
        # U.S. 477 (1994)") are reference scaffolding, not the Court's prose,
        # so keep them out of the Century Schoolbook body face.
        neutral = self._is_scotus and block.kind in ("center", "heading") and bool(
            _CITE_ONLY_LINE_RE.match(
                re.sub(r"\s+", " ",
                       "".join(s.text for s in block.spans if not s.pagenum)).strip()
            )
        )
        # English Reports, Federal Appendix, and unpublished federal opinions
        # render as one of our links, replacing any Scholar link inside it.
        link_ranges = _special_citation_ranges(
            block.spans, self._recap_spec_index)
        if link_ranges:
            self._insert_spans_with_links(block, block_tags, neutral, link_ranges)
        else:
            self._insert_spans_grouped(block.spans, block_tags, neutral)
        self._text.insert("end", "\n\n", block_tags)

    def _insert_spans_grouped(self, spans, block_tags: tuple,
                              neutral: bool) -> None:
        """Render a block's spans, giving every span of one Google Scholar case
        hyperlink — which Scholar often splits across italic/roman runs ("<i>
        Calder</i> v. <i>Bull,</i> 3 Dall. 386") — a single shared click action
        computed from the whole link text, so clicking the case name, the
        reporter, or anywhere in the link follows it and falls back identically.
        Non-link spans render one at a time as before."""
        def is_case_link(s) -> bool:
            return bool(s.link) and not (s.pagenum or s.fnref or s.fndef)

        i, n = 0, len(spans)
        while i < n:
            if is_case_link(spans[i]):
                href = spans[i].link
                j = i + 1
                while j < n and is_case_link(spans[j]) and spans[j].link == href:
                    j += 1
                full_text = "".join(s.text for s in spans[i:j])
                link_tag = self._new_link(
                    self._scholar_link_action(full_text, href)
                )
                for s in spans[i:j]:
                    self._insert_span(s, block_tags, neutral=neutral,
                                      link_tag=link_tag)
                i = j
            else:
                self._insert_span(spans[i], block_tags, neutral=neutral)
                i += 1

    def _insert_spans_with_links(self, block, block_tags: tuple,
                                 neutral: bool, ranges: list) -> None:
        """Render a block whose text contains citation runs we link ourselves
        (English Reports → CommonLII scan, Federal Appendix → case.law PDF): text
        inside a run gets one shared link (Scholar links dropped); everything
        outside renders exactly as it normally would."""
        range_tag: list = [None] * len(ranges)  # one link tag per run
        pos = 0
        for span in block.spans:
            s_start = pos
            s_end = pos + len(span.text)
            pos = s_end
            # Page/footnote markers never overlap a citation run — render whole.
            if span.pagenum or span.fnref or span.fndef or not span.text:
                self._insert_span(span, block_tags, neutral=neutral)
                continue
            cur = s_start
            while cur < s_end:
                ri = next((i for i, (rs, re_, _a) in enumerate(ranges)
                           if rs <= cur < re_), None)
                if ri is None:
                    nxt = min([s_end] + [rs for rs, _e, _a in ranges
                                         if rs > cur])
                    seg = span.text[cur - s_start: nxt - s_start]
                    if seg:
                        self._insert_span(_dc_replace(span, text=seg),
                                          block_tags, neutral=neutral)
                    cur = nxt
                else:
                    rs, re_, action = ranges[ri]
                    seg_end = min(s_end, re_)
                    seg = span.text[cur - s_start: seg_end - s_start]
                    if seg:
                        if range_tag[ri] is None:
                            range_tag[ri] = self._new_link(action)
                        self._insert_linked_segment(seg, span, block_tags,
                                                    neutral, range_tag[ri])
                    cur = seg_end

    def _insert_linked_segment(self, text: str, span, block_tags: tuple,
                               neutral: bool, link_tag: str) -> None:
        """Insert one piece of a citation run with the shared link, preserving
        the span's own formatting."""
        tags = list(block_tags)
        tags.append(self._font_tag(
            span.italic, span.bold, span.small, span.sup,
            family="Georgia" if neutral else None,
        ))
        if span.underline:
            tags.append("underline")
        tags += ["citelink", link_tag]
        self._text.insert("end", text, tuple(tags))
        # A following "Id." should resolve to this E.R. case, not the last
        # plain-text cite.
        self._last_cite_action = self._link_actions.get(
            link_tag, self._last_cite_action)

    def _clear_part_region_marks(self) -> None:
        txt = getattr(self, "_text", None)
        names = list(getattr(self, "_part_region_marks", []))
        self._part_region_marks = []
        if txt is None:
            return
        for name in names:
            try:
                txt.mark_unset(name)
            except tk.TclError:
                pass

    def _record_part_region(self, start: str, end: str, part_index: int) -> None:
        txt = self._text
        serial = len(self._part_region_marks) // 2
        start_mark = f"part_region_{serial}_start"
        end_mark = f"part_region_{serial}_end"
        txt.mark_set(start_mark, start)
        txt.mark_gravity(start_mark, "left")
        txt.mark_set(end_mark, end)
        # Text.insert("end", ...) inserts at the same boundary later parts use.
        # Left gravity keeps a finished part from absorbing the next separate
        # opinion, which would duplicate dissents/concurrences during export.
        txt.mark_gravity(end_mark, "left")
        self._part_region_marks.extend([start_mark, end_mark])
        self._part_regions.append((start_mark, end_mark, part_index))

    def _part_region_indices(self) -> list[tuple[str, str, int]]:
        txt = getattr(self, "_text", None)
        if txt is None:
            return []
        regions: list[tuple[str, str, int]] = []
        for start_mark, end_mark, part_index in getattr(self, "_part_regions", []):
            try:
                start = txt.index(start_mark)
                end = txt.index(end_mark)
                if txt.compare(start, "<", end):
                    regions.append((start, end, part_index))
            except tk.TclError:
                continue
        return regions

    def _render_scholar(self) -> None:
        txt = self._text
        txt.config(state="normal")
        self._clear_part_region_marks()
        txt.delete("1.0", "end")
        self._hide_fn_tip()
        self._link_actions.clear()
        self._fn_text.clear()
        self._fnref_pages: dict[str, Optional[int]] = {}
        self._fn_regions: list[tuple[str, str, str, Optional[int]]] = []
        self._part_regions: list[tuple[str, str, int]] = []
        self._rendered_parts = self._parts  # parts list _part_regions indexes
        self._scroll_part: Optional[int] = None
        self._fn_ref_pos: dict[str, str] = {}  # footnote id → in-text marker index
        self._fn_def_pos: dict[str, str] = {}  # footnote id → body marker index
        self._page_pos: dict[int, str] = {}    # star page → start index
        self._cur_page: Optional[int] = None
        self._short_cite_index = _build_short_cite_index(self._scholar_text)
        self._recap_spec_index = _recap_spec_index(self._scholar_text)
        self._last_cite_action = None
        self._pending_id = None
        self._const_linked = set()
        # Keep the part selector in step with the parts being rendered — the
        # Scholar parts may differ from the CourtListener ones when this window
        # opened on CL and later switched to a found Scholar match.
        self._part_combo.config(values=["Full opinion"] + [
            f"{i + 1}. {p.label}" for i, p in enumerate(self._parts)
        ])
        if (self._current_part is not None
                and self._current_part >= len(self._parts)):
            self._current_part = None  # CL part index out of range for Scholar
            self._part_combo.current(0)
        if not self._parts:
            txt.insert("1.0", self._scholar_text)
        else:
            if self._current_part is None:
                shown = list(enumerate(self._parts))
            else:
                shown = [(self._current_part, self._parts[self._current_part])]
            prev_kind: Optional[str] = None
            for pi, part in shown:
                # A white separator line between two adjacent same-kind tinted
                # parts (two dissents, or two concurrences) so they read as
                # separate opinions rather than one big coloured block.
                if (self._current_part is None
                        and part.kind in self._PART_BOX_TAGS
                        and part.kind == prev_kind):
                    txt.insert("end", "\n")
                part_start = txt.index("end-1c")
                if self._part_start_pages:
                    self._cur_page = self._part_start_pages[pi] or self._cur_page
                for block in part.blocks:
                    self._insert_block(block, None)  # body text stays black
                if part.footnotes:
                    txt.insert("end", "Footnotes\n\n", ("fnhead",))
                    self._render_footnotes(part.footnotes, None)
                part_end = txt.index("end-1c")
                self._record_part_region(part_start, part_end, pi)
                if self._current_part is None:
                    box = self._PART_BOX_TAGS.get(part.kind)
                    if box:  # light tint behind concurrences/dissents
                        txt.tag_add(box, part_start, part_end)
                prev_kind = part.kind
        txt.config(state="disabled")
        self._mode = "scholar"
        self._source_var.set(self._scholar_url)
        # From the Scholar view, offer the official PDF (the CourtListener text
        # is invariably worse, so it's no longer offered here).
        _style_ui_button(self._toggle_btn, primary=True)
        self._toggle_btn.configure(
            text="Finding PDF...", command=self._view_pdf, state="disabled",
        )
        self._hide_pdf_button()  # Scholar view uses the toggle for the PDF
        self._show_cl_button()   # …and offers a switch to the CourtListener text
        self._export_btn.configure(text="Export ▾", command=self._post_export_menu)
        self._print_btn.pack_forget()  # text view: no Print button
        self._zoom_out_btn.configure(text="A−")
        self._zoom_in_btn.configure(text="A+")
        if len(self._parts) > 1:
            self._part_combo.config(state="readonly")
        if self._current_part is None:
            self._view_label_var.set("Full opinion")
            self._set_view_color("black")
        else:
            part = self._parts[self._current_part]
            self._view_label_var.set(part.label)
            self._set_view_color(
                self._PART_LABEL_COLORS.get(part.kind, "black"))
        extra = f" | {self._note}" if self._note else ""
        self._status_var.set(
            f"{len(self._scholar_text):,} characters | Google Scholar version{extra}"
        )
        self._finder.refresh()
        self._refresh_pdf_button()
        self._locate_pdf()
        self._refresh_outline_panel()
        self._schedule_text_justify()
        self._schedule_gutter_redraw()

    def _on_part_selected(self, _event=None) -> None:
        idx = self._part_combo.current()
        self._current_part = None if idx <= 0 else idx - 1
        if self._cl_primary or self._mode == "courtlistener":
            self._render_cl_blocks()
        else:
            self._render_scholar()

    def _on_yscroll(self, first: str, last: str) -> None:
        """Keep the scrollbar in sync and, in the full-opinion view, colour the
        top-of-window label to name the part now at the top of the page."""
        vsb = getattr(self, "_vsb", None)
        if vsb is not None:
            vsb.set(first, last)
        self._update_scroll_part()
        self._draw_page_column()

    def _update_scroll_part(self) -> None:
        """In the full-opinion view, name+colour the part at the top of the
        viewport (so scrolling into a concurrence/dissent colours the header).
        A single selected part keeps its fixed label, so this no-ops there."""
        if getattr(self, "_current_part", None) is not None:
            return
        parts = getattr(self, "_rendered_parts", None)
        regions = self._part_region_indices()
        if not parts or not regions:
            return
        txt = self._text
        try:
            top = txt.index("@0,0")
        except tk.TclError:
            return
        pi = None
        for rs, rend, p in regions:
            if txt.compare(top, ">=", rs) and txt.compare(top, "<", rend):
                pi = p
                break
        if pi is None or pi == getattr(self, "_scroll_part", None):
            return
        self._scroll_part = pi
        kind = parts[pi].kind
        if kind in ("concurrence", "dissent", "separate"):
            self._view_label_var.set(parts[pi].label)
            self._set_view_color(
                self._PART_LABEL_COLORS.get(kind, "black"))
        else:
            self._view_label_var.set("Full opinion")
            self._set_view_color("black")

    # ------------------------------------------------------------------
    # Text justification
    # ------------------------------------------------------------------

    def _on_text_configure(self) -> None:
        """Reflow display-line justification and redraw side gutters on resize."""
        self._schedule_text_justify()
        self._schedule_gutter_redraw()

    def _justification_enabled(self) -> bool:
        var = getattr(self, "_justify_text", None)
        try:
            return bool(var is not None and var.get())
        except tk.TclError:
            return False

    def _on_justify_toggle(self) -> None:
        if self._justification_enabled():
            self._schedule_text_justify()
        else:
            self._clear_justification()
            self._schedule_gutter_redraw()

    def _schedule_text_justify(self) -> None:
        """Justify opinion text after Tk has recalculated display lines.

        Tk's Text widget supports left, right, and centered paragraphs, but not
        newspaper-style full justification.  The standard workaround is to add
        small runs of extra spaces to each wrapped display line and then rebuild
        those padding spaces whenever the widget is resized.  The spaces are
        tagged so they can be removed before each recalculation.
        """
        if getattr(self, "_justify_pending", False):
            return
        if getattr(self, "_mode", None) == "pdf":
            return
        if not self._justification_enabled():
            self._clear_justification()
            return
        self._justify_pending = True

        def run() -> None:
            self._justify_pending = False
            self._justify_display_lines()
            self._schedule_gutter_redraw()

        try:
            self._win.after_idle(run)
        except tk.TclError:
            self._justify_pending = False

    def _clear_justification(self) -> None:
        txt = self._text
        try:
            old_state = str(txt.cget("state"))
        except tk.TclError:
            return
        try:
            if old_state != "normal":
                txt.config(state="normal")
            ranges = list(txt.tag_ranges(self._JUSTIFY_PAD_TAG))
            for start, end in zip(ranges[-2::-2], ranges[-1::-2]):
                txt.delete(start, end)
            txt.tag_remove(self._JUSTIFY_HIDE_TAG, "1.0", "end")
        finally:
            try:
                if old_state != "normal":
                    txt.config(state=old_state)
            except tk.TclError:
                pass

    def _line_has_any_tag(self, start: str, end: str, names: tuple[str, ...]) -> bool:
        txt = self._text
        for name in names:
            if name in txt.tag_names(start):
                return True
            try:
                before_end = txt.index(f"{end} -1c")
                if (txt.compare(before_end, ">=", start)
                        and name in txt.tag_names(before_end)):
                    return True
            except tk.TclError:
                pass
            ranges = txt.tag_nextrange(name, start, end)
            if ranges:
                return True
        return False

    def _hyphen_points(self, word: str) -> list[int]:
        """Conservative English-ish syllable break candidates for layout only."""
        if (len(word) < self._HYPHEN_MIN_WORD
                or not word.isascii()
                or not word.isalpha()
                or not any(c.islower() for c in word)):
            return []

        lower = word.lower()
        vowel_groups: list[tuple[int, int]] = []
        i, n = 0, len(lower)
        while i < n:
            if lower[i] not in self._VOWELS:
                i += 1
                continue
            start = i
            while i < n and lower[i] in self._VOWELS:
                i += 1
            vowel_groups.append((start, i))

        onsets = {
            "bl", "br", "ch", "cl", "cr", "dr", "fl", "fr", "gh", "gl",
            "gr", "ph", "pl", "pr", "qu", "sc", "sh", "sk", "sl", "sm",
            "sn", "sp", "st", "str", "sw", "th", "tr", "tw", "wh", "wr",
        }
        points: list[int] = []
        for (_v_start, v_end), (next_v_start, _next_v_end) in zip(
                vowel_groups, vowel_groups[1:]):
            cluster = lower[v_end:next_v_start]
            if not cluster:
                continue
            if len(cluster) == 1:
                next_piece = lower[v_end:]
                if next_piece.startswith((
                        "ci", "gi", "si", "ti", "tu", "cial", "sion",
                        "tial", "tian", "tion", "tious", "tive", "tory",
                        "ture")):
                    candidates = (v_end,)
                else:
                    candidates = (next_v_start,)
            else:
                point = v_end + 1
                for off in range(1, len(cluster)):
                    if cluster[off:] in onsets:
                        point = v_end + off
                        break
                candidates = (point,)
            for point in candidates:
                if (self._HYPHEN_MIN_PREFIX <= point
                        and n - point >= self._HYPHEN_MIN_SUFFIX):
                    points.append(point)
        return sorted(set(points))

    def _hyphenate_next_word(self, line_end: str, used: float, width: int,
                             space_px: int) -> bool:
        """Borrow a syllable-like prefix from the next wrapped word.

        The original prefix is hidden, not deleted; the visible prefix, hyphen,
        and forced line break are tagged as temporary justification characters
        and are removed on the next reflow.
        """
        txt = self._text
        try:
            scan = txt.index("__justify_next")
            line_limit = txt.index(f"{scan} lineend")
        except tk.TclError:
            return False
        following = txt.get(scan, line_limit)
        m = re.match(r"\s*([A-Za-z]{%d,})\b" % self._HYPHEN_MIN_WORD,
                     following)
        if not m:
            return False

        word = m.group(1)
        word_start = txt.index(f"{scan}+{m.start(1)}c")
        word_end = txt.index(f"{word_start}+{len(word)}c")
        if self._line_has_any_tag(
                word_start, word_end,
                ("center", "heading", "blockquote", "pagenum", "fnhead")):
            return False

        available = width - used - self._HYPHEN_SAFETY_PX
        if available <= space_px:
            return False
        best: Optional[int] = None
        gap_px = space_px if m.start(1) else 0
        for point in self._hyphen_points(word):
            fragment = word[:point] + "-"
            if gap_px + self._fonts["base"].measure(fragment) <= available:
                best = point
            else:
                break
        if best is None:
            return False

        fragment = word[:best]
        tags = tuple(
            t for t in txt.tag_names(word_start)
            if t not in ("sel", self._JUSTIFY_HIDE_TAG)
        )
        txt.insert(word_start, fragment + "-\n",
                   tags + (self._JUSTIFY_PAD_TAG,))
        hidden_start = txt.index(f"{word_start}+{len(fragment) + 2}c")
        hidden_end = txt.index(f"{hidden_start}+{best}c")
        txt.tag_add(self._JUSTIFY_HIDE_TAG, hidden_start, hidden_end)
        txt.mark_set("__justify_next", hidden_end)
        txt.mark_gravity("__justify_next", "right")
        return True

    def _pad_display_line_to_margin(self, start: str, end: str, used: float,
                                    width: int, space_px: int) -> None:
        """Apply the original extra-space justification to one display line."""
        txt = self._text
        line_text = txt.get(start, end)
        space_offsets = [
            m.start() for m in re.finditer(r"(?<=\S) (?=\S)", line_text)
        ]
        if not space_offsets:
            return
        need = max(0, width - used)
        # Leave a small safety margin so approximate font measurements do not
        # push the line onto the next wrap.
        extra = max(0, (need // space_px) - 1)
        if not extra:
            return
        per, rem = divmod(extra, len(space_offsets))
        # Insert from right to left so offsets remain valid.
        for n, off in enumerate(reversed(space_offsets)):
            add = per + (1 if n < rem else 0)
            if add:
                txt.insert(f"{start}+{off + 1}c", " " * add,
                           (self._JUSTIFY_PAD_TAG,))

    def _is_filled_hard_break(self, line_end: str, used: float,
                              width: int, space_px: int) -> bool:
        """Whether a display line that reaches its logical end — a <br>/source
        hard break rather than a soft wrap — should still be justified.

        Only a line the source *filled* and that the same paragraph continues
        past is stretched to the margin.  A paragraph's last line, a line
        standing on its own, or any line onto which the next line's first word
        would still have fit is left ragged-right so short and final lines keep
        their natural left (or centered) alignment instead of being spread out.
        """
        txt = self._text
        next_start = txt.index(f"{line_end} +1c")
        if txt.compare(next_start, ">=", "end-1c"):
            return False  # nothing follows: last line of the document
        next_text = txt.get(next_start, f"{next_start} lineend")
        if not next_text.strip():
            return False  # a blank line ends the paragraph: this is its last line
        first_word = next_text.split(None, 1)[0]
        remaining = width - used
        threshold = (
            self._fonts["base"].measure(first_word)
            + self._JUSTIFY_HARD_BREAK_EXTRA_SPACES * space_px
        )
        return remaining < threshold

    def _justify_display_lines(self) -> None:
        txt = self._text
        if not self._justification_enabled():
            self._clear_justification()
            return
        try:
            old_state = str(txt.cget("state"))
        except tk.TclError:
            return
        try:
            txt.config(state="normal")
            self._clear_justification()
            txt.update_idletasks()
            width = txt.winfo_width() - int(txt.cget("padx")) * 2 - 4
            if width <= 100:
                return
            space_px = max(self._fonts["base"].measure(" "), 1)
            idx = "1.0"
            while txt.compare(idx, "<", "end-1c"):
                try:
                    line_end = txt.index(f"{idx} display lineend")
                    txt.mark_set("__justify_next", f"{line_end} +1c")
                    txt.mark_gravity("__justify_next", "right")
                except tk.TclError:
                    break
                logical_end = txt.index(f"{idx} lineend")
                line_text = txt.get(idx, line_end)
                used = self._fonts["base"].measure(line_text)
                is_wrapped_line = txt.compare(line_end, "<", logical_end)
                # Full justification stretches every line of a paragraph except
                # its last one, and never a line standing on its own.  A
                # soft-wrapped display line is always a mid-paragraph line, so it
                # is justified.  A line that reaches its logical end is a
                # paragraph's last line, a lone line, or a <br>/source hard break;
                # only a filled hard break the same paragraph runs on past is
                # justified — last and short lines stay ragged-right (their
                # natural left or centered alignment).
                if is_wrapped_line:
                    should_justify = True
                else:
                    should_justify = self._is_filled_hard_break(
                        line_end, used, width, space_px)
                if (should_justify
                        and not self._line_has_any_tag(
                            idx, line_end,
                            ("center", "heading", "blockquote", "pagenum", "fnhead"))):
                    # Step 1: if the next word can be split cleanly, pull a
                    # syllable-like prefix up so this line starts closer to the
                    # right margin.  Step 2 below still runs the original
                    # extra-space justification on the resulting display line.
                    if is_wrapped_line and self._hyphenate_next_word(
                            line_end, used, width, space_px):
                        try:
                            line_end = txt.index(f"{idx} display lineend")
                            used = self._fonts["base"].measure(
                                txt.get(idx, line_end))
                        except tk.TclError:
                            pass
                    self._pad_display_line_to_margin(
                        idx, line_end, used, width, space_px)
                idx = txt.index("__justify_next")
        finally:
            try:
                txt.config(state=old_state)
            except tk.TclError:
                pass

    # ------------------------------------------------------------------
    # Side gutters: left page-number column and right concurrence/dissent map
    # ------------------------------------------------------------------

    def _ypixels(self, index: str) -> int:
        """Vertical offset (px) of `index` from the top of the document.
        Tolerates Tk's `count` returning either an int or a 1-tuple."""
        try:
            r = self._text.count("1.0", index, "ypixels")
        except tk.TclError:
            return 0
        if r is None:
            return 0
        return r[0] if isinstance(r, (tuple, list)) else int(r)

    def _schedule_gutter_redraw(self) -> None:
        """Redraw both gutters once the text has settled (after a resize, font
        change, or fresh render)."""
        if getattr(self, "_gutter_redraw_pending", False):
            return
        self._gutter_redraw_pending = True

        def run() -> None:
            self._gutter_redraw_pending = False
            self._draw_page_column()
            self._draw_part_map()

        try:
            self._win.after_idle(run)
        except tk.TclError:
            self._gutter_redraw_pending = False

    def _draw_page_column(self) -> None:
        """Draw the reporter page numbers (bold black) in the left gutter, each
        aligned to the screen line where its star-pagination marker sits.  Only
        the currently visible pages are drawn (it scrolls with the text)."""
        canvas = getattr(self, "_pagecol", None)
        if canvas is None:
            return
        page_pos = getattr(self, "_page_pos", None) or {}
        if self._mode == "pdf" or not page_pos:
            canvas.delete("all")
            canvas.config(width=1)
            return
        canvas.config(width=self._PAGECOL_W)
        canvas.delete("all")
        txt = self._text
        w = self._PAGECOL_W
        seen_y: set[int] = set()
        for page, idx in page_pos.items():
            try:
                di = txt.dlineinfo(idx)
            except tk.TclError:
                di = None
            if not di:  # page not on screen right now
                continue
            y = di[1] + di[3] // 2  # vertical centre of that display line
            if y in seen_y:
                continue
            seen_y.add(y)
            canvas.create_text(w - 5, y, anchor="e", text=str(page),
                               fill="black", font=self._pagecol_font)

    def _draw_part_map(self) -> None:
        """Draw a colour-coded strip on the right marking where each
        majority or separate writing begins, with its label.  Shown only in the
        full-opinion text view; clicking a marker jumps to that part."""
        canvas = getattr(self, "_partmap", None)
        if canvas is None:
            return
        self._partmap_rows = []
        canvas.delete("all")
        parts = getattr(self, "_rendered_parts", None)
        regions = self._part_region_indices()
        if (self._mode == "pdf" or getattr(self, "_current_part", None) is not None
                or not parts or not regions):
            canvas.config(width=0)
            return
        marks = [
            (rs, parts[p].kind, parts[p].label)
            for rs, _re, p in regions
            if parts[p].kind in ("majority", "concurrence", "dissent", "separate")
        ]
        if not marks:
            canvas.config(width=0)
            return
        txt = self._text
        total = self._ypixels("end-1c")
        if not total:
            canvas.config(width=0)
            return
        canvas.config(width=self._PARTMAP_W)
        try:
            h = canvas.winfo_height() or txt.winfo_height()
        except tk.TclError:
            h = txt.winfo_height()
        w = self._PARTMAP_W
        # Ideal vertical position of each marker (proportional to where the part
        # begins in the document).
        top, bot = 6, max(6, h - 6)
        ys = [max(top, min(bot, int(self._ypixels(rs) / total * h)))
              for rs, _kind, _label in marks]
        # Several short separate opinions clustered at the end would overlap, so
        # enforce a minimum gap: push collisions down, and if that runs off the
        # bottom, pack the run upward from the bottom edge.  The markers then no
        # longer line up exactly with the text, but every label stays readable.
        gap = self._partmap_font.metrics("linespace") + 3
        for i in range(1, len(ys)):
            if ys[i] - ys[i - 1] < gap:
                ys[i] = ys[i - 1] + gap
        if ys and ys[-1] > bot:
            ys[-1] = bot
            for i in range(len(ys) - 2, -1, -1):
                if ys[i + 1] - ys[i] < gap:
                    ys[i] = ys[i + 1] - gap
            ys[0] = max(ys[0], top)
        for (rs, kind, label), y in zip(marks, ys):
            color = self._PARTMAP_COLORS.get(kind, "black")
            canvas.create_line(2, y, w - 2, y, fill=color, width=2)
            canvas.create_rectangle(2, y, 8, y + 10, fill=color, outline=color)
            short = self._partmap_short_label(label, kind)
            tid = canvas.create_text(11, y + 1, anchor="nw", text=short,
                                     fill=color, font=self._partmap_font,
                                     width=w - 13)
            bbox = canvas.bbox(tid)
            y2 = bbox[3] if bbox else y + 14
            self._partmap_rows.append((y, y2, rs))

    @staticmethod
    def _partmap_short_label(label: str, kind: str) -> str:
        """A compact label for the narrow part-map strip.  For separate
        opinions just the author's surname (the colour conveys its known or
        neutral role); for the main opinion 'Opinion
        (Author)'."""
        text = re.sub(r"\s+", " ", label or "").strip()
        # Per curiam opinions: the parenthetical should read "(per curiam)", not
        # the truncated "(Per)" the surname extraction below would produce.
        if re.search(r"per\s+curiam", text, re.IGNORECASE):
            return "Opinion (per curiam)" if kind == "majority" else "per curiam"
        m = (re.search(r"\b(?:[Cc]hief\s+)?(?:JUSTICE|Justice)\s+"
                       r"([A-Z][A-Za-z'’.]+)", text)
             or re.search(r"\b([A-Z][A-Za-z'’]{2,}),\s*(?:C\.\s*)?J\.", text)
             or re.search(r"\(([A-Z][A-Za-z'’.]+)", text))
        name = None
        if m:
            name = m.group(1).rstrip(".")
            if name.isupper():  # THOMAS → Thomas
                name = name[:1] + name[1:].lower()
        if kind == "majority":
            return f"Opinion ({name})" if name else "Opinion"
        # Dissent / concurrence: surname alone (colour distinguishes the two).
        if name is None:  # last resort: any capitalised token
            m2 = re.search(r"\b([A-Z][A-Za-z'’]{2,})\b", text)
            if m2:
                name = m2.group(1)
                if name.isupper():
                    name = name[:1] + name[1:].lower()
        fallback = {
            "dissent": "Dissent", "concurrence": "Concurrence",
            "separate": "Separate opinion",
        }
        return name or fallback.get(kind, "Opinion")

    def _on_partmap_click(self, event) -> None:
        for y1, y2, rs in getattr(self, "_partmap_rows", []):
            if y1 - 4 <= event.y <= y2 + 4:
                try:
                    self._text.see(rs)
                    self._text.yview(rs)
                except tk.TclError:
                    pass
                self._draw_page_column()
                return

    def _render_cl_blocks(self) -> None:
        """Render CourtListener opinion parts with full block formatting."""
        parts = self._cl_parts or self._parts
        # Update part selector to reflect CL parts
        part_values = ["Full opinion"] + [
            f"{i + 1}. {p.label}" for i, p in enumerate(parts)
        ]
        self._part_combo.config(values=part_values)
        if self._current_part is None:
            self._part_combo.current(0)
        txt = self._text
        txt.config(state="normal")
        self._clear_part_region_marks()
        txt.delete("1.0", "end")
        self._hide_fn_tip()
        self._link_actions.clear()
        self._fn_text.clear()
        self._fnref_pages: dict[str, Optional[int]] = {}
        self._fn_regions: list[tuple[str, str, str, Optional[int]]] = []
        self._part_regions: list[tuple[str, str, int]] = []
        self._rendered_parts = parts  # parts list _part_regions indexes
        self._scroll_part: Optional[int] = None
        self._fn_ref_pos: dict[str, str] = {}
        self._fn_def_pos: dict[str, str] = {}
        self._page_pos: dict[int, str] = {}
        self._cur_page: Optional[int] = None
        self._short_cite_index = _build_short_cite_index(
            self._scholar_text or self._cl_text or "")
        self._recap_spec_index = _recap_spec_index(
            self._scholar_text or self._cl_text or "")
        self._last_cite_action = None
        self._pending_id = None
        self._const_linked = set()
        if not parts:
            self._insert_plain_with_links(self._cl_text or "(no text)", ())
        else:
            if self._current_part is None:
                shown = list(enumerate(parts))
            else:
                shown = [(self._current_part, parts[self._current_part])]
            prev_kind: Optional[str] = None
            for pi, part in shown:
                # White separator between two adjacent same-kind tinted parts.
                if (self._current_part is None
                        and part.kind in self._PART_BOX_TAGS
                        and part.kind == prev_kind):
                    txt.insert("end", "\n")
                part_start = txt.index("end-1c")
                for block in part.blocks:
                    self._insert_block(block, None)  # body text stays black
                if part.footnotes:
                    txt.insert("end", "Footnotes\n\n", ("fnhead",))
                    self._render_footnotes(part.footnotes, None)
                part_end = txt.index("end-1c")
                self._record_part_region(part_start, part_end, pi)
                if self._current_part is None:
                    box = self._PART_BOX_TAGS.get(part.kind)
                    if box:  # light tint behind concurrences/dissents
                        txt.tag_add(box, part_start, part_end)
                prev_kind = part.kind
        txt.config(state="disabled")
        self._mode = "courtlistener"
        source = self._primary_source_url or (
            "CourtListener (REST API)"
            if self._primary_source_kind == "courtlistener"
            else self._primary_source_label
        )
        self._source_var.set(source)
        _style_ui_button(self._toggle_btn, primary=False)
        self._toggle_btn.configure(
            text="Google Scholar Text", command=self._toggle_source,
            state="normal" if self._scholar_url else "disabled",
        )
        self._export_btn.configure(text="Export ▾", command=self._post_export_menu)
        self._print_btn.pack_forget()
        self._zoom_out_btn.configure(text="A−")
        self._zoom_in_btn.configure(text="A+")
        if len(parts) > 1:
            self._part_combo.config(state="readonly")
        else:
            self._part_combo.config(state="disabled")
        if self._current_part is None:
            self._view_label_var.set("Full opinion")
            self._set_view_color("black")
        else:
            part = parts[self._current_part]
            self._view_label_var.set(part.label)
            self._set_view_color(
                self._PART_LABEL_COLORS.get(part.kind, "black"))
        char_count = len(self._cl_text or self._scholar_text or "")
        self._status_var.set(
            f"{char_count:,} characters | {self._primary_source_label} version"
        )
        self._hide_cl_button()  # CL view uses the toggle for "Google Scholar Text"
        self._show_pdf_button()
        self._finder.refresh()
        self._refresh_outline_panel()
        self._schedule_text_justify()
        self._schedule_gutter_redraw()

    def _show_courtlistener(self) -> None:
        if self._cl_parts or self._cl_blocks:
            self._render_cl_blocks()
            return
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        self._insert_plain_with_links(self._cl_text or "(no text)", ())
        txt.config(state="disabled")
        self._mode = "courtlistener"
        source = self._primary_source_url or (
            "CourtListener (assembled from the REST API)"
            if self._primary_source_kind == "courtlistener"
            else self._primary_source_label
        )
        self._source_var.set(source)
        _style_ui_button(self._toggle_btn, primary=False)
        self._toggle_btn.configure(
            text="Google Scholar Text", command=self._toggle_source,
            state="normal" if self._scholar_url else "disabled",
        )
        self._export_btn.configure(text="Export ▾", command=self._post_export_menu)
        self._print_btn.pack_forget()
        self._zoom_out_btn.configure(text="A−")
        self._zoom_in_btn.configure(text="A+")
        self._hide_cl_button()
        self._part_combo.config(state="disabled")
        self._view_label_var.set(f"{self._primary_source_label} text")
        self._set_view_color("black")
        self._status_var.set(
            f"{len(self._cl_text or ''):,} characters | "
            f"{self._primary_source_label} version"
        )
        self._show_pdf_button()
        self._finder.refresh()
        self._refresh_outline_panel()
        self._schedule_text_justify()
        self._schedule_gutter_redraw()

    # ------------------------------------------------------------------
    # Bluebook citation
    # ------------------------------------------------------------------

    def _compute_bluebook_parts(self) -> dict[str, str]:
        item = self._item
        item_name = re.sub(
            r"<[^>]+>", "", item.get("caseName") or item.get("case_name") or ""
        ).strip()
        # The opened opinion owns its caption.  Search-result/API metadata is
        # only a fallback when no caption can be read from the opinion itself.
        name = _scholar_caption_name(self._blocks) or item_name

        # Scholar's header lists each parallel cite on its own line.  When
        # the page has star pagination, pick the reporter the stars follow
        # (the first star falls just past that cite's first page); without
        # stars, prefer a recognized national/regional reporter, the
        # Bluebook default for state cases.
        header = "  ".join(
            b.text() for b in self._blocks[:16] if b.kind in ("center", "heading")
        )
        cands: list[tuple[str, int]] = []
        header_cites: list[str] = []
        header_cite_seen: set[str] = set()
        for b in self._blocks[:16]:
            if b.kind not in ("center", "heading"):
                continue
            t = re.sub(r"\s+", " ", b.text()).strip()
            t = re.sub(r"\bU\.\s+S\.", "U.S.", t)
            t = re.sub(r"\b(\d{1,4})\s+US\s+(\d{1,5})\b", r"\1 U.S. \2", t)
            for m in _HEADER_CITE_SCAN_RE.finditer(t):
                vol, rep, page = m.group(1), m.group(2).strip(" ,"), m.group(3)
                candidate = f"{vol} {rep} {page}"
                key = re.sub(r"\s+", "", candidate).lower()
                if key in header_cite_seen:
                    continue
                header_cite_seen.add(key)
                header_cites.append(candidate)
                if _is_paginable_cite(candidate):
                    cands.append((candidate, int(page)))
        # Every parallel reporter cite printed above the case name — Scholar
        # often lists two or three (U.S., S. Ct., L. Ed.).  Kept so the PDF
        # resolver can try them all before giving up (not just the one chosen
        # for the Bluebook citation below).
        self._header_cites = header_cites
        # The star pagination may follow a reporter that isn't printed in the
        # opinion's own header — CourtListener's combined opinions carry no
        # parallel-cite header at all.  Fold in the cluster's parallel citations
        # so the reporter the pages actually follow can be matched; otherwise
        # the pin cite is computed against the wrong reporter (e.g. an A.3d
        # first page when the stars are really N.J. Reports pages).
        match_cands = list(cands)
        seen = {re.sub(r"\s+", "", c).lower() for c, _p in match_cands}
        for c in item.get("citation", []) or []:
            c = re.sub(r"\s+", " ", str(c)).strip()
            cm = _CITE_PARSE_RE.match(c)
            if not (cm and _is_paginable_cite(c)):
                continue
            key = re.sub(r"\s+", "", c).lower()
            if key in seen:
                continue
            seen.add(key)
            try:
                match_cands.append((c, int(cm.group(3))))
            except ValueError:
                pass
        cite = ""
        first_star: Optional[int] = None
        for b in self._blocks:
            for s in b.spans:
                if s.pagenum:
                    mm = re.search(r"\d+", s.text)
                    if mm:
                        first_star = int(mm.group(0))
                        break
            if first_star is not None:
                break
        if match_cands and first_star is not None:
            # The reporter whose first page the stars fall just past (within a
            # volume's worth of pages) is the one being paginated.  Parallel
            # cites can tie exactly — an early SCOTUS case prints "5 U.S. 299"
            # and "1 Cranch 299", the same page in both — so break ties toward
            # the recognized national reporter ("5 U.S. 299"): it's the
            # Bluebook form, and the nominative form is ambiguous downstream
            # (CourtListener resolves "1 Cranch 299" to two different cases).
            fits = [
                (first_star - p, 0 if _TEXT_CITE_RE.fullmatch(c) else 1, c)
                for c, p in match_cands
                if 0 <= first_star - p <= 400
            ]
            if fits:
                cite = min(fits)[2]
        if not cite and cands:
            cite = next(
                (c for c, _p in cands if _TEXT_CITE_RE.fullmatch(c)),
                cands[0][0],
            )
        if not cite:
            header_norm = re.sub(r"\bU\.\s+S\.", "U.S.", header)
            header_norm = re.sub(
                r"\b(\d{1,4})\s+US\s+(\d{1,5})\b", r"\1 U.S. \2", header_norm
            )
            m = _TEXT_CITE_RE.search(header_norm)
            if m:
                cite = re.sub(r"\s+", " ", m.group(0))
        if not cite:
            cite = _pick_citation(item.get("citation", []))

        item_cites = _cluster_citations_to_strings(item.get("citation", []))
        known_cites = list(header_cites)
        known_seen = {re.sub(r"\s+", "", c).lower() for c in known_cites}
        for candidate in item_cites:
            key = re.sub(r"\s+", "", candidate).lower()
            if key not in known_seen:
                known_seen.add(key)
                known_cites.append(candidate)

        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        year = date_filed[:4] if len(date_filed) >= 4 else ""
        # Year candidates from the header, most reliable first.  Page markers
        # are excluded before scanning: a star page ("*2066 Syllabus" in
        # Cedar Point) or a bare S. Ct. page number is indistinguishable from
        # a year (S. Ct. pages run 1600–2099 through much of a term), and a
        # body heading inside the first blocks can carry a stray date ("A.
        # December 20, 2013 Suppression Hearing").  A year in parentheses
        # after a citation ("323 U.S. 214 (1944)") is the decision year; a
        # "Decided June 23, 2021." line is next; a bare year is last.
        hdr_clean = "  ".join(
            "".join(s.text for s in b.spans if not s.pagenum)
            for b in self._blocks[:16] if b.kind in ("center", "heading")
        )
        # Lower-court headers date the decision with a bare centered
        # "February 27, 2026." line (no "Decided" keyword).  Only *center*
        # blocks count for that form: a body section heading can carry a
        # full date that is not the decision's ("A. December 20, 2013
        # Suppression Hearing").
        hdr_center = "  ".join(
            "".join(s.text for s in b.spans if not s.pagenum)
            for b in self._blocks[:16] if b.kind == "center"
        )
        header_years = (
            re.findall(r"\([^()]{0,40}?(1[6-9]\d{2}|20\d{2})\s*\)", hdr_clean)
            or re.findall(
                r"\b(?:Decided|Filed|Released|Entered)\b[^0-9]{0,40}?"
                r"(1[6-9]\d{2}|20\d{2})", hdr_clean, re.IGNORECASE)
            or re.findall(
                r"\b(?:January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+\d{1,2},?\s+"
                r"(1[6-9]\d{2}|20\d{2})\b", hdr_center, re.IGNORECASE)
            or re.findall(r"\b(1[6-9]\d{2}|20\d{2})\b", hdr_clean)
        )
        if not year and header_years:
            year = header_years[-1]

        if not name and self._blocks:
            first = self._blocks[0].text().strip()
            name = re.split(r",\s*\d{1,4}\s", first)[0].strip().rstrip(",")[:120]
        opinion_body = _scholar_body_text(self._blocks)
        name = refine_caption_case(normal_case_caption(name), opinion_body)
        name = simplify_historical_entity_caption(name, opinion_body)
        unresolved_caption_case = caption_case_reference_tokens(
            name, opinion_body)

        court_id = str(item.get("court_id") or "").strip().lower()
        if not court_id:
            court_id = _scholar_court_id(self._blocks)
        is_scotus = "scotus" in court_id or bool(
            re.match(r"\d+\s+(U\.S\.|S\.\s?Ct\.|L\.\s?Ed\.)", cite)
        )
        self._is_scotus = is_scotus
        # CourtListener occasionally supplies the rehearing-denial date as
        # ``date_filed``.  The year printed in a Supreme Court opinion's own
        # citation header is the decision year and therefore controls.
        if is_scotus and header_years:
            year = header_years[-1]
        if not year:
            # The earliest U.S. Reports volumes print no year in the citation
            # line (Scholar renders "2 U.S. 409 (____)"); a centered term
            # line ("February Term, 1803.") carries the decision year.  Only
            # front-matter blocks are trusted: the reporter's statement of
            # the case narrates earlier terms ("AT the December term 1801,
            # William Marbury … moved" — Marbury was decided in 1803), so a
            # prose scan would print the wrong year, which is worse than
            # printing none.
            for b in self._blocks[:16]:
                if b.kind not in ("center", "heading"):
                    continue
                tm = re.search(
                    r"\b(?:January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\s+Term,?\s+"
                    r"(1[6-9]\d{2}|20\d{2})\b", b.text(), re.IGNORECASE)
                if tm:
                    year = tm.group(1)
                    break
        court_abbr = ""
        if not is_scotus:
            fallback = str(item.get("court") or court_id).strip() if court_id else ""
            court_abbr = _court_for_paren(cite, court_id, fallback)
            if not court_abbr and not court_id:
                # A federal district/bankruptcy court read from the Scholar
                # header — ``_scholar_court_id`` leaves those unmapped, but
                # an F. Supp. / B.R. date parenthetical requires the court
                # (rule 10.4(a)): "627 F. Supp. 3d 520 (M.D.N.C. 2022)".
                for b in self._blocks[:8]:
                    if b.kind != "center":
                        continue
                    abbr = _bluebook_federal_trial_court(
                        re.sub(r"\s+", " ", b.text()).strip())
                    if abbr:
                        court_abbr = abbr
                        break
        name = abbreviate_case_name(name)
        cite = _respace_reporter_in_cite(cite)
        display_cite = cite
        omit_parenthetical = ""
        pin_kind = "page"
        if is_scotus:
            display_cite = _nominative_display_cite(cite, known_cites) or cite
        elif first_star is None:
            # With no page markers there is no pin-pagination reason to prefer
            # a regional reporter.  A printed official state reporter in the
            # source header gives a stable citation regardless of whether the
            # case was opened through Scholar or CourtListener.
            official = next((c for c in header_cites if (
                _is_paginable_cite(c)
                and not _REGIONAL_REPORTER_RE.search(f" {c} ")
                and not re.search(r"\s(?:U\.S\.|S\. Ct\.|L\. Ed\.|F\.)\s", f" {c} ")
            )), "")
            if official:
                display_cite = _respace_reporter_in_cite(official)

        if court_id in ("wis", "wisctapp") or court_id.startswith("wis"):
            wi_cite = _wisconsin_display_cite(known_cites)
            if wi_cite:
                display_cite = wi_cite
                omit_parenthetical = "1"
                pin_kind = "paragraph"
        return {
            "name": name, "cite": cite, "display_cite": display_cite,
            "court": court_abbr, "year": year,
            "omit_parenthetical": omit_parenthetical, "pin_kind": pin_kind,
            "_caption_case_unresolved": unresolved_caption_case,
        }

    def _writer_parenthetical(self, part) -> str:
        """
        Bluebook writer parenthetical for a separate opinion (rule 10.6.1):
        "Rehnquist, J., dissenting", "Story, J., separate opinion" when the
        role is unresolved, or "per curiam" for unsigned opinions.
        Empty for the header and signed majority opinions.
        """
        def block_text(b) -> str:
            # The title-comma fix mirrors segmentation's classification view
            # ("Justice, BREYER, concurring." — Alleyne's Scholar text).
            t = fix_title_comma(re.sub(r"\s+", " ", b.text()).strip())
            return re.sub(r"^(?:\*\d+\s+)+", "", t)  # leading page markers

        if part.kind == "majority":
            for b in part.blocks[:3]:
                bt = block_text(b)
                if re.match(r"PER\s+CURIAM\b", bt, re.IGNORECASE):
                    return "per curiam"
                # "JUSTICE O'CONNOR announced the judgment of the Court…" —
                # a lead opinion without a majority (Bluebook rule 10.6.1)
                if re.search(
                    r"announced the judgment of the Court", bt, re.IGNORECASE
                ):
                    return "plurality opinion"
            # CourtListener sub-opinion parts carry the signal in the label.
            if re.search(r"\(per\s+curiam\)", part.label or "", re.IGNORECASE):
                return "per curiam"
            return ""
        if part.kind not in ("concurrence", "dissent", "separate") or not part.blocks:
            return ""
        neutral_role = "separate opinion" if part.kind == "separate" else (
            "dissenting" if part.kind == "dissent" else "concurring")
        t = block_text(part.blocks[0])
        m = re.search(r"\b(?:concurring|dissenting)\b", t, re.IGNORECASE)
        if not m:
            # A CourtListener sub-opinion part: the opinion body has no byline
            # to parse — the type and author live in the label the assembler
            # built from the cluster ("Dissent (Brandeis)", "Concurrence in
            # Part (O'Connor)").  Same data CourtListener uses for its own
            # color-coding.
            lm = re.match(
                r"(Concurrence\s+in\s+Part|Concurrence|Dissent)\b"
                r"(?:.*?\(([^)]+)\))?\s*$",
                part.label or "",
            )
            if not lm:
                sm = re.match(
                    r"Separate\s+opinion\s+of\s+"
                    r"(?:MR\.\s+|MRS\.\s+|MS\.\s+)?(CHIEF\s+)?JUSTICE\s+"
                    r"([A-Z][\w.'’-]+)\s*[.:]?\s*$",
                    part.label or "", re.IGNORECASE,
                )
                if sm:
                    title = "C.J." if sm.group(1) else "J."
                    surname = _fix_name_case(
                        sm.group(2).replace("’", "'").rstrip(".:")
                    )
                    return f"{surname}, {title}, {neutral_role}"
                # A bare attribution heading ("MR. JUSTICE HOLMES:",
                # "Statement of Justice Souter.") never says which way the
                # author voted, so guessing "concurring"/"dissenting" could
                # misdescribe it.  Keep a statement labeled as a statement;
                # otherwise use the neutral "separate opinion" parenthetical.
                jm = re.match(
                    r"(Statement\s+of\s+)?(?:MR\.\s+|MRS\.\s+|MS\.\s+)?"
                    r"(?:CHIEF\s+)?JUSTICE\s+([A-Z][\w.'’-]+?)\s*[.:]?\s*$",
                    part.label or "", re.IGNORECASE,
                )
                if jm:
                    surname = _fix_name_case(jm.group(2).replace("’", "'"))
                    if jm.group(1):
                        return f"statement of {surname}, J."
                    return (f"{surname}, J., {neutral_role}"
                            if part.kind == "separate"
                            else f"opinion of {surname}, J.")
                # Headers that never name the role — the role was read from
                # the opinion's opening lines when the part was segmented, so
                # part.kind is trustworthy here.  A bare state-court byline
                # ("TRAYNOR, J."), a spelled-out one ("CLINTON, Judge."), a
                # joinder byline with no role word ("MR. JUSTICE BLACKMUN,
                # with whom THE CHIEF JUSTICE and MR. JUSTICE BLACK join." in
                # Cohen), or an explicit hand-off ("MR. JUSTICE FIELD
                # delivered the following separate opinion.").
                wm = re.match(
                    r"(?:MR\.\s+|MRS\.\s+|MS\.\s+)?(CHIEF\s+)?JUSTICE\s+"
                    r"([A-Z][\w.'’-]+)\s*,\s*with\s+whom\b",
                    fix_title_comma((part.label or "").strip()),
                    re.IGNORECASE,
                )
                if wm:
                    title = "C.J." if wm.group(1) else "J."
                    surname = _fix_name_case(wm.group(2).replace("’", "'"))
                    return f"{surname}, {title}, {neutral_role}"
                bm = re.match(
                    r"((?:[A-Z][\w.'’-]*\s+)?[A-Z][\w.'’-]+),\s*"
                    r"(?:(C\.\s*J\.|Ch\.\s*J\.|Chief\s+(?:Justice|Judge))|"
                    r"J\.|Justice|(?:(?:Senior\s+|Presiding\s+)?"
                    r"(?:Circuit\s+|District\s+)?Judge))\s*[.:]?\s*$",
                    (part.label or "").strip(),
                )
                if bm:
                    title = "C.J." if bm.group(2) else "J."
                    surname = _fix_name_case(bm.group(1).replace("’", "'"))
                    return f"{surname}, {title}, {neutral_role}"
                dm = re.match(
                    r"(?:MR\.\s+|MRS\.\s+|MS\.\s+)?(CHIEF\s+)?JUSTICE\s+"
                    r"([A-Z][\w.'’-]+)\s+delivered\s+(?:the\s+following|a)\s+"
                    r"(?:separate|concurring|dissenting)\s+opinion",
                    (part.label or "").strip(), re.IGNORECASE,
                )
                if dm:
                    title = "C.J." if dm.group(1) else "J."
                    surname = _fix_name_case(dm.group(2).replace("’", "'"))
                    return f"{surname}, {title}, {neutral_role}"
                return "separate opinion" if part.kind == "separate" else ""
            phrase = {
                "concurrence": "concurring",
                "concurrence in part": "concurring in part",
                "dissent": "dissenting",
            }[re.sub(r"\s+", " ", lm.group(1)).lower()]
            author = (lm.group(2) or "").strip()
            if not author:
                return ""
            if re.search(r"per\s+curiam", author, re.IGNORECASE):
                return f"per curiam, {phrase}"
            surname = _fix_name_case(
                author.split(",")[0].split()[-1].replace("’", "'"))
            return f"{surname}, J., {phrase}"
        phrase = t[m.start():].rstrip(" .:;")
        phrase = re.sub(r"\s*\[[^\]]{1,6}\]$", "", phrase)  # trailing footnote marker
        head = t[: m.start()].strip().rstrip(", ")
        head = re.sub(r"^(?:MR\.|MRS\.|MS\.)\s+", "", head, flags=re.IGNORECASE)
        # The chief-justice test must look only at the author's own
        # designation — "JUSTICE THOMAS, with whom THE CHIEF JUSTICE and
        # JUSTICE ALITO join, dissenting" is Thomas, J., not C.J.
        segs = [s.strip() for s in head.split(",")]
        author_seg = segs[0]
        role_seg = segs[1] if len(segs) > 1 else ""
        is_chief = bool(
            re.match(r"(?:THE\s+)?CHIEF\s+JUSTICE\b", author_seg, re.IGNORECASE)
        ) or bool(
            re.fullmatch(
                r"(?:THE\s+)?(?:Chief\s+(?:Justice|Judge)|C\.\s?J\.)",
                role_seg,
                re.IGNORECASE,
            )
        )
        title = "C.J." if is_chief else "J."
        name = re.sub(
            r"^(?:THE\s+)?(?:CHIEF\s+)?JUSTICE\s+", "", author_seg, flags=re.IGNORECASE
        ).strip()
        name = _fix_name_case(name)
        # A circuit byline may print the judge's full name ("TOBY HEYTENS,
        # Circuit Judge, …"); the writer parenthetical takes the surname
        # alone, while disambiguating initials ("R. Nelson") stay.
        tokens = name.split()
        if len(tokens) > 1 and is_recognized_given_name(tokens[0]):
            name = " ".join(tokens[1:])
        if not name:
            return ""
        return f"{name}, {title}, {phrase}"

    def _majority_author(self, part) -> str:
        """Running-head label for the lead opinion: 'Blackmun, J.',
        'Sykes, C.J.', 'per curiam', or '' when no author is identified.
        Case-insensitive: Google Scholar renders many opinions' attribution
        lines in mixed case ('Justice Barrett delivered the opinion…')."""
        for b in part.blocks[:3]:
            t = fix_title_comma(re.sub(r"\s+", " ", b.text()).strip())
            t = re.sub(r"^(?:\*\d+\s+)+", "", t)
            if re.match(r"PER\s+CURIAM\b", t, re.IGNORECASE):
                return "per curiam"
            m = re.match(
                r"(?:(?:MR\.|MRS\.|MS\.)\s+)?(CHIEF\s+)?JUSTICE\s+([A-Z][\w.'’-]+)\s*,?\s+"
                r"(?:delivered|announced)",
                t, re.IGNORECASE,
            )
            if m:
                title = "C.J." if m.group(1) else "J."
                return f"{_fix_name_case(m.group(2))}, {title}"
            m = re.match(
                r"([A-Z][\w.'’ -]{0,40}?),\s*((?:Chief\s+)?(?:Senior\s+)?"
                r"(?:Circuit\s+|District\s+)?Judge)\s*[.:;]?\s*$",
                t, re.IGNORECASE,
            )
            if m:
                title = ("C.J." if re.search(r"\bChief\b", m.group(2), re.IGNORECASE)
                         else "J.")
                return f"{_fix_name_case(m.group(1).split(',')[0])}, {title}"
        return ""

    def _refine_part_labels(self, parts: list) -> None:
        """Sharpen the lead opinion's label.  ``segment_blocks`` calls every
        lead opinion "Majority Opinion", but a lead opinion that stands alone
        — no concurrences and no dissents — is simply the "Opinion" (as it is
        for a one-judge district court).  A Supreme Court lead opinion that
        only announces the judgment is a "Plurality Opinion"; otherwise it is
        the "Majority Opinion".  Each type is then followed by its author, the
        way the concurrence and dissent headers already name theirs."""
        maj = next((p for p in parts if p.kind == "majority"), None)
        if maj is None:
            return
        has_dissent = any(p.kind == "dissent" for p in parts)
        has_concurrence = any(p.kind == "concurrence" for p in parts)
        has_separate = any(p.kind == "separate" for p in parts)
        signal = self._writer_parenthetical(maj)  # "" | per curiam | plurality
        if signal == "plurality opinion" and self._is_scotus:
            base = "Plurality Opinion"
        elif not has_dissent and not has_concurrence and not has_separate:
            base = "Opinion"
        else:
            base = "Majority Opinion"
        author = self._majority_author(maj)  # "Name, J." | "per curiam" | ""
        if signal == "per curiam" or author == "per curiam":
            author = "Per Curiam"
        maj.label = f"{base} ({author})" if author else base

    def _title_citation(self) -> str:
        """The window-title citation: the case name followed by every parallel
        *printed* reporter cite (the chosen reporter first, then the parallels
        from the Scholar header and the CourtListener citation list), then the
        court-year parenthetical — e.g. "Roe v. Wade, 410 U.S. 113, 93 S. Ct.
        705, 35 L. Ed. 2d 147 (1973)".  Lexis/Westlaw and neutral (year-volume)
        cites are dropped (``_is_paginable_cite``)."""
        edited = getattr(self, "_base_citation_override", "")
        if edited:
            return edited
        bb = self._bb
        name = bb.get("name", "")
        display_cite = bb.get("display_cite") or bb.get("cite", "")
        seen: set = set()
        cites: list[str] = []
        display_key = re.sub(r"\s+", "", display_cite).lower()
        display_component_keys = _nominative_display_component_keys(display_cite)
        source_cites = ([display_cite] + list(self._header_cites)
                        + list((self._item or {}).get("citation", []) or []))
        for pos, c in enumerate(source_cites):
            c = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", str(c))).strip()
            if (not c or (pos and not _is_paginable_cite(c))
                    or _NONSTANDARD_CITE_RE.search(c)):
                continue
            # Re-space standard reporters ("S.Ct." -> "S. Ct."), but leave a
            # reporter with a parenthetical qualifier ("Media L. Rep. (BNA)")
            # alone — re-spacing would drop the parentheses.
            if "(" not in c:
                c = _respace_reporter_in_cite(c)
            key = re.sub(r"\s+", "", c).lower()
            identity_key = re.sub(r"[^a-z0-9]", "", c.lower())
            if pos and (key in display_key
                        or identity_key in display_component_keys):
                continue
            if key not in seen:
                seen.add(key)
                cites.append(c)
        # The chosen reporter stays first; order the parallels Bluebook-style
        # (major national reporters first), keeping ties in the order seen.
        if len(cites) > 1:
            tail = cites[1:]
            tail.sort(key=lambda c: next(
                (i for i, p in enumerate(_TITLE_CITE_RANK) if p.search(f" {c} ")),
                len(_TITLE_CITE_RANK),
            ))
            cites = [cites[0]] + tail
        paren = "" if bb.get("omit_parenthetical") else " ".join(
            p for p in (bb.get("court", ""), bb.get("year", "")) if p
        )
        if name and cites:
            title = f"{name}, {', '.join(cites)}"
        elif cites:
            title = ", ".join(cites)
        else:
            title = name
        if title and paren:
            title += f" ({paren})"
        return title

    def _enrich_citation(self) -> None:
        """Fill missing metadata and, narrowly, unresolved entity-name casing.

        When the opinion has already supplied both values, this method returns
        before consulting another source unless an organization-like caption
        token lacks reliable casing evidence in the opinion.  In that one case,
        static.case.law may donate capitalization for the identical word; it
        can never replace the opinion's caption.
        """
        bb = self._bb
        if not bb.get("cite"):
            return
        need_year = not bb.get("year")
        # SCOTUS takes no court parenthetical; known Supreme Court status is
        # therefore a complete court value.  For every other jurisdiction, a
        # nonempty abbreviation parsed from the opinion is sufficient.
        need_court = (not self._is_scotus) and not bb.get("court")
        unresolved_case = tuple(bb.get("_caption_case_unresolved") or ())
        need_caption_case = bool(
            unresolved_case
            and not getattr(self, "_base_citation_override", "")
        )
        if not (need_year or need_court or need_caption_case):
            return

        cite = bb["cite"]
        case_name = bb.get("name", "")
        item = dict(self._item)
        candidate_cites = [cite, *self._header_cites, *(item.get("citation") or [])]
        client = None
        if need_year or need_court:
            try:
                if self._app is not None and self._app._token_var.get().strip():
                    client = self._app._get_client()
            except Exception:
                client = None

        def run() -> None:
            reference_name = (
                _case_law_name_for_cites(candidate_cites)
                if need_caption_case else ""
            )
            court_id = year = ""
            if client is not None:
                if need_year or need_court:
                    try:
                        court_id, year = self._cl_court_and_year(
                            client, cite, item, name=case_name)
                    except Exception as exc:
                        print(f"[bb-enrich] CourtListener lookup failed: {exc}")
            if need_year and not year:
                # No CourtListener year (no token, or no cluster match):
                # CAP's decision_date covers the early U.S. Reports, whose
                # Scholar pages print no year at all ("2 U.S. 409 (____)").
                for c in candidate_cites[:4]:
                    c = re.sub(r"<[^>]+>", "", str(c or "")).strip()
                    cap = _case_law_metadata(c) if c else None
                    d = str((cap or {}).get("decision_date") or "")
                    if len(d) >= 4 and d[:4].isdigit():
                        year = d[:4]
                        break
            new_court = bb.get("court", "")
            if need_court and court_id:
                new_court = _court_for_paren(
                    cite, court_id.strip().lower(), new_court
                )
            new_year = year or bb.get("year", "")
            new_name = bb.get("name", "")
            if reference_name:
                new_name = apply_caption_case_reference(
                    new_name, reference_name, unresolved_case
                )
            if (new_court != bb.get("court", "")
                    or new_year != bb.get("year", "")
                    or new_name != bb.get("name", "")):
                self._post(
                    self._apply_enriched_citation, new_court, new_year, new_name
                )

        threading.Thread(target=run, daemon=True).start()

    @staticmethod
    def _cl_court_and_year(client, cite: str, item: dict,
                           name: str = "") -> tuple[str, str]:
        """(court_id, year) for the case from CourtListener — the cluster's
        date and its docket's court — locating the cluster by id or, failing
        that, by the reporter citation (disambiguated by ``name`` when the
        citation is ambiguous, e.g. "1 Cranch 299")."""
        cluster_id = item.get("cluster_id") or item.get("id")
        if not cluster_id:
            t = _cl_item_for_citation(client, cite, name=name)
            cluster_id = (t or {}).get("cluster_id")
        if not cluster_id:
            return "", ""
        cl = client.get_cluster(int(cluster_id), fields="date_filed,docket")
        year = (cl.get("date_filed") or "")[:4]
        court_id = ""
        docket_url = cl.get("docket")
        if docket_url:
            try:
                court_id = (
                    client._get_url(docket_url, {"fields": "court_id"}).get(
                        "court_id"
                    )
                    or ""
                )
            except Exception as exc:
                print(f"[bb-enrich] docket lookup failed: {exc}")
        return court_id, year

    def _apply_enriched_citation(
        self, court: str, year: str, name: str = ""
    ) -> None:
        old_history_key = self._history_key()
        self._bb["court"] = court
        self._bb["year"] = year
        # A user edit always wins, including over a metadata request that began
        # before the edit dialog was saved.
        if name and not getattr(self, "_base_citation_override", ""):
            self._bb["name"] = name
        try:
            title = self._title_citation()
            if title and self._win.winfo_exists():
                self._win.title(title)
                # Keep the History dropdown's label in step with the
                # enriched citation (court/year), without re-promoting it.
                app = self._app
                if app is not None and hasattr(app, "retitle_case_view"):
                    app.retitle_case_view(old_history_key, title)
        except tk.TclError:
            pass

    def _citation_name(self) -> str:
        """The party-name prefix of the Bluebook citation — the portion set
        in italics (Bluebook rule 2/10).  For a manually edited citation the
        name is parsed from the override, which can differ from the stored
        caption; otherwise it is the case name."""
        edited = getattr(self, "_base_citation_override", "")
        if edited:
            _plain, name = format_edited_citation(edited, None, ())
            return name.replace("'", "’")
        return (self._bb["name"] or "").replace("'", "’")

    def _bluebook_citation(
        self, pin: Optional[str], writer: str = "",
        extra_parens: tuple[str, ...] = (),
    ) -> tuple[str, str]:
        """Return (plain, rtf-fragment) forms of the Bluebook citation.
        `extra_parens` follow the writer parenthetical — e.g. "footnote
        omitted" (Bluebook rule 5.2(d))."""
        edited = getattr(self, "_base_citation_override", "")
        if edited:
            suffixes = tuple(p for p in (writer, *extra_parens) if p)
            plain, name = format_edited_citation(edited, pin, suffixes)
            rest = plain[len(name):]
            name = name.replace("'", "’")
            rest = rest.replace("'", "’")
            plain = name + rest
            if name:
                rtf = (
                    "\\par\\pard\\sa120 {\\i "
                    + _rtf_escape(name)
                    + "}"
                    + _rtf_escape(rest)
                    + "\\par\n"
                )
            else:
                rtf = "\\par\\pard\\sa120 " + _rtf_escape(plain) + "\\par\n"
            return plain, rtf

        bb = self._bb
        name = bb["name"]
        cite = bb.get("display_cite") or bb["cite"]
        court, year = bb["court"], bb["year"]
        rest = ""
        if cite:
            if pin and bb.get("pin_kind") == "paragraph" and ", " in cite:
                first, parallels = cite.split(", ", 1)
                rest = f", {first}, {pin}, {parallels}"
            else:
                rest = f", {cite}"
                if pin:
                    m = _CITE_PARSE_RE.match(bb.get("cite", ""))
                    if not (m and pin == m.group(3)):
                        rest += f", {pin}"
        paren_inner = "" if bb.get("omit_parenthetical") else " ".join(
            p for p in (court, year) if p
        )
        if paren_inner:
            rest += f" ({paren_inner})"
        if writer:
            rest += f" ({writer})"
        for extra in extra_parens:
            if extra:
                rest += f" ({extra})"
        rest += "."
        # Bluebook abbreviations ("Ass'n", "Int'l", "Dep't", "F. App'x"),
        # possessives, and names like O'Connor take a typographic apostrophe
        # (right single quotation mark) when copied or exported.
        name = name.replace("'", "’")
        rest = rest.replace("'", "’")
        if name:
            plain = f"{name}{rest}"
            rtf = (
                "\\par\\pard\\sa120 {\\i "
                + _rtf_escape(name)
                + "}"
                + _rtf_escape(rest)
                + "\\par\n"
            )
        else:
            plain = rest.lstrip(", ")
            rtf = "\\par\\pard\\sa120 " + _rtf_escape(plain) + "\\par\n"
        return plain, rtf

    @staticmethod
    def _page_num_from(s: str) -> Optional[int]:
        m = re.search(r"\d+", s)
        return int(m.group(0)) if m else None

    def _pin_for_range(self, start: str, end: str) -> Optional[str]:
        """Pinpoint page(s) for the text between *start* and *end*, derived
        from the star-pagination markers (Bluebook-style range, e.g. 120-21)."""
        txt = self._text
        if self._bb.get("pin_kind") == "paragraph":
            # Public-domain opinions print paragraph markers in the text.
            # Use those markers and never silently substitute a reporter page
            # for a jurisdiction that requires paragraph pinpoints.
            marker_re = re.compile(r"(?:¶\s*|\[\s*¶\s*)(\d{1,5})\s*\]?")
            before = txt.get("1.0", start)
            selected = txt.get(start, end)
            prior = list(marker_re.finditer(before))
            through = list(marker_re.finditer(before + selected))
            if not through:
                return None
            first = int((prior[-1] if prior else through[0]).group(1))
            last = int(through[-1].group(1))
            if last <= first:
                return f"¶ {first}"
            return f"¶¶ {first}–{last}"
        start_page: Optional[int] = None
        prev = txt.tag_prevrange("pagenum", start)
        if prev:
            start_page = self._page_num_from(txt.get(*prev))
        else:
            # No star marker on screen before the selection: in a part view
            # use the page in effect where the part begins; otherwise the
            # text sits on the cite's first page.
            if self._current_part is not None and self._part_start_pages:
                start_page = self._part_start_pages[self._current_part]
            if start_page is None:
                m = _CITE_PARSE_RE.match(self._bb["cite"])
                if m:
                    start_page = int(m.group(3))
        if start_page is None:
            return None
        end_page = start_page
        idx = start
        while True:
            rng = txt.tag_nextrange("pagenum", idx, end)
            if not rng:
                break
            p = self._page_num_from(txt.get(*rng))
            if p is not None:
                end_page = p
            idx = rng[1]
        if end_page <= start_page:
            return str(start_page)
        sa, sb = str(start_page), str(end_page)
        if len(sa) == len(sb) and len(sa) > 2 and sa[:-2] == sb[:-2]:
            sb = sb[-2:]  # Bluebook: drop repetitious digits, keep last two
        return f"{sa}-{sb}"

    @staticmethod
    def _format_note_numbers(nums: list[str]) -> str:
        """Bluebook note pins: n.4 / nn.4-5 (consecutive) / nn.4 & 6."""
        runs: list[list[str]] = []
        for n in nums:
            if (
                runs
                and n.isdigit()
                and runs[-1][-1].isdigit()
                and int(n) == int(runs[-1][-1]) + 1
            ):
                runs[-1].append(n)
            else:
                runs.append([n])
        parts = [f"{r[0]}-{r[-1]}" if len(r) > 1 else r[0] for r in runs]
        prefix = "nn." if len(nums) > 1 else "n."
        return prefix + " & ".join(parts)

    def _pin_with_footnotes(self, start: str, end: str) -> Optional[str]:
        """
        Pinpoint for the selection, footnote-aware (Bluebook rule 3.2(b)):
        material in a footnote cites as "page n.N"; several notes as
        "nn.4-5" / "nn.4 & 6"; text plus a note on the same page as
        "page & n.N".
        """
        txt = self._text
        regions = [
            r for r in self._fn_regions
            if txt.compare(r[0], "<", end) and txt.compare(r[1], ">", start)
        ]
        if not regions:
            return self._pin_for_range(start, end)

        fallback_page: Optional[int] = None
        m = _CITE_PARSE_RE.match(self._bb["cite"])
        if m:
            fallback_page = int(m.group(3))

        # Group selected notes by the page they're cited on (document order)
        page_groups: dict[Optional[int], list[str]] = {}
        for _rs, _re, num, page in regions:
            page_groups.setdefault(page, []).append(num)
        note_strs = []
        for page, nums in page_groups.items():
            p = page if page is not None else fallback_page
            s = self._format_note_numbers(nums)
            note_strs.append(f"{p} {s}" if p is not None else s)
        notes = ", ".join(note_strs)

        # Does the selection also cover opinion text before the notes?
        first_rs = regions[0][0]
        text_before = (
            txt.compare(start, "<", first_rs)
            and txt.get(start, first_rs).strip() != ""
        )
        if not text_before:
            return notes
        text_pin = self._pin_for_range(start, first_rs)
        if text_pin is None:
            return notes
        if len(page_groups) == 1:
            (page, nums), = page_groups.items()
            p = page if page is not None else fallback_page
            if p is not None and text_pin == str(p):
                return f"{text_pin} & {self._format_note_numbers(nums)}"
        return f"{text_pin}, {notes}"

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _copy_formatted(self) -> None:
        txt = self._text
        try:
            start, end = txt.index("sel.first"), txt.index("sel.last")
            selected = True
        except tk.TclError:
            start, end = "1.0", "end-1c"
            selected = False
        # The "Copy with citation" box (checked by default) gates whether the
        # Bluebook citation is appended; unchecked, the selection is copied on
        # its own (still richly, just without the citation).
        with_cite = self._copy_with_cite.get()
        plain_cite, rtf_cite = "", ""
        omit_tags: set[str] = set()
        n_omitted = 0
        if with_cite and selected:
            # Footnote references inside the copied body text: a quote-ready
            # copy drops the marker and says so in the citation ("(footnote
            # omitted)") — unless the note's own text is inside the selection
            # (then it is being copied, not omitted).
            omit_tags, n_omitted = self._omitted_footnote_tags(start, end)
        if with_cite:
            # The appended citation already carries the reporter-page pin.
            # Keeping inline ``*123`` markers in the quotation duplicates that
            # pagination and interrupts the copied prose.  A plain copy (the
            # checkbox unchecked) deliberately retains the markers.
            omit_tags.add("pagenum")
        if with_cite:
            # Pin cites and the writer parenthetical apply whenever the opinion
            # on screen actually carries reporter page markers — the Google
            # Scholar view and any CourtListener opinion assembled with page
            # numbers (REST API included) — regardless of how the window was
            # opened.  Check the live text, not a flag fixed at open time (which
            # goes stale across a source toggle or a late Scholar match).
            scholar_like = (
                self._mode == "scholar" or bool(self._text.tag_ranges("pagenum"))
            )
            pin = (
                self._pin_with_footnotes(start, end)
                if (selected and scholar_like)
                else None
            )
            # The writer parenthetical ("Brandeis, J., dissenting") applies in
            # every parts-aware view — the CourtListener sub-opinion view knows
            # each part's type and author even without star pagination.
            writer = ""
            parts = getattr(self, "_rendered_parts", None) or self._parts
            if parts:
                pi = self._current_part
                if pi is None and selected:
                    for rs, rend, p in self._part_region_indices():
                        if txt.compare(start, ">=", rs) and txt.compare(start, "<", rend):
                            pi = p
                            break
                if pi is not None and pi < len(parts):
                    writer = self._writer_parenthetical(parts[pi])
            extras: tuple[str, ...] = ()
            if n_omitted:
                extras = ("footnote omitted" if n_omitted == 1
                          else "footnotes omitted",)
            plain_cite, rtf_cite = self._bluebook_citation(pin, writer, extras)
        body = _dump_to_rtf(txt, start, end, fn_links=self._fn_link_map(),
                            omit_tags=omit_tags)
        rtf = _rtf_document(body + rtf_cite)
        plain = _plain_without_layout_chars(txt, start, end,
                                            omit_tags=omit_tags).rstrip()
        if plain_cite:
            plain += "\n\n" + plain_cite + "\n"
        how = _copy_rich_clipboard(self._win, rtf, plain)
        what = "selection" if selected else "full text"
        self._status_var.set(
            f"Copied {what} as {how}"
            + ("; citation appended." if with_cite else ".")
        )

    def _fn_link_map(self) -> dict[str, tuple[str, str]]:
        """Link tags that anchor footnote jumps, for RTF bookmarks."""
        return {t: a for t, a in self._link_actions.items()
                if a[0] in ("fnref", "fndef")}

    def _omitted_footnote_tags(self, start: str, end: str) -> tuple[set[str], int]:
        """Footnote-reference marker tags inside [start, end) whose notes a
        quote-ready copy omits, with the count of distinct omitted notes.
        A reference is omitted unless its note's body is also selected (the
        note is then being copied along, not dropped); markers inside a
        footnote body (a note citing another note) are left alone."""
        txt = self._text
        fn_regions = getattr(self, "_fn_regions", []) or []
        def_pos = getattr(self, "_fn_def_pos", {}) or {}
        omit: set[str] = set()
        fids: set[str] = set()
        for tag, (side, fid) in self._fn_link_map().items():
            if side != "fnref":
                continue
            ranges = txt.tag_ranges(tag)
            for i in range(0, len(ranges), 2):
                rs, rend = str(ranges[i]), str(ranges[i + 1])
                if not (txt.compare(rs, "<", end)
                        and txt.compare(rend, ">", start)):
                    continue
                if any(txt.compare(rs, ">=", fr[0])
                       and txt.compare(rs, "<", fr[1]) for fr in fn_regions):
                    continue  # marker sits inside a note body
                body_pos = def_pos.get(fid)
                if body_pos and txt.compare(body_pos, ">=", start) \
                        and txt.compare(body_pos, "<", end):
                    continue  # the note itself is selected too
                omit.add(tag)
                fids.add(fid)
        return omit, len(fids)

    def _filename_item(self) -> dict:
        if self._item:
            return self._item
        bb = self._bb
        return {
            "caseName": bb["name"],
            "citation": [bb["cite"]] if bb["cite"] else [],
            "dateFiled": f"{bb['year']}-01-01" if bb["year"] else "",
            "court_id": "scotus" if not bb["court"] else "",
            "court": bb["court"],
        }

    def _export_section_list(self) -> list[tuple[str, str, str, str]]:
        """
        Sections for a full-document export, as (running-head label, start,
        end, kind): the header and majority share the first section, then
        each concurrence/dissent follows as its own.  Without parts (a plain
        text view) the whole widget is one section.
        """
        txt = self._text
        parts = getattr(self, "_rendered_parts", None) or self._parts
        regions = self._part_region_indices()
        if not parts or not regions:
            return [("", "1.0", txt.index("end-1c"), "majority")]
        main_end = -1
        for i, (_rs, _re, pi) in enumerate(regions):
            if parts[pi].kind in ("header", "majority"):
                main_end = i
            else:
                break
        main_regions = regions[: main_end + 1]
        rest_regions = regions[main_end + 1:]

        # (author label, start, end, kind)
        sections: list[tuple[str, str, str, str]] = []
        if main_regions:
            maj = next(
                (parts[pi] for _rs, _re, pi in main_regions
                 if parts[pi].kind == "majority"),
                None,
            )
            label = self._majority_author(maj) if maj is not None else ""
            sections.append((label, main_regions[0][0], main_regions[-1][1],
                             "majority"))
        for rs, rend, pi in rest_regions:
            sections.append((self._writer_parenthetical(parts[pi]), rs,
                             rend, parts[pi].kind))
        return sections

    def _build_export_rtf(self) -> str:
        """
        Two-column RTF of the full opinion, one section per separate
        opinion: the header and majority share the first section, and each
        concurrence/dissent starts a new page (numbering continues).  Every
        section carries a running head with the Bluebook citation and the
        opinion's author, and a page-number footer.  The running head is
        coloured by opinion kind (dissent red, concurrence green); the body
        text is black.
        """
        txt = self._text
        case_line = self._bluebook_citation(None)[0].rstrip(".")
        sections = self._export_section_list()

        # Colour only the running heading by opinion kind (dissent red,
        # concurrence green, unresolved separate opinion neutral slate); the
        # body text of every opinion stays black.
        head_cf = {
            "dissent": "\\cf2 ", "concurrence": "\\cf3 ",
            "separate": "\\cf4 ",
        }
        out: list[str] = []
        for i, (label, rs, rend, kind) in enumerate(sections):
            out.append(
                "\\sectd\\sbknone\\cols2\\colsx432\n"
                if i == 0
                else "\\sect\\sectd\\sbkpage\\cols2\\colsx432\n"
            )
            head = f"{case_line} — {label}" if label else case_line
            out.append(
                "{\\header\\pard\\qc\\fs18\\i " + head_cf.get(kind, "")
                + _rtf_escape(head) + "\\par}\n"
            )
            out.append("{\\footer\\pard\\qc\\fs18\\chpgn\\par}\n")
            out.append(_dump_to_rtf(txt, rs, rend, part_colors=False,
                                    fn_links=self._fn_link_map()))
        return _RTF_HEADER + "".join(out) + "}"

    def _export_rtf(self) -> None:
        if self._mode == "scholar" and self._parts:
            # Export the full opinion even from a single-part view
            prev = self._current_part
            if prev is not None:
                self._current_part = None
                self._render_scholar()
            try:
                rtf = self._build_export_rtf()
            finally:
                if prev is not None:
                    self._current_part = prev
                    self._render_scholar()
        else:
            body = _dump_to_rtf(self._text, "1.0", "end-1c",
                                fn_links=self._fn_link_map())
            rtf = _rtf_document(body, two_columns=True, page_footer=True)
        default = _build_default_filename(self._filename_item())
        path = filedialog.asksaveasfilename(
            defaultextension=".rtf",
            filetypes=[("Rich Text Format", "*.rtf"), ("All files", "*.*")],
            initialfile=f"{default}.rtf",
            title="Export Opinion as RTF (two columns)",
            parent=self._win,
        )
        if not path:
            return
        with open(path, "w", encoding="ascii", errors="replace") as f:
            f.write(rtf)
        self._status_var.set(f"Exported RTF: {path}")
        if messagebox.askyesno(
            "Export Complete", f"RTF saved to:\n{path}\n\nOpen it now?", parent=self._win
        ):
            CourtListenerGUI._open_file(path)

    # ------------------------------------------------------------------
    # PDF (LaTeX) and Markdown export
    # ------------------------------------------------------------------

    def _post_export_menu(self) -> None:
        """Drop the export-format menu under the Export button."""
        menu = tk.Menu(self._win, tearoff=0)
        menu.add_command(label="Rich Text (.rtf) — two columns…",
                         command=self._export_rtf)
        menu.add_command(label="PDF (.pdf) — typeset with LaTeX…",
                         command=self._export_pdf_latex)
        menu.add_command(label="Markdown (.md)…",
                         command=self._export_markdown)
        btn = self._export_btn
        try:
            menu.tk_popup(btn.winfo_rootx(),
                          btn.winfo_rooty() + btn.winfo_height())
        finally:
            menu.grab_release()

    def _with_full_view(self, build):
        """Run *build* with the full opinion rendered, restoring a
        single-part view afterwards (the same dance _export_rtf does)."""
        prev = self._current_part
        rerender = None
        if prev is not None:
            if self._mode == "scholar" and self._parts:
                rerender = self._render_scholar
            elif self._mode == "courtlistener" and (
                getattr(self, "_cl_parts", None)
                or getattr(self, "_cl_blocks", None)
            ):
                rerender = self._render_cl_blocks
        if rerender is None:
            return build()
        self._current_part = None
        rerender()
        try:
            return build()
        finally:
            self._current_part = prev
            rerender()

    def _section_note_map(
        self,
        sec_start: str,
        sec_end: str,
        fn_links: dict[str, tuple[str, str]],
    ) -> tuple[list, list[tuple[str, str, list[_ExpPara]]]]:
        """
        The footnote-body regions inside a section, plus each note parsed
        into (anchor id, label, body paragraphs) with its leading marker
        stripped — shared by the LaTeX and Markdown writers.
        """
        txt = self._text
        regions = sorted(
            (r for r in getattr(self, "_fn_regions", []) or []
             if _tk_ix(sec_start) <= _tk_ix(r[0]) < _tk_ix(sec_end)),
            key=lambda r: _tk_ix(r[0]),
        )
        parsed: list[tuple[str, str, list[_ExpPara]]] = []
        for nrs, nre, num, _page in regions:
            paras = _dump_export_paragraphs(txt, [(nrs, nre)], fn_links)
            fid, disp = _strip_note_marker(paras)
            label = (num or disp).strip().strip("[]").rstrip(".") or "*"
            parsed.append((fid, label, paras))
        return regions, parsed

    def _build_export_latex(self) -> str:
        """
        A complete LaTeX document of the opinion: single column, justified,
        Century Schoolbook (or the closest installed face), footnotes at
        the bottom of the page that cites them, star pagination kept inline
        and echoed as a reporter page range in the running head, a page
        number in the footer, and each separate opinion starting on a fresh
        page under its own running head.
        """
        txt = self._text
        fn_links = self._fn_link_map()
        case_line = self._bluebook_citation(None)[0].rstrip(".")
        sections = self._export_section_list()

        has_marks = bool(txt.tag_ranges("pagenum"))
        reporter_prefix = ""
        first_page: Optional[int] = None
        m = _CITE_PARSE_RE.match(self._bb["cite"] or "")
        if m:
            reporter_prefix = f"{m.group(1)} {m.group(2)}"
            first_page = int(m.group(3))
        if has_marks and first_page is None:
            rng = txt.tag_nextrange("pagenum", "1.0")
            if rng:
                pm = re.search(r"\d+", txt.get(*rng))
                if pm:
                    marker_page = int(pm.group(0))
                    first_page = (
                        max(1, marker_page - 1)
                        if txt.get("1.0", rng[0]).strip()
                        else marker_page
                    )

        doc: list[str] = [_LATEX_PREAMBLE]
        if has_marks and reporter_prefix:
            doc.append("\\renewcommand{\\reporterhead}{%s \\reporterrange}\n"
                       % _latex_escape(reporter_prefix))
        doc.append("\\begin{document}\n\\thispagestyle{plain}%\n")
        if has_marks and first_page is not None:
            doc.append("\\opfirstreporterpage=%d\\relax%%\n" % first_page)
            doc.append("\\markboth{%d}{%d}%%\n" % (first_page, first_page))
        # Bluebook rule 2/10: only the party names are italicized; the rest
        # of the citation (comma onward — reporter, court, year) is upright.
        head_name = self._citation_name()
        if head_name and case_line.startswith(head_name):
            head_cite = ("\\textit{%s}%s"
                         % (_latex_escape(head_name),
                            _latex_escape(case_line[len(head_name):])))
        else:
            head_cite = _latex_escape(case_line)
        for i, (label, rs, rend, _kind) in enumerate(sections):
            if i:
                doc.append("\\clearpage\n")
            head = head_cite
            if label:
                head += " --- " + _latex_escape(label)
            doc.append("\\renewcommand{\\opinionhead}{%s}%%\n" % head)
            doc.append(self._latex_section_body(
                rs, rend, fn_links, title_block=(i == 0)))
        doc.append("\\end{document}\n")
        return "".join(doc)

    def _latex_section_body(
        self,
        sec_start: str,
        sec_end: str,
        fn_links: dict[str, tuple[str, str]],
        title_block: bool,
    ) -> str:
        """One export section as LaTeX: its text with each footnote moved
        inline (as a real \\footnote) at its in-text reference."""
        txt = self._text
        regions, parsed = self._section_note_map(sec_start, sec_end, fn_links)
        notes: dict[str, tuple[str, str]] = {}
        ordered: list[tuple[str, str, str]] = []
        for fid, label, paras in parsed:
            body = _latex_footnote_body(paras)
            if fid and fid not in notes:
                notes[fid] = (label, body)
            ordered.append((fid, label, body))

        body_paras = _dump_export_paragraphs(
            txt, _section_body_ranges(sec_start, sec_end, regions), fn_links
        )
        used: set[str] = set()
        out = [_latex_paragraphs(body_paras, notes, used,
                                 title_block=title_block)]
        leftovers = [(label, body) for fid, label, body in ordered
                     if (not fid or fid not in used) and body]
        if leftovers:
            # Notes that were never referenced in this section's text (or
            # whose in-text anchor wasn't found) stay at the section's end.
            out.append(
                "\\bigskip\\noindent\\rule{2in}{0.4pt}\\par\\smallskip\n"
                "\\begin{footnotesize}\n"
            )
            for label, body in leftovers:
                out.append("\\noindent\\textsuperscript{%s} %s\\par\\smallskip\n"
                           % (_latex_escape(label), body))
            out.append("\\end{footnotesize}\n")
        return "".join(out)

    def _export_pdf_latex(self) -> None:
        engine = _find_latex_engine()
        if engine is None:
            if messagebox.askyesno(
                "LaTeX Not Found",
                "Exporting to PDF needs a LaTeX installation (pdflatex, "
                "xelatex, lualatex or tectonic), and none was found on "
                "this system.\n\nExport a Markdown (.md) file instead?",
                parent=self._win,
            ):
                self._export_markdown()
            return
        tex = self._with_full_view(self._build_export_latex)
        default = _build_default_filename(self._filename_item())
        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{default}.pdf",
            title="Export Opinion as PDF (typeset with LaTeX)",
            parent=self._win,
        )
        if not path:
            return
        self._status_var.set(f"Typesetting PDF with {engine[0]}…")
        try:
            self._win.update_idletasks()
        except tk.TclError:
            pass
        ok, err = _compile_latex(tex, engine, path)
        if not ok:
            tex_path = os.path.splitext(path)[0] + ".tex"
            try:
                with open(tex_path, "w", encoding="utf-8") as f:
                    f.write(tex)
            except OSError:
                tex_path = ""
            self._status_var.set("PDF export failed.")
            messagebox.showerror(
                "PDF Export Failed",
                f"{engine[0]} could not build the PDF."
                + (f"\n\nThe LaTeX source was saved to:\n{tex_path}"
                   if tex_path else "")
                + (f"\n\n{err.strip()}" if err.strip() else ""),
                parent=self._win,
            )
            return
        self._status_var.set(f"Exported PDF: {path}")
        if messagebox.askyesno(
            "Export Complete", f"PDF saved to:\n{path}\n\nOpen it now?",
            parent=self._win,
        ):
            CourtListenerGUI._open_file(path)

    def _build_export_markdown(self) -> str:
        """
        The full opinion as Markdown: title and Bluebook citation up top, a
        horizontal rule and heading before each separate opinion, bold
        star-pagination markers inline, and footnotes as Markdown footnotes
        ([^4]) collected at the end of the opinion that cites them.
        """
        txt = self._text
        fn_links = self._fn_link_map()
        case_line = self._bluebook_citation(None)[0]
        sections = self._export_section_list()

        blocks: list[str] = []
        name = self._citation_name()
        if name:
            blocks.append("# " + _md_escape(name))
            if case_line.startswith(name):
                blocks.append("*" + _md_escape(name) + "*"
                              + _md_escape(case_line[len(name):]))
            else:
                blocks.append(_md_escape(case_line))
        elif case_line.rstrip("."):
            blocks.append("# " + _md_escape(case_line.rstrip(".")))

        used_ids: set[str] = set()
        for i, (label, rs, rend, kind) in enumerate(sections):
            regions, parsed = self._section_note_map(rs, rend, fn_links)
            ref_ids: dict[str, str] = {}
            defs: list[tuple[str, list[str]]] = []
            leftovers: list[tuple[str, list[str]]] = []
            for fid, note_label, paras in parsed:
                note_blocks = _md_paragraphs(paras)
                if not note_blocks:
                    continue
                if fid:
                    base = re.sub(r"\W+", "", note_label) or "sym"
                    rid, n = base, 2
                    while rid in used_ids:
                        rid = f"{base}-{n}"
                        n += 1
                    used_ids.add(rid)
                    ref_ids[fid] = rid
                    defs.append((rid, note_blocks))
                else:
                    leftovers.append((note_label, note_blocks))

            if i:
                blocks.append("---")
                blocks.append("## " + _md_escape(label or kind.title()))
            body_paras = _dump_export_paragraphs(
                txt, _section_body_ranges(rs, rend, regions), fn_links
            )
            blocks.extend(_md_paragraphs(body_paras, ref_ids))
            for rid, note_blocks in defs:
                first = note_blocks[0]
                cont = "".join(
                    "\n\n    " + b.replace("\n", "\n    ")
                    for b in note_blocks[1:]
                )
                blocks.append(f"[^{rid}]: {first}{cont}")
            if leftovers:
                blocks.append("**Footnotes**")
                for note_label, note_blocks in leftovers:
                    body = "\n\n".join(note_blocks)
                    blocks.append(f"**{_md_escape(note_label)}.** {body}")
        return "\n\n".join(blocks) + "\n"

    def _export_markdown(self) -> None:
        md = self._with_full_view(self._build_export_markdown)
        default = _build_default_filename(self._filename_item())
        path = filedialog.asksaveasfilename(
            defaultextension=".md",
            filetypes=[("Markdown", "*.md"), ("All files", "*.*")],
            initialfile=f"{default}.md",
            title="Export Opinion as Markdown",
            parent=self._win,
        )
        if not path:
            return
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
        self._status_var.set(f"Exported Markdown: {path}")
        if messagebox.askyesno(
            "Export Complete", f"Markdown saved to:\n{path}\n\nOpen it now?",
            parent=self._win,
        ):
            CourtListenerGUI._open_file(path)

    # ------------------------------------------------------------------
    # Case details side panel (authors and joins per opinion)
    # ------------------------------------------------------------------

    def _details_panel(self) -> ttk.Frame:
        if self._details_frame is None:
            f = ttk.Frame(self._text_frame)
            base_family = tkfont.nametofont("TkDefaultFont").actual("family")
            # Base point sizes (auto-scaled for dense displays); the panel's own
            # A−/A+ control then layers a persisted offset on top of these.
            self._details_font_bases = {
                "body": _scaled_panel_font_size(self._win, 9),
                "title": _scaled_panel_font_size(self._win, 9),
                "h": _scaled_panel_font_size(self._win, 9),
                "lbl": _scaled_panel_font_size(self._win, 9),
                "ctitle": _scaled_panel_font_size(self._win, 11),
            }
            # Named Font objects so a zoom just reconfigures them and Tk
            # restyles the heading, body and every tag at once.
            def _mk(key, **kw):
                return tkfont.Font(
                    family=base_family, size=self._details_font_bases[key], **kw)
            self._details_fonts = {
                "body": _mk("body"),
                "title": _mk("title", weight="bold"),
                "h": _mk("h", weight="bold"),
                "lbl": _mk("lbl", slant="italic"),
                "ctitle": _mk("ctitle", weight="bold"),
            }
            self._details_title_var = tk.StringVar(value="Opinions & Joins")

            header = ttk.Frame(f)
            header.pack(fill="x", padx=6, pady=(4, 2))
            ttk.Label(
                header, textvariable=self._details_title_var, anchor="w",
                font=self._details_fonts["title"],
            ).pack(side="left", fill="x", expand=True)
            # Text-size control for this panel, top-right opposite the heading —
            # the side panel is otherwise fixed-size and can read small on
            # high-resolution screens (e.g. a Retina Mac).
            _ui_button(header, "A+", command=lambda: self._zoom_details(+1),
                       width=40).pack(side="right")
            _ui_button(header, "A−", command=lambda: self._zoom_details(-1),
                       width=40).pack(side="right", padx=(0, 4))

            # What the panel shows: the case details (Oyez line-up/summary,
            # falling back to CourtListener's cluster record, then to whatever
            # the open text reveals), the Court's most recent decisions, the
            # case's appellate family (appeals, decision below, remand
            # proceedings), or the opinion's detected outline.  Case details
            # and the recent-decisions list are separate views now, not one
            # falling through to the other.
            mode_row = ttk.Frame(f)
            mode_row.pack(fill="x", padx=6, pady=(0, 2))
            ttk.Label(mode_row, text="Show",
                      style="ModernMuted.TLabel" if _CTK_AVAILABLE else "TLabel",
                      ).pack(side="left")
            self._details_mode_combo = ttk.Combobox(
                mode_row, state="readonly", width=16,
                values=("Case details", "Recent SCOTUS", "Related cases",
                        "Outline"),
                style="Modern.TCombobox" if _CTK_AVAILABLE else "TCombobox",
            )
            self._details_mode_combo.current(0)
            self._details_mode_combo.pack(side="left", padx=(6, 0))
            self._details_mode_combo.bind(
                "<<ComboboxSelected>>",
                lambda _e: self._refresh_details_view())

            body = tk.Text(
                f, width=38, wrap="word",
                font=self._details_fonts["body"],
                state="disabled", padx=8, pady=4, relief="flat",
                background="#f7f5ef", cursor="",
            )
            dvsb = ttk.Scrollbar(f, orient="vertical", command=body.yview)
            body.configure(yscrollcommand=dvsb.set)
            dvsb.pack(side="right", fill="y")
            body.pack(side="left", fill="both", expand=True)
            body.tag_configure("title", font=self._details_fonts["ctitle"],
                               spacing1=2, spacing3=2)
            body.tag_configure("h", font=self._details_fonts["h"], spacing1=10)
            body.tag_configure("lbl", font=self._details_fonts["lbl"],
                               foreground="#666666")
            body.tag_configure("olink", foreground=self._LINK_COLOR,
                               underline=True)
            body.tag_bind("olink", "<Enter>",
                          lambda _e: body.config(cursor="hand2"))
            body.tag_bind("olink", "<Leave>",
                          lambda _e: body.config(cursor=""))
            # Outline entries for the opinion's parts, in the same colors the
            # text view's part map uses (configured after "olink" so the part
            # color outranks the link blue).
            body.tag_configure("olpart", font=self._details_fonts["h"],
                               foreground="#333333", spacing1=8)
            body.tag_configure("olmaj", font=self._details_fonts["h"],
                               foreground=self._MAJORITY_COLOR, spacing1=8)
            body.tag_configure("olconc", font=self._details_fonts["h"],
                               foreground=self._CONCUR_COLOR, spacing1=8)
            body.tag_configure("oldiss", font=self._details_fonts["h"],
                               foreground=self._DISSENT_COLOR, spacing1=8)
            self._details_text = body
            self._details_frame = f
            self._apply_details_fonts()   # honor the persisted zoom choice
        return self._details_frame

    def _apply_details_fonts(self) -> None:
        """Resize the side panel's fonts to their base size plus the saved zoom
        offset.  Reconfiguring the shared Font objects restyles the heading,
        body and every tag together."""
        fonts = getattr(self, "_details_fonts", None)
        if not fonts:
            return
        delta = _get_details_font_delta()
        for name, font in fonts.items():
            font.configure(
                size=max(7, min(30, self._details_font_bases[name] + delta)))

    def _zoom_details(self, delta: int) -> None:
        """Grow/shrink the side panel's text with its A−/A+ control (delta 0
        resets), persisting the choice across cases and launches."""
        if not getattr(self, "_details_fonts", None):
            return
        _set_details_font_delta(0 if delta == 0 else
                                _get_details_font_delta() + delta)
        self._apply_details_fonts()
        self._status_var.set(
            f"Side panel text: {self._details_fonts['body'].cget('size')} pt")

    def _toggle_details(self) -> None:
        if self._details_var.get():
            self._details_panel().pack(side="right", fill="y",
                                       before=self._vsb)
            self._refresh_details_view()
        elif self._details_frame is not None:
            self._details_frame.pack_forget()

    def _details_mode(self) -> str:
        """The side panel's selected view: "case", "recent", "related" or
        "outline"."""
        combo = getattr(self, "_details_mode_combo", None)
        try:
            if combo is not None:
                sel = combo.get()
                if sel == "Outline":
                    return "outline"
                if sel == "Related cases":
                    return "related"
                if sel == "Recent SCOTUS":
                    return "recent"
        except tk.TclError:
            pass
        return "case"

    def _refresh_details_view(self) -> None:
        """Fill the side panel for its selected mode."""
        if self._details_frame is None:
            return
        mode = self._details_mode()
        if mode == "outline":
            self._show_outline()
        elif mode == "related":
            self._show_related_cases()
        elif mode == "recent":
            self._show_recent_scotus()
        else:
            self._show_case_details()

    def _show_case_details(self) -> None:
        """The case-details view: cached lines when the lookup already ran,
        else kick off the background load."""
        cached = getattr(self, "_details_case", None)
        if cached is not None:
            self._apply_details(*cached)
        elif not self._details_loaded:
            self._load_details()
        else:  # lookup still in flight
            self._set_details([("lbl", "Loading case details…")])

    def _apply_case_details(self, title: str, lines: list[tuple]) -> None:
        """Store the fetched case details and show them — unless the user
        has switched the panel to another view meanwhile."""
        self._details_case = (title, lines)
        if self._details_mode() == "case":
            self._apply_details(title, lines)

    def _set_details(self, lines: list[tuple]) -> None:
        """Render the details pane.  Each line is ``(style, text)`` or, for a
        clickable link, ``(style, text, target)`` — underlined; *target* is a
        URL opened in the browser, or a callable invoked directly (an in-app
        opener, e.g. the slip-opinion viewer)."""
        body = self._details_text
        body.config(state="normal")
        body.delete("1.0", "end")
        link_n = 0
        for item in lines:
            if len(item) == 3:
                style, text, target = item
                tag = f"olink{link_n}"
                link_n += 1
                body.tag_bind(
                    tag, "<Button-1>",
                    lambda _e, t=target: (
                        t() if callable(t) else self._open_details_link(t)),
                )
                tags = ("olink", tag)
                if style:
                    tags += (style,)
                body.insert("end", text + "\n", tags)
            else:
                style, text = item
                body.insert("end", text + "\n", (style,) if style else ())
        body.config(state="disabled")

    def _apply_details(self, title: str, lines: list[tuple]) -> None:
        """Set the panel heading and body together (called on the main thread)."""
        if getattr(self, "_details_title_var", None) is not None and title:
            self._details_title_var.set(title)
        self._set_details(lines)

    def _open_details_link(self, url: str) -> None:
        webbrowser.open(url)
        self._status_var.set("Opened in your browser.")

    def _load_details(self) -> None:
        """Fill the case-details panel: the Oyez line-up/summary for a Supreme
        Court case; when Oyez has nothing, CourtListener's own record for the
        case (parties, court, date, parallel citations, panel); last, whatever
        authorship the open text itself reveals.  (The Court's recent decisions
        are their own "Recent SCOTUS" view now, not a fallback here.)"""
        self._details_loaded = True
        self._set_details([("lbl", "Loading case details…")])
        cite = self._bb["cite"]
        is_scotus = self._is_scotus
        oyez_cites = [c for c in ([cite] + list(self._header_cites)) if c]
        name = self._bb.get("name", "")
        year = self._bb.get("year", "")
        # Resolve the CourtListener client on the main thread (it reads tk vars),
        # but only when a token is set — the case-details panel opens
        # automatically for SCOTUS cases, and _get_client() pops a "Missing
        # Token" dialog when there's none.  Oyez, the primary source, needs no
        # token, so the CourtListener fallback simply stays unavailable.
        client = None
        if (self._app is not None
                and self._app._token_var.get().strip()):
            client = self._app._get_client()

        def run() -> None:
            title = "Opinions & Joins"
            lines: list[tuple] = []
            # Supreme Court cases: Oyez first — its majority/dissent line-up,
            # plain-English summary and oral-argument audio are far richer than
            # anything else.  Any failure falls through to the paths below.
            if is_scotus:
                try:
                    case = oyez.lookup(cites=oyez_cites, name=name, year=year)
                    if case is not None and case.is_substantive:
                        lines = self._details_lines_oyez(case)
                        if lines:
                            title = "Supreme Court · Oyez"
                except Exception as exc:
                    print(f"[details] oyez: {exc}")
            # No Oyez entry (a case too recent or too obscure for it, or a
            # lower-court case that Oyez never covers): fall back to
            # CourtListener's record for the case — the parties, deciding court,
            # date, every parallel citation and the panel — resolved by
            # citation.
            if not lines and client is not None and cite:
                try:
                    lines = self._details_lines_cl(client, cite, name)
                    if lines:
                        title = "Case details · CourtListener"
                except Exception as exc:
                    print(f"[details] courtlistener: {exc}")
            if not lines:
                lines = self._details_lines_parts()
            if not lines:
                lines = [("lbl", "No authorship details available "
                                 "for this case.")]
            self._post(self._apply_case_details, title, lines)

        threading.Thread(target=run, daemon=True).start()

    def _details_lines_cl(self, client, cite: str, name: str) -> list[tuple]:
        """CourtListener's record for the cited case, used as the case-details
        fallback when Oyez has nothing: parties, deciding court, decision date,
        every parallel citation, precedential status and the panel, plus a link
        to the opinion on CourtListener.  Returns [] when the citation resolves
        to no cluster."""
        item = _cl_item_for_citation(client, cite, name=name)
        if not item:
            return []
        cluster_id = item.get("cluster_id") or item.get("id")
        cluster: dict = {}
        if cluster_id:
            try:
                cluster = client.get_cluster(
                    int(cluster_id),
                    fields="case_name,case_name_full,date_filed,citations,"
                           "judges,precedential_status,absolute_url,"
                           "nature_of_suit",
                ) or {}
            except Exception as exc:
                print(f"[details] cluster fetch failed: {exc}")

        case_name = re.sub(r"<[^>]+>", "", str(
            cluster.get("case_name_full") or cluster.get("case_name")
            or item.get("caseName") or name or "")).strip()
        lines: list[tuple] = []
        if case_name:
            lines.append(("title", case_name))

        court_id = str(item.get("court_id") or "").strip().lower()
        court_label = ""
        try:
            import court_catalog
            court_label = court_catalog.COURT_BLUEBOOK.get(court_id, "")
        except Exception:
            court_label = ""
        if court_label:
            lines.append(("lbl", court_label))

        date = str(cluster.get("date_filed") or item.get("dateFiled")
                   or "")[:10]
        if date:
            lines.append(("h", "Decided"))
            lines.append(("", date))

        cites = _gather_all_citations(client, item)
        if cites:
            lines.append(("h", "Citations"))
            lines.append(("", "; ".join(cites)))

        status = str(cluster.get("precedential_status") or "").strip()
        if status:
            lines.append(("h", "Status"))
            lines.append(("", status))

        judges = re.sub(r"<[^>]+>", "", str(cluster.get("judges") or "")).strip()
        if judges:
            lines.append(("h", "Panel"))
            lines.append(("", judges))

        nature = str(cluster.get("nature_of_suit") or "").strip()
        if nature:
            lines.append(("h", "Nature of suit"))
            lines.append(("", nature))

        rel = str(cluster.get("absolute_url") or "").strip()
        if rel:
            url = rel if rel.startswith("http") \
                else "https://www.courtlistener.com" + rel
            lines.append(("", ""))
            lines.append(("", "View on CourtListener →", url))
        return lines

    def _details_lines_recent(self, decisions: list) -> list[tuple]:
        """The recent-decisions panel: each case's name, decision date and
        docket, the holding summary from the Court's homepage, and a link
        opening the slip opinion in the in-app viewer."""
        lines: list[tuple] = [
            ("lbl", "The Court's latest opinions, from supremecourt.gov."),
        ]
        for d in decisions:
            lines.append(("h", d.name))
            sub = " · ".join(p for p in (
                d.date, f"No. {d.docket}" if d.docket else "") if p)
            if sub:
                lines.append(("lbl", sub))
            if d.description:
                lines.append(("", d.description))
            lines.append((
                "", "Open the slip opinion",
                lambda d=d: _SlipOpinionWindow(
                    self._win, d.opinion_url, d.name,
                    self._status_var.set, app=self._app,
                    description=d.description),
            ))
        return lines

    # ------------------------------------------------------------------
    # Side panel: recent Supreme Court decisions (its own view)
    # ------------------------------------------------------------------

    def _show_recent_scotus(self) -> None:
        """The recent-SCOTUS view: cached lines when the fetch already ran,
        else kick off the background load."""
        cached = getattr(self, "_details_recent", None)
        if cached is not None:
            self._apply_details(*cached)
        elif not self._recent_loaded:
            self._load_recent_scotus()
        else:  # fetch still in flight
            self._set_details([("lbl", "Loading recent decisions…")])

    def _apply_recent_scotus(self, title: str, lines: list[tuple]) -> None:
        """Store the fetched recent decisions and show them — unless the user
        has switched the panel to another view meanwhile."""
        self._details_recent = (title, lines)
        if self._details_mode() == "recent":
            self._apply_details(title, lines)

    def _load_recent_scotus(self) -> None:
        """Fetch the Court's most recent decisions (supremecourt.gov) off-thread
        and render them with in-app slip-opinion links."""
        self._recent_loaded = True
        self._set_details([("lbl", "Loading recent decisions…")])

        def run() -> None:
            title = "Recent Supreme Court Decisions"
            try:
                import scotus_recent
                decisions = scotus_recent.fetch_recent_decisions()
                if decisions:
                    lines = self._details_lines_recent(decisions)
                else:
                    lines = [("lbl", "No recent decisions were found on "
                                     "supremecourt.gov.")]
            except Exception as exc:
                print(f"[details] recent decisions: {exc}")
                lines = [("lbl", f"Could not load recent decisions: {exc}")]
            self._post(self._apply_recent_scotus, title, lines)

        threading.Thread(target=run, daemon=True).start()

    def _details_lines_parts(self) -> list[tuple[str, str]]:
        """Authorship gleaned from the Scholar text: the syllabus
        "delivered the opinion … joined" paragraph plus each separate
        opinion's header line."""
        def clean(raw: str) -> str:
            t = re.sub(r"\s+", " ", raw).strip()
            return re.sub(r"^(?:\*\d+\s+)+", "", t)  # leading page markers

        lines: list[tuple[str, str]] = []
        header = next((p for p in self._parts if p.kind == "header"), None)
        if header is not None:
            for b in header.blocks:
                t = clean(b.text())
                if len(t) < 900 and re.search(
                    r"delivered the opinion|announced the judgment"
                    r"|filed (?:a|an) (?:concurring|dissenting)"
                    r"|join(?:ed|ing)\b",
                    t, re.IGNORECASE,
                ):
                    if not lines:
                        lines.append(("h", "Line-up"))
                    lines.append(("", _fix_name_case(t)))
        for part in self._parts:
            if part.kind == "header":
                continue
            if part.kind == "majority":
                lines.append(("h", part.label or "Opinion"))
                for b in part.blocks[:3]:
                    t = clean(b.text())
                    if len(t) <= 200 and re.search(
                        r"delivered the opinion|announced the judgment",
                        t, re.IGNORECASE,
                    ):
                        lines.append(("", _fix_name_case(t)))
                        break
            else:
                lines.append(("h", "Dissent" if part.kind == "dissent"
                              else "Concurrence"))
                lines.append(("", _fix_name_case(clean(part.label))))
        return lines

    @staticmethod
    def _details_lines_oyez(case: "oyez.OyezCase") -> list[tuple]:
        """Render an Oyez case: name + citation, a plain-English summary, the
        majority-vs-dissent line-up (or, when Oyez has no per-justice vote, the
        opinion authors), and links to the oral-argument recording(s) and the
        full Oyez page.  Link lines are 3-tuples (style, text, url)."""
        lines: list[tuple] = []
        if case.name:
            lines.append(("title", case.name))
        if case.citation:
            lines.append(("lbl", case.citation))
        if case.court:
            lines.append(("lbl", case.court))

        about = case.description or case.question
        if about:
            lines.append(("h", "What it's about"))
            lines.append(("", about))
        if case.question and case.question != about:
            lines.append(("h", "Question presented"))
            lines.append(("", case.question))
        # Oyez's "Conclusion" answers the question presented (how the Court
        # resolved it, with the reasoning) — show it directly under the QP.
        if case.conclusion:
            lines.append(("h", "Answer"))
            lines.append(("", case.conclusion))

        if case.has_votes:
            decs = case.voted_decisions
            multi = len(decs) > 1
            for dec in decs:
                # A fractured case decided several questions with different
                # majorities; name each holding so the line-up isn't ambiguous.
                if multi and dec.description:
                    lines.append(("h", "Holding"))
                    lines.append(("", dec.description))
                head = "Decision"
                if dec.vote_line:
                    head += f": {dec.vote_line}"
                if dec.decision_type:
                    head += f" ({dec.decision_type})"
                lines.append(("h", head))
                if dec.winning_party:
                    lines.append(("", f"In favor of {dec.winning_party}"))
                maj, dis, oth = dec.majority, dec.dissent, dec.other
                if maj:
                    lines.append(("h", f"Majority ({len(maj)})"))
                    lines.append(("", ", ".join(j.label for j in maj)))
                if dis:
                    lines.append(("h", f"Dissent ({len(dis)})"))
                    lines.append(("", ", ".join(j.label for j in dis)))
                if oth:
                    lines.append(("h", "Did not participate"))
                    lines.append(("", ", ".join(j.last for j in oth)))
        elif case.opinions:
            # No per-justice vote recorded — show who *wrote* each opinion.
            for kind, label in (("majority", "Majority opinion"),
                                ("concurrence", "Concurrences"),
                                ("dissent", "Dissents")):
                ops = case.opinions_of(kind)
                if ops:
                    lines.append(("h", label))
                    lines.append(("", ", ".join(o.last for o in ops)))
            lines.append(("lbl", "Opinion authors only — Oyez records no "
                                 "per-justice vote for this case."))

        if case.oral_arguments:
            lines.append(("h", "Oral argument"))
            for oa in case.oral_arguments:
                lines.append(("", oa.title, oa.url))

        # The Oyez page carries the rest — full facts, the audio player with
        # synchronized transcript, advocate info.  (No
        # Justia "read the opinion" link: the app is already showing the
        # opinion text, and Oyez's Justia URL is malformed for cases whose
        # U.S. Reports page isn't assigned yet.)
        if case.web_url:
            lines.append(("", ""))
            lines.append(("", "View full details on Oyez →", case.web_url))
        return lines

    # ------------------------------------------------------------------
    # Side panel: related cases (appeals, decision below, remands)
    # ------------------------------------------------------------------

    def _show_related_cases(self) -> None:
        """The related-cases view: cached lines when the lookup already
        ran, else kick off the background load."""
        cached = getattr(self, "_details_related", None)
        if cached is not None:
            self._apply_details(*cached)
        elif not self._related_loaded:
            self._load_related()
        else:  # lookup still in flight
            self._set_details([("lbl", "Finding related cases…")])

    def _apply_related_cases(self, title: str, lines: list[tuple]) -> None:
        """Store the fetched related cases and show them — unless the user
        has switched the panel to another view meanwhile."""
        self._details_related = (title, lines)
        if self._details_mode() == "related":
            self._apply_details(title, lines)

    def _load_related(self) -> None:
        """Fill the related-cases panel off-thread: this case's appellate
        family — the appeal that reviewed it, the decision below, later
        proceedings on remand — assembled by case_lineage from
        CourtListener's docket, caption and citation signals."""
        self._related_loaded = True
        self._set_details([("lbl", "Finding related cases…")])
        client = self._app._get_client()
        if client is None:
            self._apply_related_cases("Related Cases", [
                ("lbl", "Related cases need a CourtListener API token."),
            ])
            return

        item = self._item or {}
        cluster_id = item.get("cluster_id") or item.get("id")
        court_id = (str(item.get("court_id") or "").strip().lower()
                    or _scholar_court_id(self._blocks))
        name = re.sub(r"<[^>]+>", "", str(
            item.get("caseName") or item.get("case_name") or "")).strip()
        if not name:
            name = _scholar_caption_name(self._blocks) or self._bb.get(
                "name", "")
        cites: list[str] = []
        for c in ([self._bb.get("cite", "")] + list(self._header_cites)
                  + list(item.get("citation") or [])):
            c = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", str(c))).strip()
            if c and c not in cites:
                cites.append(c)
        date = str(item.get("dateFiled") or item.get("date_filed")
                   or "")[:10]
        docket_number = str(item.get("docketNumber")
                            or item.get("docket_number") or "")
        text = self._scholar_text or self._cl_text or ""

        def run() -> None:
            import case_lineage
            try:
                lineage = case_lineage.find_related(
                    client, cluster_id=cluster_id, case_name=name,
                    court_id=court_id, citations=cites, date_filed=date,
                    docket_number=docket_number, opinion_text=text)
                lines = self._related_lines(lineage)
            except Exception as exc:
                print(f"[related] lookup failed: {exc}")
                lines = [("lbl", f"Related-case lookup failed: {exc}")]
            self._post(self._apply_related_cases, "Related Cases", lines)

        threading.Thread(target=run, daemon=True).start()

    def _related_lines(self, lineage) -> list[tuple]:
        """Render a case_lineage.Lineage: each related decision is a link
        opening the opinion, grouped by its relationship to this case,
        with the docket's appeal history beneath."""
        import case_lineage
        lines: list[tuple] = []
        groups: dict[str, list] = {}
        for rc in lineage.related:
            groups.setdefault(rc.relation, []).append(rc)
        for rel in case_lineage.REL_ORDER:
            rcs = groups.pop(rel, [])
            if not rcs:
                continue
            lines.append(("h", rel))
            for rc in rcs:
                lines.append(("", rc.case_name,
                              lambda rc=rc: self._open_related_case(rc)))
                sub = " · ".join(p for p in (
                    rc.court_label, rc.date, rc.citation) if p)
                if rc.confidence != "confirmed":
                    sub = f"{sub} · probable match" if sub \
                        else "probable match"
                if sub:
                    lines.append(("lbl", sub))
        if lineage.events:
            lines.append(("h", "Docket history"))
            for date, text in lineage.events:
                lines.append(("lbl", f"{date}  {text}"))
        for note in lineage.notes:
            lines.append(("lbl", note))
        if not lines:
            lines = [("lbl", "No appeal, decision below, or later "
                             "proceeding found for this case on "
                             "CourtListener.")]
        return lines

    def _open_related_case(self, rc) -> None:
        """Open a related case in a new opinion window — straight from its
        CourtListener cluster when known, else by citation, else its
        courtlistener.com page in the browser."""
        client = self._app._get_client()
        if rc.cluster_id and client is not None:
            item = {"cluster_id": rc.cluster_id, "court_id": rc.court_id,
                    "caseName": rc.case_name, "dateFiled": rc.date}
            if rc.citation:
                item["citation"] = [rc.citation]
            self._status_var.set(
                f"Fetching {rc.case_name} from CourtListener…")

            def run() -> None:
                try:
                    parts, blocks, plain, _cluster = _assemble_case_parts(
                        client, item)
                    self._post(self._on_cl_link_ready, parts, blocks,
                               plain, item)
                except Exception as exc:
                    self._post(self._on_cl_link_error, str(exc))

            threading.Thread(target=run, daemon=True).start()
            return
        if rc.citation:
            self._follow_cite_via_cl(rc.citation, name=rc.case_name)
        elif rc.url:
            webbrowser.open(rc.url)
            self._status_var.set("Opened on CourtListener in your browser.")

    # ------------------------------------------------------------------
    # Side panel: detected outline
    # ------------------------------------------------------------------

    _OUTLINE_PART_STYLES = {"majority": "olmaj", "concurrence": "olconc",
                            "dissent": "oldiss"}

    def _show_outline(self) -> None:
        """The outline view: each opinion part (colored like the part map)
        and the section headings inside it — SCOTUS's bare "II"/"A"/"1"
        markers and other courts' explanatory headings — as links that jump
        the reader to that spot."""
        entries = self._collect_outline()
        lines: list[tuple] = []
        if not entries:
            lines.append(("lbl", "No outline detected in this opinion."))
        else:
            lines.append(("lbl", "Detected outline — click to jump."))
            for level, style, text, pos in entries:
                lines.append((style, "   " * level + text,
                              lambda p=pos: self._outline_jump(p)))
        self._apply_details("Outline", lines)

    def _outline_jump(self, pos: str) -> None:
        """Scroll the reader so *pos* tops the view, and flash its line."""
        try:
            self._text.yview(pos)
        except tk.TclError:
            return
        self._jump_to(pos)

    def _refresh_outline_panel(self) -> None:
        """Rebuild the outline after a re-render — its stored text indices
        go stale whenever the text is rebuilt (part switch, source toggle)."""
        if (self._details_frame is not None and self._details_var.get()
                and self._details_mode() == "outline"):
            self._show_outline()

    def _collect_outline(self) -> list[tuple[int, str, str, str]]:
        """Outline entries of the rendered text, in document order:
        (indent level, panel style tag, display text, text index).  Level 0
        is an opinion part; headings nest roman → letter → number the way
        Supreme Court opinions subdivide (II → A → 1)."""
        txt = getattr(self, "_text", None)
        if txt is None:
            return []
        entries: list[tuple[tuple[int, int], int, str, str, str]] = []
        parts = getattr(self, "_rendered_parts", None) or []
        for rs, _rend, pi in self._part_region_indices():
            if pi >= len(parts):
                continue
            part = parts[pi]
            label = re.sub(r"\s+", " ", part.label or "").strip() or "Part"
            if len(label) > 110:
                label = label[:107] + "…"
            style = self._OUTLINE_PART_STYLES.get(part.kind, "olpart")
            entries.append((_tk_ix(rs), 0, style, label, rs))
        for tag in ("heading", "center"):
            ranges = txt.tag_ranges(tag)
            for i in range(0, len(ranges), 2):
                start = str(ranges[i])
                text = txt.get(start, str(ranges[i + 1]))
                off = 0  # adjacent blocks share one tag range; walk its lines
                for line in text.split("\n"):
                    t = re.sub(r"^(?:\*\d+\s*)+", "", line).strip()
                    level = self._outline_level(t, tag)
                    if level is not None:
                        pos = txt.index(f"{start}+{off}c")
                        entries.append((_tk_ix(pos), level, "", t, pos))
                    off += len(line) + 1
        entries.sort(key=lambda e: e[0])
        return [(level, style, text, pos)
                for _ix, level, style, text, pos in entries]

    @staticmethod
    def _outline_level(t: str, tag: str) -> Optional[int]:
        """The outline depth of one heading/centered line, or None when the
        line isn't a heading (a caption line, a "* * *" divider…)."""
        def depth(marker: str) -> int:
            if marker.isdigit():
                return 3
            if len(marker) == 1 and marker not in "IVX":
                return 2  # a lettered subsection ("A"), not a roman numeral
            return 1
        if not t:
            return None
        if tag == "heading":
            if len(t) > 80:
                return None  # a paragraph that merely carries the tag
            m = re.match(r"([IVXLCDM]+|[A-Z]|\d{1,2})[.)]\s+\S", t)
            return depth(m.group(1)) if m else 1
        # Centered lines: only the bare section markers count ("II", "A.").
        m = _BARE_HEADING_RE.match(t)
        if not m:
            return None
        lead = re.match(r"[IVXLCDM]+|[A-Z]|\d{1,2}", t)
        return depth(lead.group(0)) if lead else 1

    # ------------------------------------------------------------------
    # Citation links
    # ------------------------------------------------------------------

    def _post(self, fn, *args) -> None:
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass  # window closed while a background fetch was running

    def _jump_to(self, pos: str) -> None:
        txt = self._text
        txt.see(pos)
        txt.tag_remove("jumpflash", "1.0", "end")
        txt.tag_add("jumpflash", f"{pos} linestart", f"{pos} lineend")
        self._win.after(
            1400, lambda: txt.tag_remove("jumpflash", "1.0", "end")
        )

    def _flash_range(self, start: str, end: str) -> None:
        """Scroll to *start* and briefly highlight everything in ``[start,
        end)`` — used to flash a whole pin-cited page, not just its line."""
        txt = self._text
        txt.see(start)
        txt.tag_remove("jumpflash", "1.0", "end")
        txt.tag_add("jumpflash", start, end)
        self._win.after(
            1400, lambda: txt.tag_remove("jumpflash", "1.0", "end")
        )

    def _follow_link(self, tag: str) -> None:
        action = self._link_actions.get(tag)
        if not action:
            return
        kind, value = action
        if kind == "fnref":
            pos = self._fn_def_pos.get(value)
            if pos:
                self._jump_to(pos)
            return
        if kind == "fndef":
            pos = self._fn_ref_pos.get(value)
            if pos:
                self._jump_to(pos)
            return
        if kind in _STATUTE_SOURCES:
            self._open_statute(kind, value)
            return
        if kind == "browse":
            # Link-out to an external source (e.g. a state statute we don't
            # render in-app); open it in the user's browser.
            webbrowser.open(value)
            self._status_var.set("Opened in your browser.")
            return
        if kind == "statpdf":
            _open_statute_pdf(
                self._win, value, self._status_var.set, app=self._app
            )
            return
        if kind == "engrep":
            _open_eng_rep(self._win, value, self._status_var.set,
                          app=self._app)
            return
        if kind == "recap":
            _open_recap_citation(
                self._app, self._win, value, self._status_var.set)
            return
        if kind == "fedcas":
            _open_fedcas_citation(
                self._app, self._win, value, self._status_var.set)
            return
        # A Scholar case URL may carry a pincite, a reporter cite, and the case
        # name ("<url>\tpin=565\tcite=…\tname=…") so a failed/blocked fetch can
        # still be located on CourtListener.
        name = ""
        if kind == "url":
            pieces = value.split("\t")
            url_val = pieces[0]
            pin = ""
            cite = ""
            for piece in pieces[1:]:
                if piece.startswith("pin="):
                    pin = piece[4:]
                elif piece.startswith("cite="):
                    cite = piece[5:]
                elif piece.startswith("name="):
                    name = piece[5:]
        elif kind == "cite":
            cite, _, pin = value.partition("@")
            url_val = ""
        else:
            cite, pin, url_val = value, "", ""
        # CourtListener opinion URL: fetch structured text from CL directly
        if kind == "url" and "courtlistener.com/opinion/" in url_val:
            self._follow_cl_link(url_val)
            return
        # Federal Appendix citations are scans Google Scholar lacks — open the
        # official static.case.law PDF straight from the citation rather than
        # fetching (often the wrong) Scholar text.  Covers both plain-text
        # F. App'x cites and Scholar links we rewrote to ("cite", …).
        if kind == "cite" and _FED_APPX_RE.search(cite):
            appx = _static_case_law_url(cite)
            if appx:
                self._status_var.set(f"Opening {cite} (case.law)…")
                _open_case_law_pdf(
                    self._win, appx,
                    cite + (f" at {pin}" if pin else ""),
                    self._status_var.set, app=self._app,
                )
                return
        fetcher = self._app._get_scholar()
        label = cite if kind == "cite" else "cited case"
        if fetcher is None:
            # No Google Scholar — go straight to CourtListener / case.law if we
            # have anything to locate the case with.
            if cite or name:
                self._follow_cite_via_cl(cite, pin, name=name)
            else:
                self._status_var.set("Google Scholar is not available.")
            return
        self._status_var.set(f"Fetching {label} from Google Scholar…")

        def run() -> None:
            # Any Google Scholar failure — a None result *or* an exception from
            # a blocked/erroring request — routes to the same CourtListener /
            # case.law fallback via _on_link_ready.
            try:
                if kind == "url":
                    result = fetcher.fetch_by_url(url_val)
                else:
                    result = fetcher.fetch_by_citation(cite)
            except Exception as exc:
                print(f"[scholar] link fetch failed: {exc}")
                result = None
            self._post(self._on_link_ready, result, cite, pin, url_val, name)

        threading.Thread(target=run, daemon=True).start()

    def _follow_cl_link(self, url: str) -> None:
        """Open a CourtListener opinion URL with structured block rendering."""
        client = self._app._get_client()
        if client is None:
            return
        m = re.search(r"/opinion/(\d+)/", url)
        if not m:
            return
        opinion_id = m.group(1)
        self._status_var.set("Fetching opinion from CourtListener…")

        def run() -> None:
            try:
                op = client.get_opinion(
                    int(opinion_id),
                    fields="cluster,html_with_citations,html,plain_text",
                )
                cluster_url = op.get("cluster") or ""
                cm = re.search(r"/(\d+)/", cluster_url)
                if cm:
                    item = {"cluster_id": cm.group(1)}
                    parts, blocks, plain, cluster = _assemble_case_parts(
                        client, item,
                    )
                    self._post(self._on_cl_link_ready, parts, blocks, plain, item)
                else:
                    self._post(self._on_cl_link_error, "Could not resolve cluster.")
            except Exception as exc:
                self._post(self._on_cl_link_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _on_cl_link_ready(self, parts, blocks, plain, item, retry=None) -> None:
        self._status_var.set("Cited case loaded from CourtListener.")
        win = _ScholarTextWindow(
            self._win, self._app, "", "",
            item=item, cl_text=plain,
            cl_parts=parts, cl_blocks=blocks,
        )
        if retry:
            # A Google Scholar link failed; keep retrying it and light up the
            # new window's "Google Scholar Text" button if it comes through.
            win._retry_scholar_link(*retry)

    def _on_cl_link_error(self, msg: str) -> None:
        self._status_var.set(f"CourtListener: {msg}")

    def _try_case_law_link_pdf(self, cite: str, pin: str = "",
                               name: str = "") -> bool:
        if not cite:
            return False
        pdf = _case_law_pdf_for_cite(cite)
        if not pdf:
            return False
        self._post(self._open_cited_case_pdf, pdf, cite, pin, name)
        return True

    def _follow_cite_via_cl(self, cite: str, pin: str = "", name: str = "",
                            retry=None) -> None:
        """Follow a cited-case link Google Scholar can't supply — whether it is
        missing, flaky, or blocking.  Resolve in order of how surely each source
        is the opinion *at* the clicked cite: CourtListener by citation, then
        the citation-keyed static.case.law PDF, and only then a (fuzzy)
        CourtListener name search — so a cite CourtListener lacks opens the
        right case.law scan rather than a same-party namesake (clicking "5
        Johns. 37", Kilburn v. Woodworth, must never open Kilbourn v. Thompson).

        ``retry`` (cite, pin, url, name) keeps trying Google Scholar in the
        background after the CourtListener view opens (used when a Scholar link
        failed rather than Scholar being absent)."""
        client = self._app._get_client()
        if client is None:
            return
        label = name or cite or "cited case"
        self._status_var.set(
            f"Google Scholar busy — loading {label} from CourtListener…"
            if retry else f"Fetching {label} from CourtListener…"
        )

        def run() -> None:
            try:
                # CourtListener by the cite as printed and — for an old
                # nominative SCOTUS cite — by its modern "U.S." form; then the
                # citation-keyed case.law PDF; and only as a last resort the
                # (fuzzy) case name.
                target = (_cl_item_for_citation(client, cite, name=name)
                          if cite else None)
                if target is None and cite:
                    alt = _us_reports_cite(cite)
                    if alt:
                        target = _cl_item_for_citation(client, alt, name=name)
                if target is None and cite:
                    # CourtListener has no cluster at this exact citation.
                    # Prefer the citation-keyed static.case.law PDF — the
                    # opinion *at* that cite — over the name search below, which
                    # ranks by party name and can surface a different case:
                    # clicking "5 Johns. 37" (Kilburn v. Woodworth) must not
                    # open the unrelated "Kilbourn v. Thompson, 103 U.S. 168".
                    pdf = _case_law_pdf_for_cite(cite)
                    if pdf:
                        self._post(
                            self._open_cited_case_pdf, pdf, cite, pin, name)
                        return
                if target is None and name:
                    target = _cl_item_for_name(client, name)
                if not target:
                    # Nothing keyed to the cite and no name match — keep
                    # retrying Google Scholar and open it if it comes through,
                    # else report nothing found.
                    if retry:
                        self._post(self._retry_scholar_only, *retry)
                    else:
                        self._post(self._on_cl_link_error, f"No match for {label}.")
                    return
                parts, blocks, plain, cluster = _assemble_case_parts(
                    client, target,
                )
                self._post(
                    self._on_cl_link_ready, parts, blocks, plain, target, retry,
                )
            except Exception as exc:
                if self._try_case_law_link_pdf(cite, pin, name):
                    return
                if retry:
                    self._post(self._retry_scholar_only, *retry)
                else:
                    self._post(self._on_cl_link_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _open_cited_case_pdf(self, url: str, cite: str, pin: str = "",
                             expected_name: str = "") -> None:
        """Open a cited case's static.case.law PDF (the fallback when neither
        Google Scholar nor CourtListener has the opinion)."""
        self._status_var.set(f"Opening {cite} (case.law PDF)…")
        _open_case_law_pdf(
            self._win, url, cite + (f" at {pin}" if pin else ""),
            self._status_var.set, app=self._app,
            expected_name=expected_name,
        )

    def _on_link_ready(self, result: Optional[tuple[str, str]],
                       cite: str = "", pin: str = "", url_val: str = "",
                       name: str = "") -> None:
        if not result:
            self._link_scholar_failed(cite, pin, url_val, name)
            return
        url, html = result
        self._status_var.set("Cited case loaded.")
        win = _ScholarTextWindow(self._win, self._app, url, html, item=None)
        if pin:  # cite or Scholar-URL pincite — jump once the window lays out
            win.jump_to_cite_page(cite, pin)

    def _link_scholar_failed(self, cite: str, pin: str, url_val: str,
                             name: str = "") -> None:
        """A Google Scholar opinion link failed — missing, flaky, or blocking.
        Show the CourtListener view now if the case can be located by citation
        or by name (else its case.law PDF), and keep retrying Google Scholar in
        the background; if it comes through, the window's "Google Scholar Text"
        button lights up so the reader can switch to it."""
        client = self._app._get_client()
        fetcher = self._app._get_scholar()
        if (cite or name) and client is not None:
            # Open CourtListener / case.law now; retry Scholar in the background.
            self._follow_cite_via_cl(
                cite, pin, name=name, retry=(cite, pin, url_val, name),
            )
        elif cite:
            self._status_var.set(f"Checking case.law for {cite}…")

            def run() -> None:
                if self._try_case_law_link_pdf(cite, pin, name):
                    return
                if fetcher is not None and (url_val or cite):
                    self._post(self._retry_scholar_only, cite, pin, url_val, name)
                else:
                    self._post(
                        self._status_var.set,
                        "Google Scholar: cited case not found (or blocked).",
                    )

            threading.Thread(target=run, daemon=True).start()
        elif fetcher is not None and (url_val or cite):
            # Nothing to locate the case with — just keep retrying Google
            # Scholar, and open it if it comes through.
            self._retry_scholar_only(cite, pin, url_val, name)
        else:
            self._status_var.set(
                "Google Scholar: cited case not found (or blocked)."
            )

    def jump_to_cite_page(self, cite: str, pin: str) -> None:
        """Scroll to the pin page and briefly flash the *whole* page — from its
        star-pagination marker to the next page's marker (or, on the last page,
        to the footnotes / end of the opinion) — so the cited passage stands out
        rather than just the marker's line.  Deferred until the window has laid
        out (an immediate ``see`` on an unmapped widget does nothing), with one
        retry while the text is still rendering."""
        m_page = re.match(r"\d+", pin or "")
        if not m_page:
            return
        page = int(m_page.group(0))

        def do(attempt: int = 0) -> None:
            try:
                pos = (self._page_pos or {}).get(page)
            except tk.TclError:
                return
            if pos:
                txt = self._text
                # End of the flash: the nearest later star-page marker, else
                # the start of the footnotes, else the end of the text.
                later = [txt.index(p) for p in self._page_pos.values()
                         if txt.compare(p, ">", pos)]
                if later:
                    end = min(later,
                              key=lambda ix: tuple(map(int, ix.split("."))))
                else:
                    fn = txt.tag_nextrange("fnhead", pos)
                    end = fn[0] if fn else "end-1c"
                self._flash_range(pos, end)
                self._status_var.set(f"Jumped to page *{page}.")
            elif attempt < 2:
                self._win.after(250, lambda: do(attempt + 1))
            else:
                self._status_var.set(f"Page *{page} not marked in this text.")

        self._win.after(200, do)

    def _open_statute(self, kind: str, spec: str) -> None:
        """Fetch a U.S. Code (OLRC) or C.F.R. (eCFR) section and show it."""
        _fetch_statute_window(
            self._win, kind, spec, self._status_var.set, app=self._app
        )

    # ------------------------------------------------------------------
    # CourtListener toggle
    # ------------------------------------------------------------------

    def _toggle_source(self) -> None:
        if self._mode == "courtlistener":
            if self._cl_primary and not self._scholar_url:
                return
            self._render_scholar()
            return
        if self._cl_parts:
            self._render_cl_blocks()
            return
        if self._cl_text is not None:
            self._show_courtlistener()
            return
        client = self._app._get_client()
        if client is None:
            return
        self._toggle_btn.configure(state="disabled")
        self._status_var.set("Fetching CourtListener text…")
        item = dict(self._item)
        cite = self._bb["cite"]
        case_name = self._bb.get("name", "")

        def run() -> None:
            try:
                target = item
                if not (target.get("cluster_id") or target.get("id")):
                    if not cite:
                        raise RuntimeError(
                            "No citation available to locate this case on CourtListener."
                        )
                    target = _cl_item_for_citation(client, cite, name=case_name)
                    if not target:
                        raise RuntimeError(f"No CourtListener match for {cite!r}.")
                parts, blocks, plain, cluster = _assemble_case_parts(
                    client, target,
                )
                text = _assemble_case_text(client, target) if not plain else plain
                self._post(self._on_cl_ready, text, parts, blocks)
            except Exception as exc:
                self._post(self._on_cl_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _on_cl_ready(self, text: str, parts=None, blocks=None) -> None:
        self._cl_text = text
        if parts:
            self._cl_parts = parts
            self._cl_blocks = blocks
            self._render_cl_blocks()
        else:
            self._show_courtlistener()

    def _on_cl_error(self, msg: str) -> None:
        self._toggle_btn.configure(state="normal")
        self._status_var.set(f"CourtListener: {msg}")
        messagebox.showerror("CourtListener", msg, parent=self._win)

    def _search_for_scholar_version(self, cl_text: Optional[str] = None) -> None:
        """The CourtListener text is showing because Google Scholar's first
        case didn't match; keep hunting for a matching Scholar opinion in the
        background and, when one turns up, light up the "Google Scholar Text"
        button so the reader can switch to it."""
        if not _SCHOLAR_AVAILABLE:
            return
        if cl_text is not None and self._cl_text is None:
            self._cl_text = cl_text
        app = self._app
        fetcher = app._get_scholar()
        if fetcher is None:
            return
        client = app._get_client()
        item = dict(self._item)
        if not (item.get("cluster_id") or item.get("id")):
            return

        def run() -> None:
            try:
                result, _cl, note = _find_scholar_for_item(
                    client, fetcher, item, lambda _m: None,
                )
            except Exception as exc:
                print(f"[scholar] background match search failed: {exc}")
                result, note = None, ""
            if result:
                url, html = result
                self._post(
                    self._attach_scholar_version, url, html,
                    note or "matching Google Scholar version found",
                )

        threading.Thread(target=run, daemon=True).start()

    def _attach_scholar_version(
        self, url: str, html: str, note: str = "",
    ) -> None:
        """Wire a (later-found) matching Google Scholar opinion into a window
        that opened on the CourtListener text, and enable the toggle so the
        reader can switch over."""
        try:
            if not self._win.winfo_exists():
                return
        except tk.TclError:
            return
        blocks = parse_opinion_blocks(html)
        text = blocks_to_text(blocks) or _strip_html(html)
        if not (text or "").strip():
            return
        self._scholar_url = url
        self._blocks = blocks
        self._scholar_text = text
        self._parts = segment_blocks(blocks)
        self._refine_part_labels(self._parts)
        # Per-part starting pages, for pin cites when a single part is shown.
        self._part_start_pages = []
        page: Optional[int] = None
        for part in self._parts:
            self._part_start_pages.append(page)
            for b in part.blocks:
                for s in b.spans:
                    if s.pagenum:
                        m = re.search(r"\d+", s.text)
                        if m:
                            page = int(m.group(0))
        self._scholar_has_text = len(re.sub(r"\s+", "", text or "")) >= 500
        self._cl_primary = False
        if note:
            self._note = note
        # Showing the CourtListener text: enable the switch-to-Scholar button.
        # (If the PDF is up, its button is left alone — the Scholar text is
        # picked up when the reader returns to the text view.)
        if self._mode == "courtlistener":
            _style_ui_button(self._toggle_btn, primary=False)
            self._toggle_btn.configure(
                text="Google Scholar Text", command=self._toggle_source,
                state="normal",
            )
            char_count = len(self._cl_text or self._scholar_text or "")
            try:
                self._status_var.set(
                    f"{char_count:,} characters | CourtListener version | "
                    f"Google Scholar version available — use the button to switch"
                )
            except tk.TclError:
                pass

    def _retry_scholar_link(
        self, cite: str, pin: str, url_val: str, name: str = "",
        attempts: int = 3, delay: float = 4.0,
    ) -> None:
        """This window opened on the CourtListener text because a Google Scholar
        link failed.  First try the case's *parallel* standard-reporter
        citations — Google Scholar often holds an opinion under the regional
        or S. Ct. reporter when the cite clicked (an official state reporter,
        an old nominative form) finds nothing.  ``fetch_by_citation`` only
        accepts a result bearing the queried cite, and a parallel cite of
        this very cluster identifies exactly this case, so a hit counts as a
        verified match: it is cached under the cluster's verified key and
        wired in via ``_attach_scholar_version``, lighting up the "Google
        Scholar Text" button.  Failing that, retry the original fetch
        ``attempts`` more times, ``delay`` seconds apart.

        (``name`` is unused — it's accepted so the shared ``retry`` tuple
        ``(cite, pin, url, name)`` unpacks cleanly.)"""
        fetcher = self._app._get_scholar()
        if fetcher is None or not (url_val or cite):
            return
        client = self._app._get_client()
        item = dict(self._item) if self._item else {}

        def attach(url: str, html: str, note: str) -> None:
            cluster_id = item.get("cluster_id") or item.get("id")
            if cluster_id:
                try:
                    fetcher.put_cached(
                        f"verified:cluster:{cluster_id}", url, html)
                except Exception:
                    pass
            self._post(self._attach_scholar_version, url, html, note)

        def run() -> None:
            for alt in _scholar_parallel_cites(client, item, cite):
                try:
                    result = fetcher.fetch_by_citation(alt)
                except Exception:
                    result = None
                if result:
                    attach(*result,
                           f"Google Scholar version found at {alt}")
                    return
            for _ in range(attempts):
                time.sleep(delay)
                try:
                    result = (fetcher.fetch_by_url(url_val) if url_val
                              else fetcher.fetch_by_citation(cite))
                except Exception:
                    result = None
                if result:
                    url, html = result
                    self._post(
                        self._attach_scholar_version, url, html,
                        "matching Google Scholar version found",
                    )
                    return

        threading.Thread(target=run, daemon=True).start()

    def _retry_scholar_only(
        self, cite: str, pin: str, url_val: str, name: str = "",
        attempts: int = 3, delay: float = 4.0,
    ) -> None:
        """The last resort for a link nothing else could locate (a Google
        Scholar URL with no citation or name, or one CourtListener and case.law
        both lacked): retry Google Scholar a few times and open the opinion if
        it comes through."""
        fetcher = self._app._get_scholar()
        if fetcher is None:
            self._status_var.set("Google Scholar: cited case not found.")
            return
        self._status_var.set("Google Scholar busy — retrying this link…")

        def run() -> None:
            for _ in range(attempts):
                time.sleep(delay)
                try:
                    result = (fetcher.fetch_by_url(url_val) if url_val
                              else fetcher.fetch_by_citation(cite))
                except Exception:
                    result = None
                if result:
                    self._post(
                        self._on_link_ready, result, cite, pin, url_val, name,
                    )
                    return
            self._post(
                self._status_var.set,
                "Google Scholar still unavailable for this link.",
            )

        threading.Thread(target=run, daemon=True).start()

    # ------------------------------------------------------------------
    # PDF view (official opinion PDF, shown in-app)
    # ------------------------------------------------------------------

    def _pdf_item(self) -> dict:
        """The search-result-shaped dict used to resolve a PDF URL.  Falls back
        to the Bluebook citation when this window wasn't opened from a result."""
        item = dict(self._item) if self._item else {}
        if not (item.get("caseName") or item.get("case_name")):
            # A Scholar-only window may have no CourtListener item, but its
            # parsed/Bluebooked caption is still authoritative enough to pick
            # the right ``-01``/``-02`` CAP opinion on a shared reporter page.
            item["caseName"] = self._bb.get("name", "")
        cites: list[str] = []
        raw = item.get("citation")
        if raw:
            cites = list(raw) if isinstance(raw, list) else [raw]
        # Add every parallel reporter cite from the header and the chosen
        # Bluebook cite, so the resolver tries them all (point: Scholar cases
        # list several reporters above the case name).
        for c in list(self._header_cites) + [self._bb.get("cite", "")]:
            if c and c not in cites:
                cites.append(c)
        if cites:
            item["citation"] = cites
        return item

    # Federal Appendix reporter: "F. App'x", "F.App'x", "Fed. Appx.", etc.
    # (straight or typographic apostrophe).
    _FED_APPX_RE = re.compile(r"F(?:ed)?\.?\s*App['’]?x\.?", re.IGNORECASE)

    def _is_fed_appx(self) -> bool:
        """True if any known citation places this case in the Federal Appendix
        (a scan-only reporter that Google Scholar seldom has text for)."""
        cites = list(self._header_cites)
        if self._bb.get("cite"):
            cites.append(self._bb["cite"])
        raw = self._item.get("citation") if self._item else None
        if raw:
            cites += raw if isinstance(raw, list) else [raw]
        return any(self._FED_APPX_RE.search(str(c)) for c in cites)

    # ------------------------------------------------------------------
    # CourtListener-view "View PDF" button
    # ------------------------------------------------------------------

    def _remember_case_law_pdf_choices(self, item: dict) -> None:
        choices = item.get("_case_law_pdf_choices") or []
        self._case_law_pdf_choices = list(choices)
        self._post(self._refresh_pdf_button)

    def _post_pdf_menu(self) -> None:
        choices = list(getattr(self, "_case_law_pdf_choices", []) or [])
        if len(choices) <= 1:
            self._view_pdf()
            return
        btn = self._active_pdf_button()
        if btn is None:
            return
        menu = tk.Menu(self._win, tearoff=0)
        for choice in choices:
            menu.add_command(
                label=choice.label or choice.cite,
                command=lambda c=choice: self._view_pdf_choice(c),
            )
        try:
            menu.tk_popup(btn.winfo_rootx(), btn.winfo_rooty() + btn.winfo_height())
        finally:
            menu.grab_release()

    def _view_pdf_choice(self, choice: _CaseLawPdfChoice) -> None:
        """Open a specific reporter scan chosen from the View PDF menu."""
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            self._pdf_url = choice.url
            if messagebox.askyesno(
                "PDF viewer not installed",
                "Viewing PDFs inside the app needs two Python packages:\n\n"
                "    pip install pypdfium2 Pillow\n\n"
                "Open the PDF in your web browser instead?",
                parent=self._win,
            ):
                webbrowser.open(choice.url)
            return
        if self._mode in ("scholar", "courtlistener"):
            self._pre_pdf_mode = self._mode
        if self._pdf_prefetch is not None and self._pdf_prefetch[1] == choice.url:
            data, url = self._pdf_prefetch
            self._show_pdf(data, url)
            return
        busy = self._active_pdf_button()
        try:
            if busy is not None:
                busy.configure(state="disabled")
        except tk.TclError:
            pass
        self._pdf_url = choice.url
        self._status_var.set(f"Opening PDF for {choice.cite}...")
        client = self._app._get_client()

        def run() -> None:
            try:
                fetched = _fetch_pdf_bytes(choice.url, client=client, timeout=30)
                if fetched is None:
                    self._post(
                        self._on_pdf_error,
                        "The source returned something that isn't a PDF.",
                    )
                    return
                data, final_url = fetched
                self._pdf_url = final_url
                self._post(self._show_pdf, data, final_url)
            except Exception as exc:
                self._post(self._on_pdf_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _pack_courtlistener_action_buttons(self) -> None:
        """Pack right-side actions so View PDF stays second from the right."""
        scholar = getattr(self, "_toggle_btn", None)
        pdf = getattr(self, "_pdf_btn", None)
        if scholar is None or pdf is None:
            return
        for btn in (scholar, pdf):
            try:
                if btn.winfo_ismapped():
                    btn.pack_forget()
            except tk.TclError:
                return
        pdf.pack(side="right", padx=4)
        scholar.pack(side="right", padx=4)

    def _show_pdf_button(self) -> None:
        """Reveal the View PDF button (CourtListener text view), kicking off the
        background PDF search the first time so it ends up enabled or greyed."""
        btn = self._active_pdf_button()
        if btn is None:
            return
        self._pack_courtlistener_action_buttons()
        self._refresh_pdf_button()
        self._locate_pdf()

    def _hide_pdf_button(self) -> None:
        btn = getattr(self, "_pdf_btn", None)
        if btn is not None and btn.winfo_ismapped():
            btn.pack_forget()

    def _can_show_courtlistener(self) -> bool:
        """Whether a CourtListener view is reachable from this Scholar window —
        the original CL opinion it opened from, or a cluster/reporter citation
        to fetch the equivalent."""
        return bool(
            self._cl_parts or self._cl_text is not None
            or self._item.get("cluster_id") or self._item.get("id")
            or (self._bb.get("cite") if getattr(self, "_bb", None) else "")
        )

    def _show_cl_button(self) -> None:
        """Reveal the 'CourtListener Text' button in the Scholar view when a
        CourtListener view can be reached (the mirror of _show_pdf_button)."""
        btn = getattr(self, "_cl_btn", None)
        if btn is None:
            return
        _style_ui_button(btn, primary=False)
        if self._can_show_courtlistener():
            if not btn.winfo_ismapped():
                btn.pack(side="right", padx=4)
        elif btn.winfo_ismapped():
            btn.pack_forget()

    def _hide_cl_button(self) -> None:
        btn = getattr(self, "_cl_btn", None)
        if btn is not None and btn.winfo_ismapped():
            btn.pack_forget()

    def _active_pdf_button(self):
        if getattr(self, "_mode", None) == "scholar":
            return getattr(self, "_toggle_btn", None)
        return getattr(self, "_pdf_btn", None)

    def _refresh_pdf_button(self) -> None:
        btn = self._active_pdf_button()
        if btn is None:
            return
        try:
            if self._pdf_located is True or self._pdf_prefetch is not None:
                choices = getattr(self, "_case_law_pdf_choices", []) or []
                label = "View PDF ▾" if len(choices) > 1 else "View PDF"
                command = self._post_pdf_menu if len(choices) > 1 else self._view_pdf
                _style_ui_button(btn, primary=True)
                btn.configure(state="normal", text=label, command=command)
            elif self._pdf_located is False:
                _style_ui_button(btn, primary=False)
                btn.configure(state="disabled", text="No PDF")
            else:
                _style_ui_button(btn, primary=False)
                btn.configure(state="disabled", text="Finding PDF...")
            self._apply_button_bar_compact()
        except tk.TclError:
            pass

    def _locate_pdf(self) -> None:
        """Find the opinion's PDF in the background — every PDF path the app
        knows (LOC/GovInfo US Reports, static.case.law, the original court
        source), then CourtListener's stored copy — recording whether one
        exists so the View PDF button is enabled or left greyed out.  Warms the
        bytes for an instant view when that's allowed and the libs are present.
        """
        if (self._pdf_locate_started or self._pdf_prefetch is not None
                or (self._pdf_located is True and self._pdf_url)):
            if self._pdf_prefetch is not None or self._pdf_url:
                self._pdf_located = True
                self._refresh_pdf_button()
            return
        self._pdf_locate_started = True
        client = (self._app._get_client()
                  if self._app._token_var.get().strip() else None)
        item = self._pdf_item()

        def run() -> None:
            url = None
            try:
                url = self._app._resolve_pdf_url(client, item)
                self._remember_case_law_pdf_choices(item)
            except Exception as exc:
                print(f"[pdf] resolve failed: {exc}")
            if not url:
                self._post(self._on_pdf_located, False)
                return
            self._pdf_url = url
            # Warm the bytes for an instant view (skipped when a big PDF is
            # already live — prefetch_pdf=False — or the render libs are absent).
            if self._prefetch_ok:
                try:
                    import pypdfium2  # noqa: F401
                    from PIL import ImageTk  # noqa: F401
                    fetched = _fetch_pdf_bytes(url, client=client, timeout=30)
                    if fetched is not None:
                        data, final_url = fetched
                        self._pdf_prefetch = (data, final_url)
                        self._pdf_url = final_url
                except ImportError:
                    pass  # no render libs; View PDF will offer the browser
                except Exception as exc:
                    print(f"[pdf] prefetch failed: {exc}")
            self._post(self._on_pdf_located, True)

        threading.Thread(target=run, daemon=True).start()

    def _on_pdf_located(self, found: bool) -> None:
        self._pdf_located = found
        self._refresh_pdf_button()

    def _render_courtlistener_view(self) -> None:
        """Re-show whichever CourtListener text rendering this window uses."""
        if self._cl_parts or self._cl_blocks:
            self._render_cl_blocks()
        else:
            self._show_courtlistener()

    def _prefetch_pdf(self) -> None:
        """Resolve and fetch the official PDF in the background right after the
        Scholar view opens, caching the bytes so a later 'View PDF' is instant.
        Best-effort: any failure is swallowed and the on-demand path still runs.
        Skipped when the PDF libraries aren't installed."""
        if self._pdf_prefetch_started or self._pdf_locate_started:
            return
        self._pdf_prefetch_started = True
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            return
        client = self._app._get_client()
        if client is None:
            return
        item = self._pdf_item()

        def run() -> None:
            try:
                url = self._app._resolve_pdf_url(client, item)
                self._remember_case_law_pdf_choices(item)
                if not url:
                    return
                fetched = _fetch_pdf_bytes(url, client=client, timeout=30)
                if fetched is not None:
                    data, final_url = fetched
                    self._pdf_prefetch = (data, final_url)
                    self._pdf_url = final_url
                    self._post(self._on_pdf_located, True)
            except Exception as exc:
                print(f"[prefetch] PDF prefetch failed: {exc}")

        threading.Thread(target=run, daemon=True).start()

    def _view_pdf(self) -> None:
        """Show the official PDF of the opinion inside the window — using the
        background-prefetched copy when it's ready, else resolving on demand."""
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            if messagebox.askyesno(
                "PDF viewer not installed",
                "Viewing PDFs inside the app needs two Python packages:\n\n"
                "    pip install pypdfium2 Pillow\n\n"
                "Open the PDF in your web browser instead?",
                parent=self._win,
            ):
                self._open_pdf_in_browser()
            return
        # Remember which text view to return to when leaving the PDF.
        if self._mode in ("scholar", "courtlistener"):
            self._pre_pdf_mode = self._mode
        if self._pdf_prefetch is not None:  # warmed in the background already
            data, url = self._pdf_prefetch
            self._pdf_url = url
            self._show_pdf(data, url)
            return
        known_url = self._pdf_url if self._pdf_located is True else None
        client = None if known_url else self._app._get_client()
        self._pdf_url = known_url
        # Disable the control that was clicked while we look (the CL view uses
        # its own View PDF button; the Scholar view reuses the toggle).
        busy = (self._pdf_btn if self._pre_pdf_mode == "courtlistener"
                else self._toggle_btn)
        try:
            busy.configure(state="disabled")
        except tk.TclError:
            pass
        self._status_var.set("Locating a PDF of the opinion…")
        item = self._pdf_item()

        def run() -> None:
            try:
                url = known_url or (
                    self._app._resolve_pdf_url(client, item)
                    if client is not None else None
                )
                self._remember_case_law_pdf_choices(item)
                if not url:
                    self._post(self._on_pdf_error,
                               "No PDF is available for this opinion.")
                    return
                self._pdf_url = url  # so a fetch failure can offer the browser
                fetched = _fetch_pdf_bytes(url, client=client, timeout=30)
                if fetched is None:
                    self._post(self._on_pdf_error,
                               "The source returned something that isn't a PDF.")
                    return
                data, final_url = fetched
                self._pdf_url = final_url
                self._post(self._show_pdf, data, final_url)
            except Exception as exc:
                self._post(self._on_pdf_error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _show_pdf(self, data: bytes, url: str) -> None:
        _clamp_toplevel_to_work_area(
            self._win, min_width=430, min_height=300, bottom_gap=72
        )
        width = max(self._text.winfo_width() - 24, 520)
        # US Reports scans get a roughly 3× margin (see _is_us_reports_pdf).
        margin = _PdfPane._MARGIN * 3 if _is_us_reports_pdf(url) else None
        # The pane lives in a holder frame so a parts strip (built once the
        # text layer reveals the separate opinions) can sit to its right.
        if getattr(self, "_pdf_holder", None) is not None:
            self._pdf_holder.destroy()  # a previous PDF view left behind
            self._pdf_holder = None
            self._pdf_pane = None
        holder = ttk.Frame(self._win)
        try:
            pane = _PdfPane(
                holder, data, width=width, margin=margin,
                link_style="recolor", uniform_crop=True,
            )
        except Exception as exc:  # pragma: no cover - render/lib failure
            holder.destroy()
            self._on_pdf_error(str(exc))
            return
        # Swap the text view for the PDF pane (kept above the button row).
        self._text_frame.pack_forget()
        holder.pack(fill="both", expand=True, padx=8, pady=4,
                    before=self._btn_frame)
        pane.pack(side="left", fill="both", expand=True)
        self._pdf_holder = holder
        self._pdf_pane = pane
        self._pdf_url = url
        self._pdf_bytes = data
        self._mode = "pdf"

        # Extract the text layer in the background so text on the page can be
        # selected with the mouse and copied with Ctrl-C.  The find keys are
        # NOT rebound — Ctrl-F in this window belongs to the text view's
        # finder — so only the pane-local selection is enabled.
        def extract_text(d=data, p=pane) -> None:
            try:
                pages = _extract_pdf_text_pages(d)
            except Exception as exc:
                print(f"[pdf-text] extraction failed: {exc}")
                return
            links: dict = {}
            quiet: set = set()
            if not any(pages or []):
                return  # scan-only PDF: no text layer to select
            try:
                links, quiet = _citation_links_from_visible_pdf_text(d, pages)
            except Exception as exc:
                print(f"[pdf-links] citation scan failed: {exc}")
            # The separate opinions (Syllabus, Opinion of the Court, each
            # concurrence/dissent), read from the running heads — slip
            # opinions and US Reports pages share that layout.
            sections: list = []
            try:
                sections = slip_opinion.detect_sections(pages)
            except Exception as exc:
                print(f"[pdf-parts] section detection failed: {exc}")

            def apply() -> None:
                try:
                    if p.winfo_exists() and self._pdf_pane is p:
                        if links:
                            p.set_citation_links(
                                links,
                                self._open_pdf_cite,
                                self._open_pdf_cite_browser,
                                quiet_pages=quiet,
                            )
                        p.enable_find(pages, bind_keys=False)
                        if len(sections) > 1:
                            nav = _build_pdf_parts_nav(
                                self._pdf_holder, sections, p.scroll_to_page)
                            nav.pack(side="right", fill="y", padx=(4, 0))
                        bits = []
                        n = sum(len(v) for v in links.values())
                        nq = sum(len(v) for pg, v in links.items()
                                 if pg in quiet)
                        if n:
                            bits.append(
                                f"{n} citation link{'s' if n != 1 else ''} "
                                + ("clickable (not colored on scanned pages)"
                                   if nq else "shown in blue")
                            )
                        if len(sections) > 1:
                            bits.append(f"{len(sections)} opinion parts "
                                        "listed on the right")
                        bits.append("drag to select text; Ctrl+C copies")
                        self._status_var.set("; ".join(bits) + ".")
                except tk.TclError:
                    pass

            self._post(apply)

        threading.Thread(target=extract_text, daemon=True).start()
        self._hide_pdf_button()  # the toggle below is the way back from the PDF
        self._part_combo.config(state="disabled")
        self._view_label_var.set("PDF of opinion")
        self._set_view_color("black")
        self._source_var.set(url)
        # Returning from the PDF goes back to whichever text view we came from:
        # the Scholar text, or — for a CourtListener-primary window — the CL text.
        if self._pre_pdf_mode == "courtlistener":
            back_label, back_state = "Back to Text", "normal"
        else:
            # The "Google Scholar Text" toggle is disabled only for a Federal
            # Appendix case whose Scholar page has no opinion text (a scan-only
            # case); every other case keeps it active.
            back_label = "Google Scholar Text"
            back_state = ("disabled"
                          if self._fed_appx and not self._scholar_has_text
                          else "normal")
        _style_ui_button(self._toggle_btn, primary=True)
        self._toggle_btn.configure(
            text=back_label, command=self._back_from_pdf, state=back_state)
        self._hide_cl_button()
        # In PDF view, the RTF export becomes a "Download PDF" action, a Print
        # button appears, and the text-size buttons zoom the page.
        self._export_btn.configure(text="Download PDF", command=self._download_pdf)
        self._print_btn.pack(side="right", padx=4)
        self._zoom_out_btn.configure(text="−")
        self._zoom_in_btn.configure(text="+")
        self._apply_button_bar_compact()
        self._status_var.set("Showing the official PDF of the opinion.")

    def _download_pdf(self) -> None:
        """Save the original PDF currently being viewed."""
        data = getattr(self, "_pdf_bytes", None)
        if not data:
            return
        default = _build_default_filename(self._filename_item())
        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{default}.pdf",
            title="Download Opinion PDF",
            parent=self._win,
        )
        if not path:
            return
        try:
            with open(path, "wb") as fh:
                fh.write(data)
        except Exception as exc:
            messagebox.showerror("Download PDF", str(exc), parent=self._win)
            return
        self._status_var.set(f"Saved PDF to {path}")

    def _print_pdf(self) -> None:
        """Print the PDF currently being viewed — the re-spaced/centered
        rendering shown on screen, falling back to the original scan.  A
        redacted case.law scan gets its black redaction boxes whitened so
        they don't waste ink on paper, and its running-head line re-lettered
        with the Bluebook citation the redaction took away."""
        data = getattr(self, "_pdf_bytes", None)
        if not data:
            return
        whiten = _is_redacted_case_pdf(self._pdf_url)
        header = ""
        if whiten:
            cite_hint = next(
                (c.cite for c in getattr(self, "_case_law_pdf_choices", [])
                 or [] if getattr(c, "url", None) == self._pdf_url), "")
            try:
                header = _case_law_print_citation(
                    data, self._pdf_url, item=getattr(self, "_item", None),
                    cite_hint=cite_hint, client=_client_for_window(self._app))
            except Exception as exc:
                print(f"[pdf] header citation failed: {exc}")
        # Name the print file the same as "Download PDF" would, so a Save-As
        # from the viewer already carries the Bluebook citation.
        path = _named_temp_pdf_path(_build_default_filename(self._filename_item()))
        try:
            if self._pdf_pane is not None:
                self._pdf_pane.export_pdf(
                    path, whiten_redactions=whiten, header_cite=header)
            else:
                with open(path, "wb") as fh:
                    fh.write(data)
        except Exception:
            with open(path, "wb") as fh:
                fh.write(data)
        _print_pdf_file(self._win, path, self._status_var.set)

    def _open_pdf_cite(self, action: tuple, snippet: str) -> None:
        if self._app is not None:
            _follow_brief_action(self._app, self._win, action,
                                 self._status_var.set)
        else:
            _open_citation_in_browser(action, snippet)

    def _open_pdf_cite_browser(self, action: tuple, snippet: str) -> None:
        _open_citation_in_browser(action, snippet)

    def _back_from_pdf(self) -> None:
        """Return from the PDF to the text view it was opened from — the Google
        Scholar text, or the CourtListener text for a CL-primary window."""
        holder = getattr(self, "_pdf_holder", None)
        if holder is not None:
            holder.destroy()  # takes the pane and its parts strip with it
            self._pdf_holder = None
        elif self._pdf_pane is not None:
            self._pdf_pane.destroy()
        self._pdf_pane = None
        self._text_frame.pack(fill="both", expand=True, padx=8, pady=4,
                              before=self._btn_frame)
        if self._pre_pdf_mode == "courtlistener":
            self._render_courtlistener_view()
        else:
            self._render_scholar()  # restores label, combo and "View PDF"

    def _on_pdf_error(self, msg: str) -> None:
        if self._pre_pdf_mode == "courtlistener" and self._mode != "pdf":
            # Failure came from the CL view's View PDF button — restore it
            # rather than turning the Scholar toggle into a "View PDF".
            self._refresh_pdf_button()
        else:
            if not self._pdf_url:
                self._pdf_located = False
            self._refresh_pdf_button()
        self._status_var.set(f"PDF: {msg}")
        if self._pdf_url and messagebox.askyesno(
            "PDF", f"{msg}\n\nOpen the PDF in your web browser instead?",
            parent=self._win,
        ):
            webbrowser.open(self._pdf_url)
        elif not self._pdf_url:
            messagebox.showinfo("PDF", msg, parent=self._win)

    def _open_pdf_in_browser(self) -> None:
        """Resolve the PDF URL in the background and open it in the browser."""
        if self._pdf_url and self._pdf_located is True:
            webbrowser.open(self._pdf_url)
            self._status_var.set("Opened the PDF in your browser.")
            return
        client = self._app._get_client()
        if client is None:
            return
        self._status_var.set("Locating a PDF of the opinion…")
        item = self._pdf_item()

        def run() -> None:
            try:
                url = self._app._resolve_pdf_url(client, item)
                self._remember_case_law_pdf_choices(item)
            except Exception:
                url = None
            self._post(self._after_resolve_for_browser, url)

        threading.Thread(target=run, daemon=True).start()

    def _after_resolve_for_browser(self, url: Optional[str]) -> None:
        if url:
            webbrowser.open(url)
            self._status_var.set("Opened the PDF in your browser.")
        else:
            self._status_var.set("No PDF is available for this opinion.")
            messagebox.showinfo(
                "PDF", "No PDF is available for this opinion.", parent=self._win)


# Cross-references in the U.S. Code's own style: "section 3142(f) of
# title 18", "section 102 of this title" (resolved against the open doc).
_USC_XREF_RE = re.compile(
    r"\bsections?\s+(\d+[a-zA-Z0-9]*(?:[-–—]\d+[a-zA-Z0-9]*)?)"
    r"((?:\((?:\d{1,3}|[ivxIVX]{2,4}|[a-zA-Z]{1,3})\))*)"
    r"\s+of\s+(?:[Tt]itle\s+(\d{1,2})|this\s+title)",
    re.IGNORECASE,
)

# Bare section references inside a C.F.R. provision ("§ 1614.106(a)"),
# resolved against the open title.
_CFR_SECREF_RE = re.compile(
    r"§§?\s*(\d+[a-zA-Z]?\.\d+[a-zA-Z0-9]*)"
    r"((?:\((?:\d{1,3}|[ivxIVX]{2,4}|[a-zA-Z]{1,3})\))*)"
)

# A reporter citation on a hand-typed line: volume, reporter, page.
# Broader than _TEXT_CITE_RE (any capitalized reporter form, so official
# state reporters like "306 Md. 556" work) since the input is a citation
# list, not running prose.
_LINE_CITE_RE = re.compile(
    r"(\d{1,4})\s+([A-Z][A-Za-z0-9.'’ ]{0,24}?)\s+(\d{1,5})(?=[\s,;.)(]|$)"
)


_SPOTLIGHT_CASE_ACTIONS = frozenset(("cite", "engrep", "recap", "fedcas"))


def _spotlight_case_action(
    query: str,
) -> Optional[tuple[int, int, tuple[str, str]]]:
    """Return Spotlight's one unambiguous case action from the shared
    opinion/brief citation detector.

    Spotlight falls back to its ordinary results dropdown when the text has no
    case citation or has multiple case citations.  Filtering to case actions
    is important because statute and rule queries have their own earlier route.
    """
    try:
        cases = [
            (start, end, action)
            for start, end, action in detect_brief_links(query or "")
            if action[0] in _SPOTLIGHT_CASE_ACTIONS
        ]
    except Exception:
        # A convenience detector should never prevent the existing permissive
        # hand-typed parser and case-name search from getting their chance.
        return None
    return cases[0] if len(cases) == 1 else None


def _parse_citation_line(line: str) -> Optional[tuple[str, str, str]]:
    """Parse "Name v. Name, 365 U.S. 167, 171 (1961)" into
    (case name, citation, pin) — name and pin may be empty."""
    m = _LINE_CITE_RE.search(line)
    if not m:
        return None
    cite = re.sub(r"\s+", " ",
                  f"{m.group(1)} {m.group(2)} {m.group(3)}")
    cite = cite.replace("U. S.", "U.S.").replace("’", "'")
    cite = _respace_reporter_in_cite(cite)
    name = line[: m.start()].strip().rstrip(",;–—- ").strip()
    pin_m = _PINCITE_AFTER_RE.match(line, m.end())
    pin = pin_m.group(1) if pin_m else ""
    return name, cite, pin


# A hand-typed statute/regulation lookup: "42 USC 1983(b)", "29 cfr
# 1614.105(a)", with or without periods and the section symbol.
_STATUTE_QUERY_RE = re.compile(
    r"^\s*(\d{1,2})\s*"
    r"(u\.?\s*s\.?\s*c\.?\s*a?\.?|c\.?\s*f\.?\s*r\.?)\s*"
    r"(?:§§?|sec(?:tions?)?\.?)?\s*"
    r"(\d[\w.–—-]*)"
    r"((?:\s*\(\w{1,4}\))*)\s*$",
    re.IGNORECASE,
)


def _parse_statute_query(query: str) -> Optional[tuple[str, str]]:
    """Parse a typed citation into ("usc"|"cfr"|"rule"|"statestat", spec), or
    None.  Federal-rule queries ("fre 404(b)", "Fed. R. Civ. P. 56") and state
    statute queries ("Cal. Penal Code § 187") never start with a volume number,
    so they can't collide with the U.S.C./C.F.R. form and are tried first."""
    rule = fed_rules.parse_query(query)
    if rule:
        return rule
    const = constitution.parse_query(query)
    if const:
        return const
    statestat = state_statutes.parse_query(query)
    if statestat:
        return statestat
    m = _STATUTE_QUERY_RE.match(query or "")
    if not m:
        return None
    kind = "cfr" if "f" in m.group(2).lower() else "usc"
    section = m.group(3).rstrip(".").replace("–", "-").replace("—", "-")
    if not section or (kind == "cfr" and "." not in section):
        return None  # CFR sections are part.section ("1614.105")
    subs = re.findall(r"\(([^)]+)\)", m.group(4) or "")
    return kind, f"{m.group(1)}:{section}:{','.join(subs)}"


# Registry of statute/rule sources, keyed by the action `kind` carried on a
# citation link.  Each module exposes the same contract (a CITE_RE,
# cite_spec/spec_label, load_section(title, section), and a Doc with
# paras/label/source_name/source_note/url/kind/bluebook_cite/neighbors), so
# one viewer serves them all.  ``_SOURCE_HOST`` is only the name shown in the
# "Fetching … from <host>" status line.
_STATUTE_SOURCES: dict[str, object] = {
    "usc": us_code,
    "cfr": ecfr,
    "rule": fed_rules,
    "statestat": state_statutes,  # in-app state statutes (CA; more to follow)
    "const": constitution,        # U.S. Constitution (bundled text)
}
_SOURCE_HOST: dict[str, str] = {
    "usc": "uscode.house.gov",
    "cfr": "ecfr.gov",
    "rule": "law.cornell.edu",
    "statestat": "the official source",
    "const": "the U.S. Constitution",
}


def _fetch_statute_window(parent: tk.Misc, kind: str, spec: str,
                          status=lambda _s: None, *, app=None) -> None:
    """Fetch a statute, regulation or federal rule section in a background
    thread and open a _StatuteWindow over `parent` when it arrives."""
    mod = _STATUTE_SOURCES[kind]
    host = _SOURCE_HOST.get(kind, "the source")
    title, section, subs = spec.split(":", 2)
    label = mod.spec_label(spec)

    def safe_status(s: str) -> None:
        try:
            status(s)
        except tk.TclError:
            pass  # the window owning the status display was closed

    def post(fn, *args) -> None:
        try:
            parent.after(0, fn, *args)
        except tk.TclError:
            pass

    safe_status(f"Fetching {label} from {host}…")

    def run() -> None:
        try:
            doc = mod.load_section(title, section)
        except Exception as exc:
            post(safe_status, str(exc))
            return

        def show() -> None:
            safe_status(f"{label} loaded.")
            _StatuteWindow(
                parent, doc, tuple(s for s in subs.split(",") if s),
                app=app,
            )

        post(show)

    threading.Thread(target=run, daemon=True).start()


def _open_statute_action(parent: tk.Misc, action: tuple[str, str],
                         status=lambda _s: None, *, app=None) -> None:
    """Carry out a parsed statute-lookup action: open the in-app viewer, or —
    for a state we only link out to (N.Y., Tex., other states) — open the
    official source in the browser."""
    kind, value = action
    if kind == "browse":
        webbrowser.open(value)
        status("Opened in your browser.")
        return
    if kind == "statpdf":
        _open_statute_pdf(parent, value, status, app=app)
        return
    _fetch_statute_window(parent, kind, value, status, app=app)


def _stat_cite_from_url(url: str) -> str:
    """'… /link/statute/88/1932' → '88 Stat. 1932' (the Statutes at Large
    citation), falling back to a generic label."""
    m = re.search(r"/statute/(\d+)/(\d+)", url or "")
    return f"{m.group(1)} Stat. {m.group(2)}" if m else "Statutes at Large"


def _choose_case_law_page_opinion(
    parent: tk.Misc, opinions: list[_CaseLawPageOpinion], title: str,
) -> Optional[_CaseLawPageOpinion]:
    """Ask which CAP case to open when several begin on one reporter page."""
    dlg = _ui_toplevel(parent)
    _ensure_modern_ttk_styles(dlg)
    dlg.title("Choose Case")
    dlg.geometry("640x330")
    if parent.winfo_viewable():
        dlg.transient(parent)
    _ui_label(
        dlg,
        f"{len(opinions)} cases begin at {title}. Pick the case to open:",
        size=13, weight="bold", anchor="w",
    ).pack(anchor="w", fill="x", padx=14, pady=(12, 0))
    box = _ui_frame(dlg, card=True)
    box.pack(fill="both", expand=True, padx=12, pady=(8, 8))
    lb_kw = dict(activestyle="dotbox", borderwidth=0, highlightthickness=0)
    if _CTK_AVAILABLE:
        lb_kw.update(
            bg=_UI["window"], fg=_UI["text"],
            selectbackground=_UI["selection"],
            selectforeground=_UI["text"], font=("TkDefaultFont", 11),
        )
    lb = tk.Listbox(box, **lb_kw)
    sb = ttk.Scrollbar(box, orient="vertical", command=lb.yview)
    lb.configure(yscrollcommand=sb.set)
    pad = 8 if _CTK_AVAILABLE else 0
    sb.pack(side="right", fill="y", pady=pad, padx=(0, pad))
    lb.pack(side="left", fill="both", expand=True, padx=(pad, 0), pady=pad)
    for i, opinion in enumerate(opinions, 1):
        lb.insert("end", _case_law_opinion_name(opinion) or f"Opinion {i}")
    lb.selection_set(0)
    chosen: dict[str, Optional[_CaseLawPageOpinion]] = {"opinion": None}

    def finish(accept: bool) -> None:
        selected = lb.curselection()
        chosen["opinion"] = (
            opinions[selected[0]] if accept and selected else None
        )
        dlg.destroy()

    lb.bind("<Double-Button-1>", lambda _e: finish(True))
    buttons = _ui_frame(dlg)
    buttons.pack(fill="x", padx=14, pady=(0, 12))
    _ui_button(
        buttons, "Open", command=lambda: finish(True), primary=True, width=92,
    ).pack(side="right")
    _ui_button(
        buttons, "Cancel", command=lambda: finish(False), width=88,
    ).pack(side="right", padx=8)
    dlg.bind("<Return>", lambda _e: finish(True))
    dlg.bind("<Escape>", lambda _e: finish(False))
    dlg.update_idletasks()
    dlg.deiconify()
    dlg.lift()
    lb.focus_set()
    try:
        dlg.grab_set()
    except tk.TclError:
        pass
    parent.wait_window(dlg)
    return chosen["opinion"]


def _open_case_law_pdf(
    parent: tk.Misc, url: str, title: str, status=lambda _s: None, *,
    app=None, expected_name: str = "",
) -> None:
    """Open a CAP PDF after resolving rare same-page multiple opinions.

    ``expected_name`` is supplied only by a CourtListener/Scholar-backed view.
    It is matched against CAP's JSON names automatically.  Direct citation/PDF
    paths deliberately leave it empty, so a multi-opinion page asks the user.
    """
    m = _CASE_LAW_NUMBERED_PDF_RE.match((url or "").strip())
    if not m or int(m.group(2)) != 1:
        _PdfWindow(parent, url, title, status, app=app, is_case=True)
        return

    try:
        status("Checking for other cases on this reporter pageâ€¦")
    except tk.TclError:
        pass

    def post(fn, *args) -> None:
        try:
            parent.after(0, fn, *args)
        except tk.TclError:
            pass

    def ready(opinions: list[_CaseLawPageOpinion]) -> None:
        chosen = (
            _match_case_law_page_opinion(opinions, expected_name)
            if opinions and expected_name else None
        )
        if opinions and chosen is None:
            chosen = _choose_case_law_page_opinion(parent, opinions, title)
            if chosen is None:
                try:
                    status("Case selection cancelled.")
                except tk.TclError:
                    pass
                return
        final_url = chosen.url if chosen is not None else url
        final_title = title
        picked_name = _case_law_opinion_name(chosen) if chosen else ""
        if picked_name and picked_name.lower() not in title.lower():
            final_title = f"{picked_name} â€” {title}"
        _PdfWindow(
            parent, final_url, final_title, status, app=app, is_case=True,
        )

    def run() -> None:
        opinions = _case_law_page_opinions(url)
        post(ready, opinions)

    threading.Thread(target=run, daemon=True).start()


class _PdfWindow:
    """A standalone in-app PDF viewer (e.g. a Statutes at Large scan).  Reuses
    the centered, zoomable pane from the opinion window and adds a Download
    button; the citation is shown at the top.  Falls back to opening the PDF in
    the browser when the PDF libraries are missing or the fetch fails."""

    def __init__(self, parent: tk.Misc, url: str, title: str,
                 status=lambda _s: None, *, app=None,
                 is_case: bool = False) -> None:
        self._url = url
        self._title = title
        self._ext_status = status
        self._app = app          # for the History dropdown (may be None)
        self._is_case = is_case  # case scan (case.law) vs statute scan
        self._bytes: Optional[bytes] = None
        self._pane: Optional[_PdfPane] = None
        self._text_source: Optional[_CasePdfTextSource] = None
        self._can_discover_text = bool(
            self._is_case and _case_law_json_for_pdf_url(self._url)
        )

        self._win = (
            _case_view_host(parent, app) if self._is_case
            else _secondary_view_host(parent, app)
        )
        self._win.title(title)
        self._history_menubar = (
            _install_history_menubar(self._app, self._win)
            if self._is_case else
            _install_window_menubar(self._app, self._win)
        )
        self._win.geometry(
            _fit_toplevel_geometry(
                self._win, 820, 900, min_width=500, min_height=320,
                bottom_gap=72,
            )
        )
        self._win.minsize(500, 320)

        top = _ui_frame(self._win)
        top.pack(fill="x", padx=12, pady=(12, 4))
        _ui_label(top, title, size=14, weight="bold", anchor="w").pack(side="left")

        btns = _ui_frame(self._win)
        btns.pack(fill="x", side="bottom", padx=12, pady=(0, 10))
        _ui_button(btns, "Download PDF", primary=True, width=124,
                   command=self._download).pack(side="right")
        _ui_button(btns, "Print…", width=96,
                   command=self._print).pack(side="right", padx=(0, 6))
        self._text_btn = None
        if self._can_discover_text:
            self._text_btn = _ui_button(
                btns, "Finding Text...", width=142,
                command=self._open_discovered_text,
            )
            self._text_btn.configure(state="disabled")
            self._text_btn.pack(side="right", padx=(0, 6))
        _ui_button(btns, "−", width=42,
                   command=lambda: self._zoom(-1)).pack(side="left")
        _ui_button(btns, "+", width=42,
                   command=lambda: self._zoom(+1)).pack(side="left", padx=(6, 10))
        self._status_var = tk.StringVar(value="Loading PDF…")
        _install_status_label(btns, self._status_var)

        self._body = _ui_frame(self._win)
        self._body.pack(fill="both", expand=True)

        for seq in ("<Control-plus>", "<Control-equal>", "<Control-KP_Add>"):
            self._win.bind(seq, lambda _e: self._zoom(+1))
        for seq in ("<Control-minus>", "<Control-KP_Subtract>"):
            self._win.bind(seq, lambda _e: self._zoom(-1))
        self._win.bind("<Control-0>", lambda _e: self._zoom(0))

        entry = self._history_entry()
        if entry is not None and self._app is not None and hasattr(
                self._app, "record_case_view"):
            self._app.record_case_view(*entry)
            if hasattr(self._app, "register_case_window"):
                self._app.register_case_window(
                    self, self._win, entry[0], entry[1], entry[2]
                )
        elif self._app is not None and hasattr(
            self._app, "register_secondary_window"
        ):
            def reopen(app=self._app, source=self) -> None:
                _PdfWindow(
                    app.root, source._url, source._title,
                    app=app, is_case=False,
                )
            self._app.register_secondary_window(
                self, self._win, f"pdf-view:{id(self)}", title, reopen,
            )

        self._fetch()
        if self._can_discover_text:
            self._discover_text()

    def _history_entry(self) -> "Optional[tuple[str, str, object]]":
        """(key, label, reopen) for the History dropdown, or None when this
        PDF isn't a case (a statute scan).  Subclasses override."""
        if not (self._is_case and self._app is not None):
            return None
        app, url, title = self._app, self._url, self._title
        payload = {"type": "pdf", "url": url, "title": title}
        return (f"pdf:{url}", title,
                lambda: _open_case_law_pdf(
                    app.root, url, title, app=app,
                ),
                payload)

    def _post(self, fn, *args) -> None:
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass

    def _zoom(self, delta: int) -> None:
        if self._pane is not None:
            self._pane.zoom(delta)
            self._status_var.set(f"Zoom: {self._pane.zoom_percent()}%")

    def _discover_text(self) -> None:
        """Find the best text version without delaying the PDF viewer."""
        client = _client_for_window(self._app)
        url, title = self._url, self._title

        def run() -> None:
            source = _case_pdf_text_source(url, title, client=client)
            self._post(self._on_text_discovered, source)

        threading.Thread(target=run, daemon=True).start()

    def _on_text_discovered(
        self, source: "Optional[_CasePdfTextSource]",
    ) -> None:
        self._text_source = source
        btn = self._text_btn
        if btn is None:
            return
        try:
            if source is None:
                btn.configure(text="No Text", state="disabled")
            else:
                btn.configure(
                    text=source.button_label, state="normal",
                    command=self._open_discovered_text,
                )
        except tk.TclError:
            pass

    def _open_discovered_text(self) -> None:
        source = self._text_source
        if source is None or self._app is None:
            return
        initial_pdf = (
            (self._bytes, self._url) if self._bytes is not None else None
        )
        try:
            _ScholarTextWindow(
                self._app.root, self._app, "", "",
                item=dict(source.item), cl_text=source.text,
                cl_parts=list(source.parts), cl_blocks=list(source.blocks),
                prefetch_pdf=False,
                primary_source_label=source.source_label,
                primary_source_url=source.source_url,
                primary_source_kind=source.kind,
                history_pdf_url=self._url,
                initial_pdf=initial_pdf,
            )
        except Exception as exc:
            self._status_var.set(f"Could not open the text view: {exc}")
            return
        self._win.destroy()

    def _fetch(self) -> None:
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            if messagebox.askyesno(
                "PDF viewer not installed",
                "Viewing PDFs inside the app needs two Python packages:\n\n"
                "    pip install pypdfium2 Pillow\n\n"
                "Open the PDF in your web browser instead?",
                parent=self._win,
            ):
                webbrowser.open(self._url)
            self._win.destroy()
            return

        def run() -> None:
            try:
                client = (
                    getattr(self._app, "_client", None)
                    if self._app is not None and _is_courtlistener_url(self._url)
                    else None
                )
                fetched = _fetch_pdf_bytes(self._url, client=client, timeout=30)
                if fetched is None:
                    raise ValueError("the source returned something that "
                                     "isn't a PDF")
                data, final_url = fetched
                self._url = final_url
                self._post(self._show, data)
            except Exception as exc:
                self._post(self._error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _show(self, data: bytes) -> None:
        try:
            pane = _PdfPane(
                self._body, data, width=760,
                link_style="recolor" if self._is_case else "tint",
                uniform_crop=self._is_case,
            )
        except Exception as exc:  # pragma: no cover - render/lib failure
            self._error(str(exc))
            return
        pane.pack(fill="both", expand=True, padx=8, pady=4)
        self._pane = pane
        self._bytes = data
        self._status_var.set(self._title)

        # Background text-layer extraction: enables Ctrl-F search and mouse
        # text selection (Ctrl-C copies) when the PDF carries text.
        def extract_text(d=data, p=pane) -> None:
            try:
                pages = _extract_pdf_text_pages(d)
            except Exception as exc:
                print(f"[pdf-text] extraction failed: {exc}")
                return
            links: dict = {}
            quiet: set = set()
            if not any(pages or []):
                return
            if self._is_case:
                try:
                    links, quiet = _citation_links_from_visible_pdf_text(
                        d, pages)
                except Exception as exc:
                    print(f"[pdf-links] citation scan failed: {exc}")

            def apply() -> None:
                try:
                    if p.winfo_exists() and self._pane is p:
                        if links:
                            p.set_citation_links(
                                links,
                                self._open_cite,
                                self._open_cite_browser,
                                quiet_pages=quiet,
                            )
                        p.enable_find(pages)
                        n = sum(len(v) for v in links.values())
                        nq = sum(len(v) for pg, v in links.items()
                                 if pg in quiet)
                        if n:
                            self._status_var.set(
                                f"{n} citation link{'' if n == 1 else 's'} "
                                + ("clickable (not colored on scanned "
                                   "pages); " if nq else "shown in blue; ")
                                + "drag to select text."
                            )
                except tk.TclError:
                    pass

            self._post(apply)

        threading.Thread(target=extract_text, daemon=True).start()

    def _error(self, msg: str) -> None:
        self._status_var.set(f"PDF: {msg}")
        if messagebox.askyesno(
            "PDF", f"{msg}\n\nOpen the PDF in your web browser instead?",
            parent=self._win,
        ):
            webbrowser.open(self._url)
        self._win.destroy()

    def _print(self) -> None:
        # A redacted case.law scan gets its black redaction boxes whitened
        # for paper — and its running-head line re-lettered with the Bluebook
        # citation the redaction took away; other sources print exactly
        # what's shown.
        data = self._bytes
        if not data:
            return
        whiten = _is_redacted_case_pdf(self._url)
        header = ""
        if whiten:
            try:
                header = _case_law_print_citation(
                    data, self._url, title=self._title,
                    client=_client_for_window(self._app))
            except Exception as exc:
                print(f"[pdf] header citation failed: {exc}")
        # Same file name "Download PDF" would offer, so a Save-As from the
        # viewer already carries the proper name.
        stem = re.sub(r"[^\w.-]+", "_", self._title).strip("_") or "statute"
        path = _named_temp_pdf_path(stem)
        try:
            if self._pane is not None:
                self._pane.export_pdf(
                    path, whiten_redactions=whiten, header_cite=header)
            else:
                with open(path, "wb") as fh:
                    fh.write(data)
        except Exception:
            with open(path, "wb") as fh:
                fh.write(data)
        _print_pdf_file(self._win, path, self._status_var.set)

    def _download(self) -> None:
        data = self._bytes
        if not data:
            return
        safe = re.sub(r"[^\w.-]+", "_", self._title).strip("_") or "statute"
        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{safe}.pdf",
            title="Download PDF",
            parent=self._win,
        )
        if not path:
            return
        try:
            with open(path, "wb") as fh:
                fh.write(data)
        except Exception as exc:
            messagebox.showerror("Download PDF", str(exc), parent=self._win)
            return
        self._status_var.set(f"Saved PDF to {path}")

    def _open_cite(self, action: tuple, snippet: str) -> None:
        if self._app is not None:
            _follow_brief_action(self._app, self._win, action,
                                 self._status_var.set)
        else:
            _open_citation_in_browser(action, snippet)

    def _open_cite_browser(self, action: tuple, snippet: str) -> None:
        _open_citation_in_browser(action, snippet)


def _print_pdf_file(parent: tk.Misc, path: str,
                    status=lambda _s: None) -> None:
    """Open the PDF in the system's default viewer so the user can print it —
    choosing a printer in the viewer's own Print dialog — rather than sending it
    straight to the default printer."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
        status("Opened the PDF — print it (Ctrl/Cmd-P) and choose your printer.")
    except Exception:
        try:
            webbrowser.open("file://" + path)
            status("Opened the PDF — print it and choose your printer.")
        except Exception as exc:
            messagebox.showerror("Print", str(exc), parent=parent)


def _open_statute_pdf(parent: tk.Misc, url: str,
                      status=lambda _s: None, *, app=None) -> None:
    """Open a Statutes at Large GovInfo scan in the in-app PDF viewer."""
    cite = _stat_cite_from_url(url)
    status(f"Opening {cite}…")
    _PdfWindow(parent, url, cite, status, app=app)


# ---------------------------------------------------------------------------
# English Reports — open the CommonLII scan (cached; CloudFlare hand-off)
# ---------------------------------------------------------------------------

class _EngRepPdfWindow(_PdfWindow):
    """The in-app viewer for an English Reports scan from CommonLII.  Reuses the
    Statutes-at-Large PDF pane (centered, zoomable, Download/Print) but fetches
    through :mod:`eng_rep_pdf` — disk cache first, then a ``curl_cffi`` fetch
    using Firefox's CloudFlare clearance.  When there is no clearance yet it
    shows a hand-off panel that opens the case in Firefox and offers Retry."""

    def __init__(self, parent: tk.Misc, case: "eng_rep.ERCase",
                 status=lambda _s: None, *, app=None) -> None:
        self._case = case
        name = case.name if len(case.name) <= 60 else case.name[:57] + "…"
        super().__init__(parent, case.pdf_url, f"{name} — {case.er_cite}",
                         status, app=app, is_case=True)

    def _history_entry(self):  # overrides _PdfWindow
        app, case = self._app, self._case
        if app is None:
            return None
        return (f"engrep:{case.year}:{case.num}",
                f"{case.name} — {case.er_cite}",
                lambda: _EngRepPdfWindow(app.root, case, app=app))

    def _fetch(self) -> None:  # overrides _PdfWindow._fetch
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            if messagebox.askyesno(
                "PDF viewer not installed",
                "Viewing PDFs inside the app needs two Python packages:\n\n"
                "    pip install pypdfium2 Pillow\n\n"
                "Open this English Reports case on CommonLII instead?",
                parent=self._win,
            ):
                eng_rep_pdf.open_in_browser(self._case.web_url)
            self._win.destroy()
            return
        self._status_var.set("Loading the English Reports scan…")
        case = self._case

        def run() -> None:
            try:
                data = eng_rep_pdf.fetch_pdf(case.year, case.num, case.web_url)
                self._post(self._show, data)
            except eng_rep_pdf.CloudflareChallenge as exc:
                self._post(self._need_clearance, exc.web_url)
            except eng_rep_pdf.FetchUnavailable:
                self._post(self._link_out)
            except eng_rep_pdf.OriginError as exc:
                self._post(self._error,
                           f"CommonLII returned an error (HTTP {exc.status}).")
            except Exception as exc:  # pragma: no cover - defensive
                self._post(self._error, str(exc))

        threading.Thread(target=run, daemon=True).start()

    def _clear_body(self) -> None:
        for child in self._body.winfo_children():
            child.destroy()

    def _need_clearance(self, web_url: str) -> None:
        """Show the CloudFlare hand-off panel (open in Firefox, then Retry)."""
        self._clear_body()
        self._status_var.set("CommonLII needs a CloudFlare check.")
        frame = ttk.Frame(self._body)
        frame.pack(fill="both", expand=True, padx=24, pady=24)
        ttk.Label(
            frame, wraplength=560, justify="left",
            text=("CommonLII is behind a CloudFlare check.\n\n"
                  "To view this scan in the app, click “Open in Firefox”, pass "
                  "the “Just a moment…” check there, then click “Retry”.  Once "
                  "cleared, this and other English Reports cases load straight "
                  "in the app (and are cached so you won't be asked again)."),
        ).pack(anchor="w", pady=(0, 16))
        row = ttk.Frame(frame)
        row.pack(anchor="w")

        def open_ff() -> None:
            if eng_rep_pdf.open_in_firefox(web_url):
                self._status_var.set("Pass the check in Firefox, then Retry.")
            else:
                eng_rep_pdf.open_in_browser(web_url)

        def retry() -> None:
            self._clear_body()
            self._fetch()

        ttk.Button(row, text="Open in Firefox", command=open_ff).pack(side="left")
        ttk.Button(row, text="Retry", command=retry).pack(side="left", padx=8)
        ttk.Button(row, text="Open in browser instead",
                   command=lambda: (eng_rep_pdf.open_in_browser(self._case.web_url),
                                    self._win.destroy())).pack(side="left")

    def _link_out(self) -> None:
        """In-app fetch isn't possible here (Firefox or a dependency missing).
        Offer the same hand-off as the CloudFlare panel: open the case page on
        CommonLII — the *main site*, so the scan's hotlink check passes when you
        click through to it — preferring Firefox when it's installed."""
        self._clear_body()
        web_url = self._case.web_url
        has_ff = eng_rep_pdf.firefox_available()
        frame = ttk.Frame(self._body)
        frame.pack(fill="both", expand=True, padx=24, pady=24)
        msg = ("This English Reports scan can't be fetched inside the app "
               "here.\n\nOpen the case on CommonLII and click through to the "
               "scan there: the site only serves the PDF when you arrive from "
               "a link on its own pages, so opening the case page first gets "
               "you past that block.")
        if not has_ff:
            msg += ("\n\nFor in-app viewing (cached, no repeat checks), install "
                    "Firefox and run:\n\n    pip install curl_cffi browser_cookie3")
        ttk.Label(frame, wraplength=560, justify="left", text=msg).pack(
            anchor="w", pady=(0, 16))
        row = ttk.Frame(frame)
        row.pack(anchor="w")
        if has_ff:
            ttk.Button(
                row, text="Open in Firefox",
                command=lambda: (eng_rep_pdf.open_in_firefox(web_url),
                                 self._status_var.set("Opened in Firefox.")),
            ).pack(side="left")
        ttk.Button(
            row, text="Open in browser",
            command=lambda: (eng_rep_pdf.open_in_browser(web_url),
                             self._status_var.set("Opened in your browser.")),
        ).pack(side="left", padx=8)
        self._status_var.set("In-app viewing unavailable — open on CommonLII.")

    def _error(self, msg: str) -> None:  # overrides _PdfWindow._error
        """Origin error fallback — open the CommonLII *case page* (not the
        hotlink-blocked .pdf), so the scan loads when clicked from there."""
        self._status_var.set(f"English Reports: {msg}")
        if messagebox.askyesno(
            "English Reports",
            f"{msg}\n\nOpen this case on CommonLII instead?",
            parent=self._win,
        ):
            eng_rep_pdf.open_in_browser(self._case.web_url)
        self._win.destroy()


def _choose_eng_rep_case(parent: tk.Misc,
                         cases: "list[eng_rep.ERCase]") -> "eng_rep.ERCase | None":
    """Several cases share one E.R. page — let the user pick.  Returns the chosen
    case, or None if cancelled."""
    dlg = _ui_toplevel(parent)
    _ensure_modern_ttk_styles(dlg)
    dlg.title(f"{cases[0].er_cite} — {len(cases)} cases")
    dlg.geometry("640x380")
    # Only tie the dialog to the parent when the parent is actually on screen.
    # Invoked from the spotlight the main window is withdrawn, and making a modal
    # dialog transient to (or grabbing against) a hidden window leaves it
    # invisible / raises "grab failed: window not viewable".
    if parent.winfo_viewable():
        dlg.transient(parent)
    _ui_label(dlg, f"{len(cases)} cases are reported at {cases[0].er_cite}. "
                   "Pick one:", size=13, weight="bold", anchor="w").pack(
        anchor="w", fill="x", padx=14, pady=(12, 0))
    box = _ui_frame(dlg, card=True)
    box.pack(fill="both", expand=True, padx=12, pady=(8, 8))
    sb_style = "Modern.Vertical.TScrollbar" if _CTK_AVAILABLE else "Vertical.TScrollbar"
    lb_kw = dict(activestyle="dotbox", borderwidth=0, highlightthickness=0)
    if _CTK_AVAILABLE:
        lb_kw.update(bg=_UI["window"], fg=_UI["text"],
                     selectbackground=_UI["selection"],
                     selectforeground=_UI["text"], font=("TkDefaultFont", 11))
    lb = tk.Listbox(box, **lb_kw)
    sb = ttk.Scrollbar(box, orient="vertical", command=lb.yview, style=sb_style)
    lb.configure(yscrollcommand=sb.set)
    pad = 8 if _CTK_AVAILABLE else 0
    sb.pack(side="right", fill="y", pady=pad, padx=(0, pad))
    lb.pack(side="left", fill="both", expand=True, padx=(pad, 0), pady=pad)
    for c in cases:
        tag = f"({c.letter}) " if c.letter else ""
        lb.insert("end", f"{tag}{c.name}  ·  {c.neutral}")
    lb.selection_set(0)
    chosen: dict[str, "eng_rep.ERCase | None"] = {"case": None}

    def ok() -> None:
        sel = lb.curselection()
        chosen["case"] = cases[sel[0]] if sel else None
        dlg.destroy()

    def cancel() -> None:
        chosen["case"] = None
        dlg.destroy()

    lb.bind("<Double-Button-1>", lambda _e: ok())
    btns = _ui_frame(dlg)
    btns.pack(fill="x", padx=14, pady=(0, 12))
    _ui_button(btns, "Open", command=ok, primary=True, width=92).pack(side="right")
    _ui_button(btns, "Cancel", command=cancel, width=88).pack(side="right", padx=8)
    dlg.bind("<Return>", lambda _e: ok())
    dlg.bind("<Escape>", lambda _e: cancel())
    # Make sure the dialog is mapped and focused before grabbing — with the main
    # window hidden it must stand on its own — and never let a failed grab abort
    # the caller (it just means the dialog isn't modal).
    dlg.update_idletasks()
    dlg.deiconify()
    dlg.lift()
    dlg.focus_force()
    lb.focus_set()
    try:
        dlg.grab_set()
    except tk.TclError:
        pass
    parent.wait_window(dlg)
    return chosen["case"]


class _SlipTextWindow:
    """The copyable-text view of a slip opinion: the PDF converted to clean
    text (running heads and page numbers stripped, paragraphs rebuilt — see
    :func:`slip_opinion.to_clean_text`) in a selectable Text widget, for the
    copy-paste convenience a rendered PDF page can't give."""

    def __init__(
        self, parent: tk.Misc, title: str, text: str, *, app=None,
    ) -> None:
        self._app = app
        self._title = title
        self._source_text = text
        win = _secondary_view_host(parent, app)
        _ensure_modern_ttk_styles(win)
        win.title(f"{title} — Text")
        win.geometry(_fit_toplevel_geometry(win, 760, 680,
                                            min_width=420, min_height=300))
        win.minsize(420, 300)
        self._win = win
        self._window_menubar = _install_window_menubar(app, win)

        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True, padx=8, pady=(8, 4))
        body = tk.Text(frame, wrap="word", font=("Georgia", _OPINION_FONT_PT),
                       padx=14, pady=10, undo=False)
        vsb = ttk.Scrollbar(frame, orient="vertical", command=body.yview)
        body.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        body.pack(side="left", fill="both", expand=True)
        body.insert("1.0", text)
        body.config(state="disabled")  # selectable & copyable, not editable
        self._text = body

        btns = _ui_frame(win)
        btns.pack(fill="x", padx=12, pady=(0, 10))
        _ui_button(btns, "Copy All", primary=True, width=100,
                   command=self._copy_all).pack(side="right")
        self._status_var = tk.StringVar(value=f"{len(text):,} characters")
        _install_status_label(btns, self._status_var, padx=(0, 0))
        win.bind("<Control-a>", self._select_all)
        if app is not None and hasattr(app, "register_secondary_window"):
            def reopen(app=app, source=self) -> None:
                _SlipTextWindow(
                    app.root, source._title, source._source_text, app=app
                )
            app.register_secondary_window(
                self, win, f"slip-text:{id(self)}", f"{title} — Text",
                reopen,
            )

    def _select_all(self, _e=None) -> str:
        self._text.tag_add("sel", "1.0", "end-1c")
        return "break"

    def _copy_all(self) -> None:
        try:
            self._win.clipboard_clear()
            self._win.clipboard_append(self._text.get("1.0", "end-1c"))
            self._status_var.set("Copied the full text to the clipboard.")
        except tk.TclError:
            pass


# Colors for a PDF's detected opinion parts — the same palette the text
# view's part map uses (_ScholarTextWindow._PARTMAP_COLORS), so the strip
# reads identically beside a PDF and beside the CourtListener/Scholar text.
_PDF_PART_COLORS = {
    "syllabus": "#555555",
    "majority": _ScholarTextWindow._MAJORITY_COLOR,
    "concurrence": _ScholarTextWindow._CONCUR_COLOR,
    "dissent": _ScholarTextWindow._DISSENT_COLOR,
    "separate": _ScholarTextWindow._SEPARATE_COLOR,
}


def _build_pdf_parts_nav(parent, sections: list, goto) -> ttk.Frame:
    """A slim strip listing a PDF's detected opinion parts (from
    slip_opinion.detect_sections), each colored by kind; clicking a label
    calls ``goto(start_page)``.  The caller packs the returned frame."""
    nav = ttk.Frame(parent, padding=(6, 4))
    ttk.Label(nav, text="Parts", font=("TkDefaultFont", 9, "bold"),
              anchor="w").pack(fill="x", pady=(2, 4))
    for sec in sections:
        lbl = tk.Label(
            nav, text=sec.label, anchor="w", justify="left",
            wraplength=150, cursor="hand2",
            foreground=_PDF_PART_COLORS.get(sec.kind, "#333333"),
            font=("TkDefaultFont", 9, "underline"),
        )
        lbl.pack(fill="x", pady=2)
        lbl.bind("<Button-1>", lambda _e, p=sec.start_page: goto(p))
        page_no = tk.Label(nav, text=f"p. {sec.start_page + 1}",
                           anchor="w", foreground="#999999",
                           font=("TkDefaultFont", 8))
        page_no.pack(fill="x", padx=(8, 0))
    return nav


class _SlipOpinionWindow:
    """Viewer for a Supreme Court slip opinion straight from supremecourt.gov.

    Shows the official PDF in the cropped, zoomable pane.  Citations in the
    text are made clickable, styled as light-blue *text* rather than highlight
    boxes (``link_style="recolor"``) — the same actions as everywhere else
    (cases via Scholar/CourtListener, statutes in the statute viewer).  The
    opinion's parts (Syllabus, Opinion of the Court, each concurrence and
    dissent) are detected from the slip's running heads and listed in a strip
    on the right for one-click jumps; "Show as Text" opens the copyable-text
    conversion."""

    def __init__(self, parent: tk.Misc, url: str, title: str,
                 status=lambda _s: None, *, app=None,
                 description: str = "") -> None:
        self._url = url
        self._title = title
        self._description = description
        self._app = app
        self._ext_status = status
        self._bytes: Optional[bytes] = None
        self._pages: Optional[list] = None   # extracted glyph data
        self._clean_text: Optional[str] = None
        self._pane: Optional[_PdfPane] = None

        win = _case_view_host(parent, app)
        _ensure_modern_ttk_styles(win)
        win.title(f"{title} — Slip Opinion")
        win.geometry(_fit_toplevel_geometry(win, 980, 900,
                                            min_width=560, min_height=360))
        win.minsize(560, 360)
        self._win = win
        self._history_menubar = _install_history_menubar(app, win)

        top = _ui_frame(win)
        top.pack(fill="x", padx=12, pady=(12, 4))
        _ui_label(top, title, size=14, weight="bold", anchor="w").pack(
            side="left", fill="x", expand=True)

        btns = _ui_frame(win)
        btns.pack(fill="x", side="bottom", padx=12, pady=(0, 10))
        _ui_button(btns, "Download PDF", primary=True, width=124,
                   command=self._download).pack(side="right")
        _ui_button(btns, "Show as Text…", width=124,
                   command=self._show_text).pack(side="right", padx=(0, 6))
        _ui_button(btns, "Print…", width=90,
                   command=self._print).pack(side="right", padx=(0, 6))
        _ui_button(btns, "−", width=42,
                   command=lambda: self._zoom(-1)).pack(side="left")
        _ui_button(btns, "+", width=42,
                   command=lambda: self._zoom(+1)).pack(side="left",
                                                        padx=(6, 10))
        hist_btn = _history_button(app, btns)
        if hist_btn is not None:
            hist_btn.pack(side="left", padx=(0, 8))
        self._status_var = tk.StringVar(value="Loading the slip opinion…")
        _install_status_label(btns, self._status_var)

        body = _ui_frame(win)
        body.pack(fill="both", expand=True)
        self._body = body
        # The parts strip is packed on the right once sections are detected.
        self._nav: Optional[ttk.Frame] = None

        for seq in ("<Control-plus>", "<Control-equal>", "<Control-KP_Add>"):
            win.bind(seq, lambda _e: self._zoom(+1))
        for seq in ("<Control-minus>", "<Control-KP_Subtract>"):
            win.bind(seq, lambda _e: self._zoom(-1))
        win.bind("<Control-0>", lambda _e: self._zoom(0))

        if app is not None and hasattr(app, "record_case_view"):
            def reopen(app=app, url=url, title=title, description=description):
                _SlipOpinionWindow(app.root, url, title, app=app,
                                   description=description)
            key, label = f"slip:{url}", f"{title} (slip op.)"
            app.record_case_view(key, label, reopen)
            if hasattr(app, "register_case_window"):
                app.register_case_window(self, self._win, key, label, reopen)

        self._fetch()

    # -- shell plumbing ------------------------------------------------------

    def _post(self, fn, *args) -> None:
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass

    def _zoom(self, delta: int) -> None:
        if self._pane is not None:
            self._pane.zoom(delta)
            self._status_var.set(f"Zoom: {self._pane.zoom_percent()}%")

    def _download(self) -> None:
        if not self._bytes:
            return
        safe = re.sub(r"[^\w.-]+", "_", self._title).strip("_") or "slip_opinion"
        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{safe}.pdf", title="Download PDF", parent=self._win,
        )
        if not path:
            return
        try:
            with open(path, "wb") as fh:
                fh.write(self._bytes)  # the official file, unmodified
            self._status_var.set(f"Saved PDF to {path}")
        except Exception as exc:
            messagebox.showerror("Download PDF", str(exc), parent=self._win)

    def _print(self) -> None:
        if not self._bytes:
            return
        # Same file name "Download PDF" would offer for this slip opinion.
        stem = re.sub(r"[^\w.-]+", "_", self._title).strip("_") or "slip_opinion"
        path = _named_temp_pdf_path(stem)
        try:
            if self._pane is not None:
                self._pane.export_pdf(path)
            else:
                with open(path, "wb") as fh:
                    fh.write(self._bytes)
        except Exception:
            with open(path, "wb") as fh:
                fh.write(self._bytes)
        _print_pdf_file(self._win, path, self._status_var.set)

    # -- load / analyze ------------------------------------------------------

    def _fetch(self) -> None:
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            if messagebox.askyesno(
                "PDF viewer not installed",
                "Viewing PDFs inside the app needs two Python packages:\n\n"
                "    pip install pypdfium2 Pillow\n\n"
                "Open the slip opinion in your web browser instead?",
                parent=self._win,
            ):
                webbrowser.open(self._url)
            self._win.destroy()
            return

        def run() -> None:
            try:
                fetched = _fetch_pdf_bytes(self._url, timeout=45)
                if fetched is None:
                    raise RuntimeError("supremecourt.gov did not return a PDF")
                data, final_url = fetched
                self._url = final_url
            except Exception as exc:
                self._post(self._error, f"Could not load the opinion: {exc}")
                return
            self._post(self._show, data)

        threading.Thread(target=run, daemon=True).start()

    def _error(self, msg: str) -> None:
        self._status_var.set(msg)
        try:
            if messagebox.askyesno(
                "Slip opinion", msg + "\n\nOpen it in your browser instead?",
                parent=self._win,
            ):
                webbrowser.open(self._url)
        except tk.TclError:
            pass

    def _show(self, data: bytes) -> None:
        self._bytes = data
        try:
            width = max(560, self._win.winfo_width() - 220)
        except tk.TclError:
            width = 800
        try:
            pane = _PdfPane(
                self._body, data, width=width,
                link_style="recolor", uniform_crop=True,
            )
        except Exception as exc:
            self._error(f"Could not render the PDF: {exc}")
            return
        self._pane = pane
        pane.pack(side="left", fill="both", expand=True, padx=8, pady=4)
        self._status_var.set(
            "Scanning for citations and separate opinions…")
        threading.Thread(target=self._analyze, args=(data,),
                         daemon=True).start()

    def _analyze(self, data: bytes) -> None:
        """Worker: extract the text layer once; build citation links and the
        section list from it."""
        try:
            pages = _extract_pdf_text_pages(data)
        except Exception as exc:
            print(f"[slip] text extraction failed: {exc}")
            pages = []
        links: dict = {}
        quiet: set = set()
        sections: list = []
        if pages:
            try:
                links, quiet = _citation_links_from_visible_pdf_text(
                    data, pages)
            except Exception as exc:
                print(f"[slip] citation scan failed: {exc}")
            try:
                sections = slip_opinion.detect_sections(pages)
            except Exception as exc:
                print(f"[slip] section detection failed: {exc}")
        self._post(self._analyzed, pages, links, sections, quiet)

    def _analyzed(self, pages: list, links: dict, sections: list,
                  quiet: "set[int] | None" = None) -> None:
        if self._pane is None:
            return
        self._pages = pages
        self._pane.enable_find(pages)
        if links:
            self._pane.set_citation_links(
                links, self._open_cite, self._open_cite_browser,
                quiet_pages=quiet)
        self._build_nav(sections)
        n = sum(len(v) for v in links.values())
        bits = []
        if n:
            bits.append(f"{n} citation link{'s' if n != 1 else ''} "
                        "(shown in blue)")
        if len(sections) > 1:
            bits.append(f"{len(sections)} opinion parts")
        if pages:
            bits.append("Ctrl-F to search; drag to select text")
        self._status_var.set("; ".join(bits) or "Slip opinion loaded.")

    def _build_nav(self, sections: list) -> None:
        """The right-hand strip listing the opinion's parts; clicking one
        scrolls the PDF to that part's first page."""
        if len(sections) < 2 or self._pane is None:
            return
        nav = _build_pdf_parts_nav(self._body, sections, self._goto_page)
        nav.pack(side="right", fill="y", padx=(0, 8), pady=4)
        self._nav = nav

    def _goto_page(self, page: int) -> None:
        if self._pane is not None:
            self._pane.scroll_to_page(page)

    # -- citation dispatch ----------------------------------------------------

    def _open_cite(self, action: tuple, snippet: str) -> None:
        if self._app is not None:
            _follow_brief_action(self._app, self._win, action,
                                 self._status_var.set)
        else:
            _open_citation_in_browser(action, snippet)

    def _open_cite_browser(self, action: tuple, snippet: str) -> None:
        _open_citation_in_browser(action, snippet)

    # -- text view -------------------------------------------------------------

    def _show_text(self) -> None:
        if self._clean_text is not None:
            _SlipTextWindow(
                self._win, self._title, self._clean_text, app=self._app
            )
            return
        if not self._pages:
            self._status_var.set(
                "The text layer isn't ready yet — try again in a moment.")
            return
        self._status_var.set("Converting the PDF to text…")

        def run() -> None:
            try:
                text = slip_opinion.to_clean_text(self._pages)
            except Exception as exc:
                self._post(self._status_var.set, f"Text conversion failed: {exc}")
                return
            if self._description:
                text = (f"{self._title}\n\n{self._description}\n\n"
                        f"{'—' * 30}\n\n{text}")
            self._clean_text = text
            self._post(self._open_text_window)

        threading.Thread(target=run, daemon=True).start()

    def _open_text_window(self) -> None:
        self._status_var.set("Text ready.")
        if self._clean_text is not None:
            _SlipTextWindow(
                self._win, self._title, self._clean_text, app=self._app
            )


_ER_CAPTION_VS_RE = re.compile(r"\s+(?:against|versus)\s+", re.IGNORECASE)


def _er_caption_to_modern(name: str) -> str:
    """Fold an English Reports caption to the modern "A v B" shape the
    spotlight's name matcher splits on: "against"/"versus" become "v"
    ("John Entick, Clerk, versus Nathan Carrington…"), and failing those the
    semicolon dividing an Appellant; Respondent caption does ("George
    Booth,-Appellant; George, Earl of Warrington,-Respondent")."""
    s = _ER_CAPTION_VS_RE.sub(" v ", name)
    if ";" in s and not _NAME_PARTY_SPLIT_RE.search(s):
        s = s.replace(";", " v ", 1)
    return s


def _eng_rep_name_search(query: str, cap: int = 250) -> "list[eng_rep.ERCase]":
    """English Reports cases matching a typed case name, best first — the
    many-results counterpart of ``eng_rep.search_by_name``'s single strict
    spotlight hit.

    Candidates (any indexed case sharing an identifying word with the query)
    are ranked the way the spotlight ranks CourtListener name results: each is
    tiered by :func:`_match_tier` and only the best tier present survives (the
    case as captioned beats a swapped caption beats a one-party match — see
    :func:`_filter_to_best_tier`), ordered within the tier by
    :func:`_name_match_score`, then exact-word hits before prefix/fuzzy ones,
    then the shorter (more precise) name."""
    query = _er_caption_to_modern(query)
    qtokens = _name_tokens(query)
    if not qtokens:
        return []

    def shares_a_word(nset: frozenset) -> bool:
        # Cheap necessary condition before the real scoring: some query word
        # appears in the name — equal, or a ≥4-letter prefix either way as in
        # _token_close (whose fuzzy tail is left to _match_tier).
        return any(
            qt in nset or (len(qt) >= 4 and any(
                nt.startswith(qt) or (len(nt) >= 4 and qt.startswith(nt))
                for nt in nset))
            for qt in qtokens
        )

    scored: list[tuple[int, float, float, "eng_rep.ERCase"]] = []
    for _nn, nset, case in eng_rep.named_cases():
        if not shares_a_word(nset):
            continue
        name = _er_caption_to_modern(case.name)
        tier = _match_tier(query, name)
        if tier < 0:
            continue
        exact = sum(1 for qt in qtokens if qt in nset) / len(qtokens)
        scored.append((tier, _name_match_score(query, name), exact, case))
    if not scored:
        return []
    best = max(t for t, _s, _e, _c in scored)
    kept = [(s, e, c) for t, s, e, c in scored if t == best]
    kept.sort(key=lambda t: (-t[0], -t[1], len(t[2].name), t[2].year, t[2].num))
    return [c for _s, _e, c in kept[:cap]]


def _open_eng_rep(parent: tk.Misc, spec: str,
                  status=lambda _s: None, app=None) -> None:
    """Open an English Reports citation ("<vol>:<page>" spec): resolve it to the
    CommonLII case(s), let the user pick when a page holds several, and show the
    scan in-app (cached, with the CloudFlare hand-off) — or, when in-app fetching
    isn't available and it isn't cached, open it in the browser.  ``app`` (when
    the caller has one) enables the History dropdown on the viewer."""
    cases = eng_rep.resolve(spec)
    if not cases:
        vp = eng_rep.parse_spec(spec)
        if vp:
            status(f"{vp[0]} Eng. Rep. {vp[1]} isn't in the index — "
                   "searching CommonLII…")
            eng_rep_pdf.open_in_browser(eng_rep.search_url(*vp))
        else:
            status("Couldn't parse that English Reports citation.")
        return
    case = cases[0] if len(cases) == 1 else _choose_eng_rep_case(parent, cases)
    if case is None:
        return
    _open_eng_rep_case(parent, case, status, app=app)


def _open_eng_rep_case(parent: tk.Misc, case: "eng_rep.ERCase",
                       status=lambda _s: None, app=None) -> None:
    """Show one already-resolved English Reports case in the in-app scan viewer.
    Used when the exact case is known (a name-search hit from the spotlight, or
    a citation link), so it skips the same-page chooser :func:`_open_eng_rep`
    runs.  The viewer handles every outcome itself — disk cache, the in-app
    fetch, the CloudFlare/Firefox hand-off, and the browser fall-back — so the
    spotlight and a clicked link reach English Reports exactly the same way."""
    status(f"Opening {case.name[:40]} ({case.er_cite})…")
    _EngRepPdfWindow(parent, case, status, app=app)


# ---------------------------------------------------------------------------
# Open Brief — follow a highlighted citation to its source
# ---------------------------------------------------------------------------
# These mirror _ScholarTextWindow._follow_link but stand alone so the brief
# viewer (text or PDF) can open the same statute/regulation/rule/constitution
# viewers and the same case reader.

#: Categories used to colour-code highlights by what the citation points at.
def _brief_action_category(kind: str) -> str:
    if kind in ("cite", "url", "engrep", "recap", "fedcas"):
        return "case"  # English Reports, RECAP and Federal Cases too
    if kind == "const":
        return "const"
    return "statute"


def _open_citation_in_browser(action: tuple[str, str], text: str = "") -> None:
    """Open a brief citation in the user's web browser — a guaranteed-reliable
    fallback (right-click) that never touches the in-app window machinery:
    cases go to Google Scholar, link-out actions to their URL, and anything
    else to a web search of the citation text."""
    kind, value = action
    if kind in ("browse", "statpdf"):
        url = value
    elif kind == "recap":
        # The RECAP search on CourtListener, pre-filtered to the docket (or,
        # for a citation printing none, the case name), court and opinion
        # date the citation names.
        try:
            spec = json.loads(value)
        except Exception:
            spec = {}
        params = {"type": "rd", "q": "",
                  "entry_date_filed_after": spec.get("date", ""),
                  "entry_date_filed_before": spec.get("date", "")}
        if spec.get("docket"):
            params["docket_number"] = spec["docket"]
        elif spec.get("name"):
            params["case_name"] = spec["name"]
        if spec.get("court"):
            params["court"] = spec["court"]
        url = ("https://www.courtlistener.com/?"
               + urllib.parse.urlencode(params))
    elif kind == "cite":
        cite = value.split("@")[0]
        url = ("https://scholar.google.com/scholar?q="
               + urllib.parse.quote(f'"{cite}"'))
    elif kind == "fedcas":
        # Federal Cases number → the CourtListener search the in-app lookup
        # would run, pre-filtered to the era: by printed case name when the
        # citation gives one, else by the "Case No. N" phrase.
        try:
            spec = json.loads(value)
        except Exception:
            spec = {}
        name = spec.get("name") or ""
        q = (f'caseName:"{name}"' if name
             else f'"Case No. {fed_cas.pretty_number(spec.get("no") or "")}"')
        url = ("https://www.courtlistener.com/?"
               + urllib.parse.urlencode(
                   {"q": q, "type": "o", "filed_before": "1882-12-31"}))
    elif kind == "engrep":
        # English Reports → the CommonLII case page (first case at that page),
        # not the .pdf directly: the origin hotlink-blocks the scan unless you
        # reach it from a link on the site.  Falls back to a CommonLII search
        # when the citation isn't in our index.
        cases = eng_rep.resolve(value)
        vp = eng_rep.parse_spec(value)
        url = (cases[0].web_url if cases
               else (eng_rep.search_url(*vp) if vp else ""))
        if not url:
            return
    else:
        q = (text or value).strip()
        url = "https://www.google.com/search?q=" + urllib.parse.quote(q)
    try:
        webbrowser.open(url)
    except Exception:
        pass


def _open_recap_citation(app: "CourtListenerGUI", parent: tk.Misc,
                         spec_json: str, status=lambda _s: None) -> None:
    """Open an unpublished opinion from CourtListener's RECAP (PACER)
    archive.  The citation may print a WL/LEXIS number with a docket ("No.
    12-6371, 2024 WL 1327972 (D.N.J. Mar. 28, 2024)"), a docket alone
    ("No. 23-1971 (4th Cir. Feb. 12, 2024)"), or a WL/LEXIS number alone —
    then the case name, court and date carry the lookup.  When RECAP hasn't
    the document (or its PDF), falls back to the ordinary citation path
    (Google Scholar — by number, or by name for a docket-only citation —
    then the CourtListener opinion text)."""
    try:
        spec = json.loads(spec_json)
    except Exception:
        status("Couldn't read that citation.")
        return
    cite = spec.get("cite") or ""
    docket = spec.get("docket") or ""
    name = spec.get("name") or ""
    label = cite or (f"No. {docket}" if docket else name) or "unpublished opinion"
    title = f"{name} — {label}" if name and name != label else (name or label)

    def safe_status(s: str) -> None:
        try:
            status(s)
        except tk.TclError:
            pass

    safe_status(f"Looking up {label} on RECAP…")
    # Resolve these on the calling (main) thread — they touch tk variables.
    client = app._get_client() if app._token_var.get().strip() else None
    fetcher = app._get_scholar() if _SCHOLAR_AVAILABLE else None

    def run() -> None:
        info = None
        try:
            info = cl_api.find_recap_document(
                docket, spec.get("court"), spec.get("date", ""),
                session=(client._session if client is not None else None),
                case_name=name or None,
            )
        except Exception as exc:
            print(f"[recap] lookup failed for {label!r}: {exc}")
        if info and info.get("pdf_url"):
            def open_pdf(url=info["pdf_url"], t=title):
                _PdfWindow(app.root, url, t, safe_status, app=app,
                           is_case=True)
            app._post_root(open_pdf)
            app._post_root(lambda: safe_status(
                f"Opened {label} from RECAP ({info.get('description') or 'document'})."))
            return
        # RECAP doesn't have the PDF — fall back to the regular citation
        # path; failing that, at least point the browser at the docket.
        safe_status(f"RECAP hasn't {label} — trying Google Scholar…")
        ok = False
        if cite and (fetcher is not None or client is not None):
            try:
                ok = app._try_open_citation(name, cite, "", fetcher, client,
                                            prefetch_pdf=False)
            except Exception as exc:
                print(f"[recap] scholar fallback failed for {label!r}: {exc}")
        elif name and fetcher is not None:
            # A docket-only citation has no WL/LEXIS number for Scholar to
            # resolve — search it by case name, scoped to the opinion year.
            result = None
            try:
                result = fetcher.fetch_by_name(
                    name, (spec.get("date") or "")[:4] or None)
            except Exception as exc:
                print(f"[recap] scholar name lookup failed for {label!r}: {exc}")
            if result:
                def open_scholar(u=result[0], h=result[1]) -> None:
                    try:
                        _ScholarTextWindow(app.root, app, u, h, item=None,
                                           prefetch_pdf=False)
                    except tk.TclError:
                        pass
                app._post_root(open_scholar)
                ok = True
        if ok:
            app._post_root(lambda: safe_status(f"Opened {label}."))
        elif info and info.get("web_url"):
            webbrowser.open(info["web_url"])
            app._post_root(lambda: safe_status(
                f"{label}: PDF not in RECAP — opened the docket in your "
                "browser."))
        else:
            app._post_root(lambda: safe_status(f"Not found: {label}"))

    threading.Thread(target=run, daemon=True).start()


def _open_fedcas_citation(app: "CourtListenerGUI", parent: tk.Misc,
                          spec_json: str, status=lambda _s: None) -> None:
    """Open a Federal Cases citation given by case number ("Cole v. The
    Atlantic, Case No. 2,976").  No public index maps the numbers to
    reporter pages, so the case is found live on CourtListener
    (:func:`courtlistener.find_fedcas_case`): searched by the printed case
    name — forgivingly, the sources being OCR — with the winner confirmed
    by the number at the head of its headnotes or, failing that, by the
    F. Cas. volume the number's alphabetical position dictates.  When
    CourtListener doesn't hold the case at all, falls back to a Google
    Scholar name search before giving up."""
    try:
        spec = json.loads(spec_json)
    except Exception:
        status("Couldn't read that citation.")
        return
    no = str(spec.get("no") or "")
    name = spec.get("name") or ""
    pretty = fed_cas.pretty_number(no)
    label = (f"{name}, Case No. {pretty}" if name else f"Case No. {pretty}")

    def safe_status(s: str) -> None:
        try:
            status(s)
        except tk.TclError:
            pass

    safe_status(f"Looking up {label} on CourtListener…")
    # Resolve these on the calling (main) thread — they touch tk variables.
    client = app._get_client() if app._token_var.get().strip() else None
    fetcher = app._get_scholar() if _SCHOLAR_AVAILABLE else None

    def run() -> None:
        item = None
        try:
            item = cl_api.find_fedcas_case(
                no, name or None,
                session=(client._session if client is not None else None),
            )
        except Exception as exc:
            print(f"[fedcas] lookup failed for {label!r}: {exc}")
        if item and client is not None:
            try:
                parts, blocks, plain, _cluster = _assemble_case_parts(
                    client, item)
            except Exception as exc:
                print(f"[fedcas] assemble failed for {label!r}: {exc}")
                parts, plain = [], ""
            if parts or plain:
                how = ("confirmed by its headnote number"
                       if item.get("matched_by") == "headnote"
                       else "matched by name and F. Cas. volume")
                cite = "; ".join(item.get("citation") or [])

                def open_cl(it=item) -> None:
                    try:
                        _ScholarTextWindow(
                            app.root, app, "", "", item=it, cl_text=plain,
                            cl_parts=parts, cl_blocks=blocks,
                        )
                    except tk.TclError:
                        pass

                app._post_root(open_cl)
                app._post_root(lambda: safe_status(
                    f"Opened {label}" + (f" — {cite}" if cite else "")
                    + f" ({how})."))
                return
        # CourtListener doesn't hold the case (or no API token): try a
        # Google Scholar search by the printed name, era-scoped.
        if name and fetcher is not None:
            result = None
            try:
                result = fetcher.fetch_by_name(name, None)
            except Exception as exc:
                print(f"[fedcas] scholar name lookup failed for {label!r}: {exc}")
            if result:
                def open_scholar(u=result[0], h=result[1]) -> None:
                    try:
                        _ScholarTextWindow(app.root, app, u, h, item=None,
                                           prefetch_pdf=False)
                    except tk.TclError:
                        pass
                app._post_root(open_scholar)
                app._post_root(lambda: safe_status(
                    f"CourtListener hasn't {label} — opened a Google "
                    "Scholar name match (verify it yourself)."))
                return
        app._post_root(lambda: safe_status(f"Not found: {label}"))

    threading.Thread(target=run, daemon=True).start()


def _follow_brief_action(app: "CourtListenerGUI", parent: tk.Misc,
                         action: tuple[str, str],
                         status=lambda _s: None,
                         prefetch_pdf: bool = True) -> None:
    """Open whatever a highlighted brief citation points at, reusing the exact
    paths the rest of the app uses — so briefs behave like the opinion reader
    and the Quick Look Up dialog rather than a parallel implementation:

      * statutes / rules / regulations / Constitution → ``_open_statute_action``
        (in-app viewer, or a browser link-out for link-only states),
      * cases → ``CourtListenerGUI._try_open_citation`` (Google Scholar by
        citation with a name retry, the static.case.law shortcut for Federal
        Appendix scans, then the CourtListener text), with the pincite jump.
    """
    kind, value = action
    if kind in _STATUTE_SOURCES or kind in ("browse", "statpdf"):
        _open_statute_action(parent, action, status, app=app)
        return
    if kind == "engrep":
        _open_eng_rep(parent, value, status, app=app)
        return
    if kind == "recap":
        _open_recap_citation(app, parent, value, status)
        return
    if kind == "fedcas":
        _open_fedcas_citation(app, parent, value, status)
        return
    if kind != "cite":
        status("Don't know how to open that citation.")
        return

    cite, _, pin = value.partition("@")
    fetcher = app._get_scholar() if _SCHOLAR_AVAILABLE else None
    client = app._get_client() if app._token_var.get().strip() else None
    if fetcher is None and client is None:
        status("Neither Google Scholar nor CourtListener is available.")
        return

    def safe_status(s: str) -> None:
        try:
            status(s)
        except tk.TclError:
            pass

    safe_status(f"Opening {cite}…")

    def run() -> None:
        ok = app._try_open_citation("", cite, pin, fetcher, client,
                                    prefetch_pdf=prefetch_pdf)
        try:
            parent.after(0, lambda: safe_status(
                f"Opened {cite}." if ok else f"Not found: {cite}"))
        except tk.TclError:
            pass

    threading.Thread(target=run, daemon=True).start()


# Background tints for highlighted citations in the brief reader,
# keyed by category (see _brief_action_category).
_BRIEF_TINTS = {"case": "#cfe2ff", "statute": "#d6f0d6", "const": "#fff3bf"}


class _BriefTextWindow:
    """Renders a brief's text (extracted from PDF, Word, RTF or plain text) with
    every detected citation highlighted (by category) and clickable — cases open
    in the Scholar reader, statutes / rules / regulations / the Constitution in
    the statute viewer; right-click opens any citation in the web browser."""

    def __init__(self, parent: tk.Misc, app: "CourtListenerGUI",
                 name: str, text: str) -> None:
        self._app = app
        self._name = name
        self._src = text
        self._link_actions: dict[str, tuple[str, str]] = {}
        self._link_n = 0

        self._win = _secondary_view_host(parent, app)
        self._win.title(f"Brief — {name}")
        self._win.geometry("860x720")
        self._win.minsize(500, 320)
        self._window_menubar = _install_window_menubar(app, self._win)
        self._build_ui()
        self._render()
        if hasattr(app, "register_secondary_window"):
            def reopen(app=app, source=self) -> None:
                _BriefTextWindow(
                    app.root, app, source._name, source._src
                )
            app.register_secondary_window(
                self, self._win, f"brief-text:{id(self)}",
                f"Brief — {name}", reopen,
            )

    def _build_ui(self) -> None:
        win = self._win
        legend = _ui_frame(win)
        legend.pack(fill="x", padx=12, pady=(12, 0))
        _ui_label(legend, "Citations are highlighted and clickable:",
                  muted=True).pack(side="left")
        for cat, label in (("case", "Cases"), ("statute", "Statutes / Rules"),
                           ("const", "Constitution")):
            tk.Label(legend, text=f" {label} ", background=_BRIEF_TINTS[cat],
                     foreground="#222222").pack(side="left", padx=(8, 0))

        text_frame = _ui_frame(win)
        text_frame.pack(fill="both", expand=True, padx=12, pady=8)
        base = tkfont.Font(family="Georgia", size=_OPINION_FONT_PT)
        txt = tk.Text(text_frame, wrap="word", font=base, padx=14, pady=10)
        self._text = txt
        vsb = ttk.Scrollbar(text_frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)
        for cat, color in _BRIEF_TINTS.items():
            txt.tag_configure(cat, background=color)
        txt.tag_configure("brieflink", underline=False)
        txt.tag_bind("brieflink", "<Enter>",
                     lambda _e: txt.config(cursor="hand2"))
        txt.tag_bind("brieflink", "<Leave>", lambda _e: txt.config(cursor=""))
        self._finder = _TextFinder(win, txt, text_frame)

        bottom = _ui_frame(win)
        bottom.pack(fill="x", padx=12, pady=(0, 10))
        # Compile every cited authority into one downloadable zip (see
        # _download_all_cases): cases via static.case.law / Scholar / CL,
        # statutes and rules as their text, all Bluebook-named.
        self._download_btn = _ui_button(
            bottom, "Download Cited Cases…", primary=True, width=188,
            command=self._download_all_cases,
        )
        self._download_btn.pack(side="right", padx=(8, 0))
        self._status_var = tk.StringVar(value="")
        _install_status_label(bottom, self._status_var, padx=(0, 0))

    def _render(self) -> None:
        txt = self._text
        src = self._src
        links = detect_brief_links(src)
        pos = 0
        for start, end, action in links:
            if start < pos:
                continue
            if start > pos:
                txt.insert("end", src[pos:start])
            self._link_n += 1
            tag = f"lnk{self._link_n}"
            seg = re.sub(r"\s+", " ", src[start:end]).strip()
            self._link_actions[tag] = (action, seg)
            cat = _brief_action_category(action[0])
            txt.insert("end", src[start:end], (cat, "brieflink", tag))
            txt.tag_bind(tag, "<Button-1>",
                         lambda _e, t=tag: self._follow(t))
            txt.tag_bind(tag, "<Button-3>",
                         lambda _e, t=tag: self._follow_browser(t))
            pos = end
        if pos < len(src):
            txt.insert("end", src[pos:])
        txt.config(state="disabled")
        n = len(links)
        self._status_var.set(
            f"{n} citation{'' if n == 1 else 's'} found — left-click to open, "
            "right-click to open in browser." if n else "No citations detected."
        )

    def _follow(self, tag: str):
        entry = self._link_actions.get(tag)
        if entry:
            _follow_brief_action(self._app, self._win, entry[0],
                                 self._status_var.set)
        return "break"

    def _follow_browser(self, tag: str):
        entry = self._link_actions.get(tag)
        if entry:
            _open_citation_in_browser(entry[0], entry[1])
        return "break"

    # ------------------------------------------------------------------
    # Download every cited authority into one zip
    # ------------------------------------------------------------------

    def _download_all_cases(self) -> None:
        """Compile every authority the brief cites into a single zip the user
        chooses, each file named with its proper Bluebook citation.  Federal
        Appendix cases come from static.case.law's JSON as text; unpublished
        opinions from RECAP; every other case is exported as RTF from the saved
        Google Scholar copy, then CourtListener, then (only if it must) a fresh
        Google Scholar search; statutes and rules as their text.  The work runs
        off-thread behind a progress dialog."""
        import brief_compiler

        authorities = brief_compiler.collect_authorities(self._src)
        if not authorities:
            self._status_var.set("No citations to download.")
            return
        n_cases = sum(1 for a in authorities if a.is_case)
        n_other = len(authorities) - n_cases
        path = filedialog.asksaveasfilename(
            parent=self._win, title="Save cited authorities as…",
            defaultextension=".zip",
            filetypes=[("Zip archive", "*.zip"), ("All files", "*.*")],
            initialfile="Cited Authorities.zip",
        )
        if not path:
            return

        # Resolve the CourtListener client and Scholar fetcher here on the main
        # thread (they read tk variables); either may be None.
        client = None
        try:
            if self._app is not None and self._app._token_var.get().strip():
                client = self._app._get_client()
        except Exception as exc:
            print(f"[compile] client unavailable: {exc}")
        fetcher = None
        try:
            if _SCHOLAR_AVAILABLE and self._app is not None:
                fetcher = self._app._get_scholar()
        except Exception as exc:
            print(f"[compile] scholar unavailable: {exc}")

        resolver = _BriefCompileResolver(client, fetcher)
        dialog = _CompileProgressDialog(
            self._win, total=len(authorities),
            subtitle=f"{n_cases} case{'' if n_cases == 1 else 's'}"
                     + (f", {n_other} statute/rule{'' if n_other == 1 else 's'}"
                        if n_other else ""))
        self._status_var.set(
            f"Downloading {len(authorities)} cited "
            f"{'authority' if len(authorities) == 1 else 'authorities'}…")

        def progress(done: int, total: int, message: str) -> None:
            dialog.post_progress(done, total, message)

        def run() -> None:
            try:
                summary = brief_compiler.compile_to_zip(
                    authorities, resolver, path,
                    progress=progress,
                    should_cancel=dialog.is_cancelled,
                )
            except Exception as exc:
                dialog.post_error(str(exc))
                self._safe_status(f"Download failed: {exc}")
                return
            dialog.post_done(summary)
            msg = (f"Saved {summary.saved} of {len(authorities)} to "
                   f"{os.path.basename(path)}"
                   + (" (cancelled)" if summary.cancelled else ""))
            self._safe_status(msg)

        threading.Thread(target=run, daemon=True).start()

    def _safe_status(self, s: str) -> None:
        try:
            self._win.after(0, self._status_var.set, s)
        except tk.TclError:
            pass


def _parse_opinion_blocks_safe(html: str):
    """``parse_opinion_blocks`` with the import and failures swallowed, for the
    compiler's worker thread."""
    try:
        from google_scholar import parse_opinion_blocks
        return parse_opinion_blocks(html or "")
    except Exception as exc:
        print(f"[compile] block parse failed: {exc}")
        return []


def _recap_stem(name: str, cite: str, docket: str, court, date: str) -> str:
    """A Bluebook file stem for an unpublished opinion:
    ``Name, No. Docket, WL-cite (Court Date)``."""
    bits = [b for b in (name, f"No. {docket}" if docket else "", cite) if b]
    stem = ", ".join(bits) or (name or "Unpublished opinion")
    court_label = ""
    try:
        import court_catalog
        court_label = court_catalog.COURT_BLUEBOOK.get(
            str(court or "").strip().lower(), "")
    except Exception:
        court_label = ""
    year = date[:4] if date and len(date) >= 4 else ""
    paren = " ".join(p for p in (court_label, year) if p)
    if paren:
        stem = f"{stem} ({paren})"
    return stem


class _BriefCompileResolver:
    """The fetching side of :mod:`brief_compiler`, built from the helpers the
    GUI already owns.  Each method does one lookup; ``brief_compiler`` decides
    the order they're tried in (so that ordering stays unit-testable)."""

    def __init__(self, client, fetcher) -> None:
        self._client = client
        self._fetcher = fetcher

    # -- cases ---------------------------------------------------------------

    def pdf_bytes(self, url: str):
        try:
            got = _fetch_pdf_bytes(url, client=self._client)
        except Exception as exc:
            print(f"[compile] pdf fetch failed for {url}: {exc}")
            return None
        return got[0] if got else None

    def is_fed_appx(self, cite: str) -> bool:
        return bool(_FED_APPX_RE.search(cite or ""))

    def fed_appx_text(self, cite: str):
        """(opinion text, Bluebook citation) parsed from static.case.law's
        per-case JSON, or None.  Federal Appendix scans have no reliable text
        layer, so the JSON — which carries the opinion and authoritative
        metadata — is the source."""
        try:
            return _case_law_json_case(cite)
        except Exception as exc:
            print(f"[compile] case.law JSON failed for {cite!r}: {exc}")
            return None

    def case_rtf(self, cite: str, name: str):
        """(rtf, Bluebook citation, source) for a published case, or None.

        The opinion is taken, in order, from the saved Google Scholar copy, then
        CourtListener, then — only if it must — a fresh Google Scholar search.
        The citation for the file name comes from the opinion and is supplemented
        by the CourtListener API."""
        blocks = None
        item = None
        source = ""
        # 1) Saved Google Scholar copy (cache only — no network).
        if self._fetcher is not None:
            try:
                cached = self._fetcher.get_cached(f"cite2:{cite.strip()}")
            except Exception:
                cached = None
            if cached:
                blocks = _parse_opinion_blocks_safe(cached[1])
                if blocks:
                    source = "Google Scholar (cache)"
        # 2) CourtListener.
        if not blocks and self._client is not None:
            try:
                item = _cl_item_for_citation(self._client, cite, name=name)
                if item is not None:
                    _parts, cl_blocks, _plain, _cluster = _assemble_case_parts(
                        self._client, item)
                    if cl_blocks:
                        blocks = cl_blocks
                        source = "CourtListener"
            except Exception as exc:
                print(f"[compile] CL text failed for {cite!r}: {exc}")
        # 3) Fresh Google Scholar search — the last resort.
        if not blocks and self._fetcher is not None:
            try:
                result = self._fetcher.fetch_by_citation(cite)
                if not result and name:
                    result = self._fetcher.fetch_by_name(name)
            except Exception as exc:
                print(f"[compile] scholar search failed for {cite!r}: {exc}")
                result = None
            if result:
                blocks = _parse_opinion_blocks_safe(result[1])
                if blocks:
                    source = "Google Scholar (search)"
        if not blocks:
            return None
        # Supplement the citation via CourtListener when the text came from
        # Scholar, so the file name is authoritative (skipped when CL already
        # resolved the case above).
        if item is None and self._client is not None:
            try:
                item = _cl_item_for_citation(self._client, cite, name=name)
            except Exception:
                item = None
        stem = self._case_stem(item, blocks, cite, name)
        return _opinion_rtf_from_blocks(blocks, stem), stem, source

    def recap_pdf(self, spec_json: str):
        """(pdf bytes, Bluebook citation) for an unpublished opinion from the
        RECAP/PACER archive, or None — reusing the app's existing RECAP
        document downloader."""
        if self._client is None:
            return None
        try:
            spec = json.loads(spec_json)
        except Exception:
            return None
        docket = spec.get("docket") or ""
        name = spec.get("name") or ""
        court = spec.get("court")
        date = spec.get("date", "")
        cite = spec.get("cite") or ""
        try:
            info = cl_api.find_recap_document(
                docket, court, date,
                session=self._client._session, case_name=name or None)
        except Exception as exc:
            print(f"[compile] recap lookup failed: {exc}")
            return None
        if not info or not info.get("pdf_url"):
            return None
        data = self.pdf_bytes(info["pdf_url"])
        if not data:
            return None
        return data, _recap_stem(name, cite, docket, court, date)

    def _case_stem(self, item, blocks, cite: str, name: str) -> str:
        """The Bluebook file stem for a case — from the CourtListener record
        when available, else scraped from the opinion's own header."""
        if item:
            stem = _build_default_filename(item)
            if stem and stem != "opinion":
                return stem
        return _build_default_filename(
            _scholar_item_from_blocks(blocks, fallback_name=name,
                                      fallback_cite=cite))

    # -- statutes / rules / regulations / Constitution -----------------------

    def statute_text(self, kind: str, spec: str):
        mod = _STATUTE_SOURCES.get(kind)
        if mod is None:
            return None
        try:
            title, section, _subs = (spec.split(":", 2) + ["", "", ""])[:3]
            doc = mod.load_section(title, section)
            label = mod.spec_label(spec)
        except Exception as exc:
            print(f"[compile] statute load failed for {spec!r}: {exc}")
            return None
        body = _statute_doc_to_text(doc)
        if not body.strip():
            return None
        return label, body

    def statute_pdf_bytes(self, url: str):
        return self.pdf_bytes(url)

    def authority_label(self, kind: str, value: str) -> str:
        if kind == "statpdf":
            return _stat_cite_from_url(value)
        mod = _STATUTE_SOURCES.get(kind)
        if mod is not None:
            try:
                return mod.spec_label(value)
            except Exception:
                pass
        return value


def _statute_doc_to_text(doc) -> str:
    """Render a loaded statute/rule/regulation document to plain text: the
    section heading, then each paragraph indented to its hierarchy level, then
    the source credit."""
    lines: list[str] = []
    label = getattr(doc, "label", "") or ""
    if label:
        lines.append(label)
        lines.append("")
    for entry in getattr(doc, "paras", []):
        try:
            _kind, indent, text = entry
        except Exception:
            continue
        if text is None:
            continue
        prefix = "    " * max(0, int(indent or 0))
        lines.append(prefix + str(text))
    credit = getattr(doc, "source_credit", "") or getattr(doc, "source_name", "")
    if credit:
        lines.append("")
        lines.append(f"Source: {credit}")
    return "\n".join(lines).strip()


class _CompileProgressDialog:
    """A small modal-ish dialog tracking the "Download Cited Cases" run: a
    progress bar, the item in flight, a scrolling log of what was found where,
    and a Cancel button that becomes Close when the run ends."""

    def __init__(self, parent: tk.Misc, total: int, subtitle: str = "") -> None:
        self._total = total
        self._cancel = threading.Event()
        self._done = False

        self._win = _ui_toplevel(parent)
        self._win.title("Downloading cited authorities")
        self._win.geometry("560x420")
        self._win.minsize(420, 300)
        try:
            self._win.transient(parent.winfo_toplevel())
        except Exception:
            pass

        head = _ui_frame(self._win)
        head.pack(fill="x", padx=14, pady=(14, 4))
        _ui_label(head, "Compiling the brief's cited authorities", size=14,
                  weight="bold", anchor="w").pack(side="left")
        if subtitle:
            sub = _ui_frame(self._win)
            sub.pack(fill="x", padx=14, pady=(0, 4))
            _ui_label(sub, subtitle, muted=True, anchor="w").pack(side="left")

        self._bar = ttk.Progressbar(self._win, mode="determinate",
                                    maximum=max(1, total))
        self._bar.pack(fill="x", padx=14, pady=(6, 4))
        self._item_var = tk.StringVar(value="Starting…")
        _ui_label(self._win, anchor="w", muted=True,
                  textvariable=self._item_var).pack(fill="x", padx=14)

        log_frame = _ui_frame(self._win)
        log_frame.pack(fill="both", expand=True, padx=14, pady=(6, 6))
        self._log = tk.Text(log_frame, wrap="word", height=10,
                            state="disabled", padx=8, pady=6,
                            font=("TkDefaultFont", 10))
        lvsb = ttk.Scrollbar(log_frame, orient="vertical",
                             command=self._log.yview)
        self._log.configure(yscrollcommand=lvsb.set)
        lvsb.pack(side="right", fill="y")
        self._log.pack(side="left", fill="both", expand=True)
        self._log.tag_configure("ok", foreground="#15803d")
        self._log.tag_configure("miss", foreground="#a31515")

        btnrow = _ui_frame(self._win)
        btnrow.pack(fill="x", padx=14, pady=(0, 12))
        self._btn = _ui_button(btnrow, "Cancel", command=self._on_button,
                               width=110)
        self._btn.pack(side="right")
        self._win.protocol("WM_DELETE_WINDOW", self._on_button)

    # -- cross-thread posting -----------------------------------------------

    def _post(self, fn, *args) -> None:
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass

    def post_progress(self, done: int, total: int, message: str) -> None:
        self._post(self._apply_progress, done, total, message)

    def _apply_progress(self, done: int, total: int, message: str) -> None:
        try:
            self._bar.configure(value=done)
            self._item_var.set(f"[{min(done + 1, total)}/{total}]  {message}")
        except tk.TclError:
            pass

    def post_done(self, summary) -> None:
        self._post(self._apply_done, summary)

    def _apply_done(self, summary) -> None:
        self._done = True
        try:
            self._bar.configure(value=self._total)
            self._log.config(state="normal")
            for r in summary.results:
                if r.ok:
                    self._log.insert("end", f"✓  {r.filename}\n", ("ok",))
                    self._log.insert(
                        "end", f"      {r.source}\n")
                else:
                    self._log.insert(
                        "end", f"✗  {r.authority.label()}\n", ("miss",))
                    if r.detail:
                        self._log.insert("end", f"      {r.detail}\n")
            self._log.config(state="disabled")
            self._log.see("end")
            head = (f"Done — saved {summary.saved} of "
                    f"{len(summary.results)}"
                    + (" (cancelled)" if summary.cancelled else "") + ".")
            self._item_var.set(head)
            self._btn.configure(text="Close")
        except tk.TclError:
            pass

    def post_error(self, message: str) -> None:
        self._post(self._apply_error, message)

    def _apply_error(self, message: str) -> None:
        self._done = True
        try:
            self._item_var.set(f"Failed: {message}")
            self._btn.configure(text="Close")
        except tk.TclError:
            pass

    # -- cancel / close ------------------------------------------------------

    def is_cancelled(self) -> bool:
        return self._cancel.is_set()

    def _on_button(self) -> None:
        if self._done:
            try:
                self._win.destroy()
            except tk.TclError:
                pass
            return
        self._cancel.set()
        try:
            self._item_var.set("Cancelling — finishing the current item…")
            self._btn.configure(state="disabled")
        except tk.TclError:
            pass


class _LinkedPdfWindow:
    """Show an imported PDF *as a PDF* (the zoomable, cropped pane) with every
    detected citation clickable, styled as light-blue *text* rather than
    highlight boxes (``link_style="recolor"``, the slip-opinion viewer's
    approach) — cases open in the Scholar reader, statutes/rules/regulations/
    Constitution in the statute viewer, right-click opens in the browser.

    The citation scan runs on a background thread; PDFium access (here and in the
    pane's rendering) is serialized through ``_PDFIUM_LOCK`` so the scan and the
    render never call the C library at the same time — the threading conflict
    that sank the earlier attempt.  Detection produces only plain data, which is
    handed to the pane back on the main thread.
    """

    def __init__(self, parent: tk.Misc, app: "CourtListenerGUI",
                 pdf_bytes: bytes, name: str) -> None:
        self._app = app
        self._name = name
        self._bytes = pdf_bytes
        self._pane: Optional[_PdfPane] = None
        self._closed = False

        self._win = _secondary_view_host(parent, app)
        self._win.title(f"PDF citations — {name}")
        self._win.geometry(
            _fit_toplevel_geometry(
                self._win, 860, 920, min_width=520, min_height=360,
                bottom_gap=72,
            )
        )
        self._win.minsize(520, 360)
        self._win.bind("<Destroy>", self._on_destroy)
        self._window_menubar = _install_window_menubar(app, self._win)

        legend = _ui_frame(self._win)
        legend.pack(fill="x", padx=12, pady=(12, 0))
        # Links are recolored blue in place (no category highlight boxes), so
        # the legend is a single line rather than the brief reader's swatches.
        self._legend_lbl = _ui_label(legend, "Scanning for citations…",
                                     muted=True)
        self._legend_lbl.pack(side="left")

        self._body = _ui_frame(self._win)
        self._body.pack(fill="both", expand=True)

        btns = _ui_frame(self._win)
        btns.pack(fill="x", side="bottom", padx=12, pady=(0, 10))
        _ui_button(btns, "Download Cropped PDF", primary=True, width=176,
                   command=self._download).pack(side="right")
        _ui_button(btns, "−", width=42,
                   command=lambda: self._zoom(-1)).pack(side="left")
        _ui_button(btns, "+", width=42,
                   command=lambda: self._zoom(+1)).pack(side="left", padx=(6, 10))
        self._status_var = tk.StringVar(value="Loading PDF…")
        _install_status_label(btns, self._status_var)

        for seq in ("<Control-plus>", "<Control-equal>", "<Control-KP_Add>"):
            self._win.bind(seq, lambda _e: self._zoom(+1))
        for seq in ("<Control-minus>", "<Control-KP_Subtract>"):
            self._win.bind(seq, lambda _e: self._zoom(-1))
        self._win.bind("<Control-0>", lambda _e: self._zoom(0))

        if hasattr(app, "register_secondary_window"):
            def reopen(app=app, source=self) -> None:
                _LinkedPdfWindow(
                    app.root, app, source._bytes, source._name
                )
            app.register_secondary_window(
                self, self._win, f"linked-pdf:{id(self)}",
                f"PDF citations — {name}", reopen,
            )
        self._show()

    def _post(self, fn, *args) -> None:
        if self._closed:
            return
        try:
            self._win.after(0, fn, *args)
        except tk.TclError:
            pass

    def _on_destroy(self, event: tk.Event) -> None:
        if event.widget is self._win:
            self._closed = True

    def _zoom(self, delta: int) -> None:
        if self._pane is not None:
            self._pane.zoom(delta)
            self._status_var.set(f"Zoom: {self._pane.zoom_percent()}%")

    def _show(self) -> None:
        try:
            import pypdfium2  # noqa: F401
            from PIL import ImageTk  # noqa: F401
        except ImportError:
            # No in-app PDF viewer — fall back to the text reader, which links
            # the same citations (just not drawn on the page).
            name = self._win.title().replace("PDF citations — ", "")
            self._win.destroy()
            self._app._open_brief_from_bytes(self._bytes, name)
            return
        try:
            # Citation links are painted as light-blue text on the page (the
            # slip-opinion viewer's style) — no highlight boxes.
            pane = _PdfPane(self._body, self._bytes, width=780,
                            link_style="recolor")
        except Exception as exc:
            messagebox.showerror("Import PDF", str(exc), parent=self._win)
            self._win.destroy()
            return
        pane.pack(fill="both", expand=True, padx=8, pady=4)
        self._pane = pane
        self._status_var.set("Scanning for citations…")
        threading.Thread(target=self._scan, daemon=True).start()

    def _scan(self) -> None:
        try:
            pages = _extract_pdf_text_pages(self._bytes)   # one extraction pass
            links, quiet = _citation_links_from_visible_pdf_text(
                self._bytes, pages)
        except Exception as exc:
            self._post(self._scan_failed, str(exc))
            return
        self._post(self._scan_done, links, pages, quiet)

    def _scan_failed(self, msg: str) -> None:
        self._legend_lbl.configure(text="Citation scan failed:")
        self._status_var.set(msg)

    def _scan_done(self, links: dict, pages: list,
                   quiet: "set[int] | None" = None) -> None:
        if self._closed or self._pane is None:
            return
        self._pane.set_citation_links(links, self._open_cite,
                                      self._open_cite_browser,
                                      quiet_pages=quiet)
        self._pane.enable_find(pages)   # Ctrl-F searches the text layer
        n = sum(len(v) for v in links.values())
        n_quiet = sum(len(v) for pg, v in links.items() if pg in (quiet or ()))
        has_text = any(pages)
        self._legend_lbl.configure(
            text=("Citations are clickable (not colored on scanned pages)."
                  if n and n_quiet
                  else "Citations are shown in blue and clickable." if n
                  else "No citations detected."))
        msg = (f"{n} citation link{'' if n == 1 else 's'} — left-click to open, "
               "right-click for the browser." if n
               else "No citations detected in this PDF's text layer.")
        if has_text:
            msg += "    Ctrl-F searches; drag selects text (Ctrl+C copies)."
        self._status_var.set(msg)

    def _open_cite(self, action: tuple, snippet: str) -> None:
        # prefetch_pdf=False: warming a second big PDF while this viewer's
        # pane is live renders/extracts in parallel and can hang the app.
        _follow_brief_action(self._app, self._win, action, self._safe_status,
                             prefetch_pdf=False)

    def _open_cite_browser(self, action: tuple, snippet: str) -> None:
        _open_citation_in_browser(action, snippet)

    def _safe_status(self, s: str) -> None:
        try:
            self._status_var.set(s)
        except tk.TclError:
            pass

    def _download(self) -> None:
        if self._pane is None:
            return
        safe = re.sub(r"[^\w.-]+", "_", self._win.title()).strip("_") or "document"
        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{safe}.pdf", title="Download PDF", parent=self._win,
        )
        if not path:
            return
        try:
            self._pane.export_best(path)
        except Exception:
            try:
                with open(path, "wb") as fh:
                    fh.write(self._bytes)
            except Exception as exc:
                messagebox.showerror("Download PDF", str(exc), parent=self._win)
                return
        self._status_var.set(f"Saved PDF to {path}")


class _StatuteWindow:
    """
    Reader for a statute or regulation section — U.S. Code from the
    Office of the Law Revision Counsel (uscode.house.gov) or C.F.R. from
    the eCFR (www.ecfr.gov).  Both sources are parsed into the same
    (kind, indent, text) stream, so one window serves both.

    Formatting follows the statutory hierarchy: the section heading and
    subdivision headings are bold, inline enumerators ("(a)", "(1)(A)")
    are bold, and each nesting level is indented with a hanging indent so
    wrapped lines stay aligned under their text.  When the citation that
    opened the window pin-cites a subdivision ("§ 922(g)(1)"), the view
    scrolls there and flashes it.  Source credit is shown small below the
    text; long editorial/statutory notes sit behind a toggle.
    """

    def __init__(
        self, parent: tk.Misc, doc, highlight: tuple = (), *, app=None,
    ) -> None:
        self._app = app
        self._doc = doc
        self._highlight = tuple(highlight)
        self._has_notes = any(k.startswith("note") for k, _i, _t in doc.paras)
        self._neighbors: tuple = (None, None)
        self._link_actions: dict[str, tuple[str, str]] = {}
        self._link_n = 0
        self._win = _secondary_view_host(parent, app)
        _ensure_modern_ttk_styles(self._win)
        self._win.title(f"{doc.label} — {doc.source_name}")
        self._win.geometry("760x640")
        self._win.minsize(440, 280)
        self._base_size = _OPINION_FONT_PT
        self._window_menubar = _install_window_menubar(app, self._win)
        self._build_ui()
        self._render()
        self._refresh_neighbors()
        if app is not None and hasattr(app, "register_secondary_window"):
            def reopen(app=app, source=self) -> None:
                _StatuteWindow(
                    app.root, source._doc, source._highlight, app=app
                )
            app.register_secondary_window(
                self, self._win, f"statute:{id(self)}",
                f"{doc.label} — {doc.source_name}", reopen,
            )

    def _build_ui(self) -> None:
        win = self._win
        muted_style = "ModernMuted.TLabel" if _CTK_AVAILABLE else "TLabel"
        entry_style = "Modern.TEntry" if _CTK_AVAILABLE else "TEntry"
        top = _ui_frame(win)
        top.pack(fill="x", padx=12, pady=(12, 0))
        ttk.Label(top, text="Source", style=muted_style).pack(side="left")
        self._src_var = tk.StringVar(value=self._doc.url)
        ttk.Entry(top, textvariable=self._src_var, state="readonly",
                  style=entry_style).pack(
            side="left", fill="x", expand=True, padx=(8, 8)
        )
        _ui_button(
            top, "Open in Browser",
            command=lambda: webbrowser.open(self._doc.url), width=132,
        ).pack(side="right")
        self._next_btn = _ui_button(
            top, "Next § ▶", width=88, command=lambda: self._go_neighbor(1),
        )
        self._next_btn.pack(side="right", padx=(6, 8))
        self._prev_btn = _ui_button(
            top, "◀ Prev §", width=88, command=lambda: self._go_neighbor(0),
        )
        self._prev_btn.pack(side="right")
        for b in (self._next_btn, self._prev_btn):
            try:
                b.configure(state="disabled")
            except tk.TclError:
                pass

        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True, padx=8, pady=4)
        s = self._base_size
        fam = "Georgia"
        self._fonts = {
            "base": tkfont.Font(family=fam, size=s),
            "bold": tkfont.Font(family=fam, size=s, weight="bold"),
            "sechead": tkfont.Font(family=fam, size=s + 2, weight="bold"),
            "small": tkfont.Font(family=fam, size=max(s - 2, 8)),
        }
        txt = tk.Text(frame, wrap="word", font=self._fonts["base"],
                      padx=14, pady=10)
        self._text = txt
        vsb = ttk.Scrollbar(frame, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)

        txt.tag_configure("sechead", font=self._fonts["sechead"],
                          spacing1=4, spacing3=12)
        txt.tag_configure("headline", font=self._fonts["bold"], spacing1=8)
        txt.tag_configure("enum", font=self._fonts["bold"])
        txt.tag_configure("credit", font=self._fonts["small"],
                          foreground="#555555", spacing1=14)
        txt.tag_configure("notehead", font=self._fonts["bold"],
                          foreground="#444444", spacing1=14)
        txt.tag_configure("notebody", font=self._fonts["small"],
                          foreground="#444444")
        for i in range(7):
            margin = 10 + 26 * i
            txt.tag_configure(f"ind{i}", lmargin1=margin,
                              lmargin2=margin + 22, spacing3=6)
        txt.tag_configure("jumpflash", background="#fff2a8")
        txt.tag_configure("citelink", foreground="#1a56b0")
        txt.tag_bind("citelink", "<Enter>",
                     lambda _e: txt.config(cursor="hand2"))
        txt.tag_bind("citelink", "<Leave>",
                     lambda _e: txt.config(cursor=""))
        self._finder = _TextFinder(win, txt, frame)

        btns = _ui_frame(win)
        btns.pack(fill="x", padx=12, pady=(2, 10))
        _ui_button(btns, "A−", width=42,
                   command=lambda: self._zoom(-1)).pack(side="left")
        _ui_button(btns, "A+", width=42,
                   command=lambda: self._zoom(+1)).pack(side="left", padx=(6, 10))
        self._notes_var = tk.BooleanVar(value=False)
        self._notes_btn = _ui_checkbox(
            btns, "Show notes", self._notes_var, self._render,
        )
        self._notes_btn.pack(side="left", padx=(0, 8))
        if not self._has_notes:
            self._notes_btn.configure(state="disabled")
        _ui_button(btns, "Copy + Cite", primary=True, width=110,
                   command=self._copy_cite).pack(side="right", padx=(6, 0))
        _ui_button(btns, "Export RTF…", width=120,
                   command=self._export_rtf).pack(side="right")
        # Status doubles as the provenance note until an action overwrites it
        self._status_var = tk.StringVar(value=self._doc.source_note)
        _install_status_label(btns, self._status_var)
        for seq in ("<Control-plus>", "<Control-equal>", "<Control-KP_Add>"):
            win.bind(seq, lambda _e: self._zoom(+1))
        for seq in ("<Control-minus>", "<Control-KP_Subtract>"):
            win.bind(seq, lambda _e: self._zoom(-1))
        txt.bind(
            "<Control-MouseWheel>",
            lambda e: self._zoom(+1 if e.delta > 0 else -1) or "break",
        )
        txt.bind("<Control-Button-4>", lambda _e: self._zoom(+1) or "break")
        txt.bind("<Control-Button-5>", lambda _e: self._zoom(-1) or "break")
        # Ctrl-C copies with the Bluebook citation appended, pin-cited to
        # the selection's subdivision (the plain default copy is
        # suppressed); the find bar's entry keeps native copy since this
        # is bound to the text widget only.
        for seq in ("<Control-c>", "<Command-c>"):
            try:
                txt.bind(seq, lambda _e: self._copy_cite() or "break")
            except tk.TclError:
                pass  # modifier not supported on this platform

    _ENUM_LEAD_RE = re.compile(r"((?:\((?:\d{1,3}|[a-zA-Z]{1,4})\)\s*)+)")

    def _render(self) -> None:
        txt = self._text
        txt.config(state="normal")
        txt.delete("1.0", "end")
        show_notes = self._notes_var.get()
        path: list[str] = []
        target = list(self._highlight)
        target_pos: Optional[str] = None
        # (position, enumerator path) per enumerated paragraph, for the
        # pin-cite jump and for citing a selection in _copy_cite
        self._anchors: list[tuple[str, tuple]] = []
        for kind, ind, text in self._doc.paras:
            if kind.startswith("note") and not show_notes:
                continue
            text = educate_quotes(text)
            indtag = f"ind{min(ind, 6)}"
            # Track the enumerator path: a paragraph at indent level N
            # replaces the path from depth N down.
            m = self._ENUM_LEAD_RE.match(text) if kind in ("body", "head") \
                else None
            lead = m.group(1) if m else ""
            if lead:
                enums = re.findall(r"\(([^)]+)\)", lead)
                path[ind:] = enums
                self._anchors.append((txt.index("end-1c"), tuple(path)))
                if (target and target_pos is None
                        and path[:len(target)] == target):
                    target_pos = txt.index("end-1c")
            if kind == "sechead":
                txt.insert("end", text + "\n", ("sechead",))
            elif kind == "head":
                # Constitution citations pin a section of an article/amendment
                # ("art. I, § 8"); jump to and flash that "Section N." heading.
                if (self._doc.kind == "const" and target and target_pos is None):
                    mh = re.match(r"Section\s+(\d+)", text)
                    if mh and mh.group(1) == target[0]:
                        target_pos = txt.index("end-1c")
                txt.insert("end", text + "\n", ("headline", indtag))
            elif kind == "body":
                if lead:
                    txt.insert("end", lead.rstrip() + " ",
                               ("enum", indtag))
                    self._insert_refs(text[len(lead):].lstrip(), (indtag,))
                else:
                    self._insert_refs(text, (indtag,))
                txt.insert("end", "\n", (indtag,))
            elif kind == "credit":
                # The source-credit parenthetical carries the Statutes at Large
                # (and Pub. L.) cites — link them like the notes do.
                self._insert_refs(text, ("credit",))
                txt.insert("end", "\n", ("credit",))
            elif kind == "note-head":
                txt.insert("end", text + "\n", ("notehead",))
            elif kind == "note-body":
                self._insert_refs(text, ("notebody", indtag))
                txt.insert("end", "\n", ("notebody", indtag))
        txt.config(state="disabled")
        self._finder.refresh()
        if target_pos:
            txt.see(target_pos)
            txt.tag_add("jumpflash", f"{target_pos} linestart",
                        f"{target_pos} lineend")
            self._win.after(
                1800,
                lambda: txt.tag_remove("jumpflash", "1.0", "end"),
            )

    def _insert_refs(self, text: str, tags: tuple) -> None:
        """Insert paragraph text, linking citations to other U.S. Code /
        C.F.R. provisions — explicit citations plus the document's own
        cross-reference style ("section 102 of title 5"; "§ 1614.106")."""
        refs: list[tuple[int, int, str, str]] = []
        for m in us_code.USC_CITE_RE.finditer(text):
            refs.append((m.start(), m.end(), "usc", us_code.cite_spec(m)))
        for m in ecfr.CFR_CITE_RE.finditer(text):
            refs.append((m.start(), m.end(), "cfr", ecfr.cite_spec(m)))
        for m in fed_rules.RULE_CITE_RE.finditer(text):
            refs.append((m.start(), m.end(), "rule", fed_rules.cite_spec(m)))
        for m in constitution.CONST_CITE_RE.finditer(text):
            refs.append((m.start(), m.end(), "const", constitution.cite_spec(m)))
        for c in state_statutes.iter_cites(text):
            kind, value = state_statutes.action_for(c)
            refs.append((c.start, c.end, kind, value))
        for m in statutes_at_large.STAT_CITE_RE.finditer(text):
            url = statutes_at_large.url_for(m)
            if url:  # Statutes at Large → free GovInfo scan (in-app PDF viewer)
                refs.append((m.start(), m.end(), "statpdf", url))
        if self._doc.kind == "usc":
            for m in _USC_XREF_RE.finditer(text):
                title = m.group(3) or self._doc.title
                section = (m.group(1).replace("–", "-").replace("—", "-"))
                subs = re.findall(r"\(([^)]+)\)", m.group(2) or "")
                refs.append((m.start(), m.end(), "usc",
                             f"{title}:{section}:{','.join(subs)}"))
        elif self._doc.kind == "cfr":
            for m in _CFR_SECREF_RE.finditer(text):
                subs = re.findall(r"\(([^)]+)\)", m.group(2) or "")
                refs.append((m.start(), m.end(), "cfr",
                             f"{self._doc.title}:{m.group(1)}:"
                             f"{','.join(subs)}"))
        elif self._doc.kind == "rule":
            # A bare "Rule 801" on a federal-rules page means a rule in the
            # same set (e.g. FRE 801 → fre:801).  Qualified forms ("Fed. R.
            # Evid. 801") are caught above and win the overlap.
            for m in fed_rules.BARE_RULE_RE.finditer(text):
                refs.append((m.start(), m.end(), "rule",
                             fed_rules.bare_rule_spec(m, self._doc.set_key)))
        refs.sort(key=lambda r: (r[0], -r[1]))
        txt = self._text
        pos = 0
        for start, end, kind, spec in refs:
            if start < pos:
                continue  # overlapping match — first/longest wins
            if start > pos:
                txt.insert("end", text[pos:start], tags)
            ltags = tags + ("citelink", self._new_link((kind, spec)))
            txt.insert("end", text[start:end], ltags)
            pos = end
        if pos < len(text):
            txt.insert("end", text[pos:], tags)

    def _new_link(self, action: tuple[str, str]) -> str:
        self._link_n += 1
        tag = f"lnk{self._link_n}"
        self._link_actions[tag] = action
        self._text.tag_bind(
            tag, "<Button-1>", lambda _e, t=tag: self._follow_link(t)
        )
        return tag

    def _follow_link(self, tag: str) -> None:
        action = self._link_actions.get(tag)
        if not action:
            return
        kind, value = action
        if kind == "browse":
            # Cross-reference to a source we don't render in-app (e.g. a state
            # statute) — open it in the user's browser.
            webbrowser.open(value)
            self._status_var.set("Opened in your browser.")
            return
        if kind == "statpdf":
            _open_statute_pdf(
                self._win, value, self._status_var.set, app=self._app
            )
            return
        _fetch_statute_window(
            self._win, kind, value, self._status_var.set, app=self._app
        )

    # ------------------------------------------------------------------
    # Previous/next provision
    # ------------------------------------------------------------------

    def _refresh_neighbors(self) -> None:
        """Resolve the adjacent sections in the background (the C.F.R.
        side may fetch the title's structure tree) and grey the buttons
        accordingly."""
        self._prev_btn.configure(state="disabled")
        self._next_btn.configure(state="disabled")
        doc = self._doc

        def run() -> None:
            nb = doc.neighbors()

            def apply() -> None:
                if self._doc is not doc:
                    return  # user already navigated elsewhere
                self._neighbors = nb
                try:
                    self._prev_btn.configure(
                        state="normal" if nb[0] else "disabled")
                    self._next_btn.configure(
                        state="normal" if nb[1] else "disabled")
                except tk.TclError:
                    pass  # window closed while neighbors were resolving

            try:
                self._win.after(0, apply)
            except tk.TclError:
                pass

        threading.Thread(target=run, daemon=True).start()

    def _go_neighbor(self, which: int) -> None:
        target = self._neighbors[which]
        if not target:
            return
        mod = _STATUTE_SOURCES[self._doc.kind]
        self._prev_btn.configure(state="disabled")
        self._next_btn.configure(state="disabled")
        self._status_var.set(
            f"Fetching {'previous' if which == 0 else 'next'} section…"
        )

        def run() -> None:
            try:
                doc = mod.load_section(*target)
            except Exception as exc:
                msg = str(exc)

                def fail() -> None:
                    self._status_var.set(msg)
                    self._refresh_neighbors()

                try:
                    self._win.after(0, fail)
                except tk.TclError:
                    pass
                return
            try:
                self._win.after(0, self._load_doc, doc)
            except tk.TclError:
                pass

        threading.Thread(target=run, daemon=True).start()

    def _load_doc(self, doc, highlight: tuple = ()) -> None:
        """Show another section in this same window (prev/next nav)."""
        self._doc = doc
        self._highlight = tuple(highlight)
        self._has_notes = any(k.startswith("note") for k, _i, _t in doc.paras)
        self._notes_btn.configure(
            state="normal" if self._has_notes else "disabled")
        self._win.title(f"{doc.label} — {doc.source_name}")
        self._src_var.set(doc.url)
        self._status_var.set(doc.source_note)
        self._render()
        self._text.yview_moveto(0.0)
        self._refresh_neighbors()

    def _pin_for(self, index: str) -> tuple:
        """Enumerator path of the paragraph containing a text index, for
        a pinpoint citation of the selection."""
        txt = self._text
        best: tuple = ()
        for pos, path in self._anchors:
            if txt.compare(pos, "<=", index):
                best = path
            else:
                break
        return best

    def _copy_cite(self) -> None:
        """Copy the selection (or all) with formatting, appending the
        Bluebook citation — pin-cited to the selection's subdivision."""
        txt = self._text
        try:
            start, end = txt.index("sel.first"), txt.index("sel.last")
            selected = True
        except tk.TclError:
            start, end = "1.0", "end-1c"
            selected = False
        subs = self._pin_for(start) if selected else ()
        cite = self._doc.bluebook_cite(subs) + "."
        body = _dump_statute_rtf(txt, start, end)
        rtf = _rtf_document(body + "\\pard\\sa120 " + _rtf_escape(cite)
                            + "\\par\n")
        plain = txt.get(start, end).rstrip() + "\n\n" + cite + "\n"
        how = _copy_rich_clipboard(self._win, rtf, plain)
        what = "selection" if selected else "full text"
        self._status_var.set(f"Copied {what} as {how}; citation appended.")

    def _export_rtf(self) -> None:
        """Export the section as RTF with a heading block: the citation,
        then provenance, then the formatted text."""
        head = (
            "\\pard\\qc\\sa60{\\b\\fs30 "
            + _rtf_escape(self._doc.bluebook_cite()) + "}\\par\n"
            "\\pard\\qc\\sa240{\\fs18 "
            + _rtf_escape(f"{self._doc.source_note} — {self._doc.url}")
            + "}\\par\n"
        )
        body = _dump_statute_rtf(self._text, "1.0", "end-1c")
        rtf = _rtf_document(head + body)
        default = self._doc.label.replace("§", "Sec.")
        path = filedialog.asksaveasfilename(
            defaultextension=".rtf",
            filetypes=[("Rich Text Format", "*.rtf"), ("All files", "*.*")],
            initialfile=f"{default}.rtf",
            title="Export Statute as RTF",
            parent=self._win,
        )
        if not path:
            return
        with open(path, "w", encoding="ascii", errors="replace") as f:
            f.write(rtf)
        self._status_var.set(f"Exported RTF: {path}")
        if messagebox.askyesno(
            "Export Complete", f"RTF saved to:\n{path}\n\nOpen it now?",
            parent=self._win,
        ):
            CourtListenerGUI._open_file(path)

    def _zoom(self, delta: int) -> None:
        global _OPINION_FONT_PT
        new = max(_OPINION_FONT_MIN,
                  min(_OPINION_FONT_MAX, self._base_size + delta))
        if new == self._base_size:
            return
        self._base_size = new
        _OPINION_FONT_PT = new
        _save_opinion_font_pt(new)
        self._fonts["base"].configure(size=new)
        self._fonts["bold"].configure(size=new)
        self._fonts["sechead"].configure(size=new + 2)
        self._fonts["small"].configure(size=max(new - 2, 8))


class _CitingOpinionsWindow:
    """
    Popup window listing all opinions that cite the selected case,
    sorted by depth of treatment (number of times cited within the
    citing document, descending).

    Data strategy (single stage)
    -----------------------------
    1. Resolve the cited opinion's numeric ID from its cluster
       (``/api/rest/v4/opinions/?cluster=<id>``).
    2. Fetch citing opinions sorted by depth from the citations endpoint
       (``/api/rest/v4/citations/?cited_opinion=<id>&ordering=-depth``).
    3. In parallel (thread pool), resolve each citing opinion URL →
       opinion record → cluster ID.
    4. In parallel, fetch each cluster's case name, date, and citation.
    5. Display the merged results immediately with depth populated.

    Falls back to a plain ``cites:(cluster_id)`` search (depth shown as
    "–") when step 1 fails (opinion not in citations database).
    """

    _COLS = ("case_name", "court", "date_filed", "citation", "depth")
    _COL_LABELS = {
        "case_name": "Case Name",
        "court":     "Court",
        "date_filed": "Date Filed",
        "citation":  "Citation",
        "depth":     "Depth",
    }

    def __init__(
        self,
        parent: tk.Tk | tk.Toplevel,
        app: "CourtListenerGUI",
        cited_item: dict,
    ) -> None:
        self._app = app
        self._cited_item = cited_item
        self._cluster_id = cited_item.get("cluster_id") or cited_item.get("id")
        # Cached after first load so pagination doesn't re-fetch it
        self._cited_op_id: Optional[int] = None

        # Pagination: history[i] is the citations-endpoint next-URL that
        # leads TO page i+1 (None = page 1, string URL = page 2+).
        self._cursor_history: list[Optional[str]] = [None]
        self._history_idx: int = 0
        self._next_cursor: Optional[str] = None
        self._total_count: int = 0
        self._page_results: list[dict] = []

        # Background fetch cancellation: replaced each time _load_page() is
        # called so any in-flight background thread knows to stop.
        self._bg_stop = threading.Event()

        case_name = re.sub(
            r"<[^>]+>",
            "",
            cited_item.get("caseName") or cited_item.get("case_name") or "?",
        ).strip()

        self._win = _secondary_view_host(parent, app)
        _ensure_modern_ttk_styles(self._win)
        self._win.title(f"Citing: {case_name}")
        self._win.geometry("950x480")
        self._win.minsize(700, 300)
        self._win.protocol("WM_DELETE_WINDOW", self._on_close)
        self._win.bind("<Destroy>", self._on_destroy, add="+")
        self._window_menubar = _install_window_menubar(app, self._win)

        self._build_ui(case_name)
        if hasattr(app, "register_secondary_window"):
            def reopen(app=app, item=dict(cited_item)) -> None:
                _CitingOpinionsWindow(app.root, app, item)
            app.register_secondary_window(
                self, self._win, f"citing:{id(self)}",
                f"Citing: {case_name}", reopen,
            )
        self._load_page()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self, case_name: str) -> None:
        muted_style = "ModernMuted.TLabel" if _CTK_AVAILABLE else "TLabel"
        tree_style = "Modern.Treeview" if _CTK_AVAILABLE else "Treeview"
        sb_style = "Modern.Vertical.TScrollbar" if _CTK_AVAILABLE else "Vertical.TScrollbar"
        pad = 8 if _CTK_AVAILABLE else 0

        # ── status bar (top) ──────────────────────────────────────────
        top = _ui_frame(self._win)
        top.pack(fill="x", padx=12, pady=(12, 0))
        _ui_label(top, f"Opinions citing:  {case_name}", size=13,
                  weight="bold", anchor="w").pack(side="left")
        self._status_var = tk.StringVar(value="Loading…")
        _ui_label(top, muted=True, textvariable=self._status_var).pack(side="right")

        # ── treeview ─────────────────────────────────────────────────
        tree_frame = _ui_frame(self._win, card=True)
        tree_frame.pack(fill="both", expand=True, padx=12, pady=8)

        self._tree = ttk.Treeview(
            tree_frame,
            columns=self._COLS,
            show="headings",
            selectmode="browse",
            style=tree_style,
        )
        for col, label in self._COL_LABELS.items():
            self._tree.heading(col, text=label)
        self._tree.column("case_name",  width=320, minwidth=160)
        self._tree.column("court",      width=80,  minwidth=50,  anchor="center")
        self._tree.column("date_filed", width=85,  minwidth=70,  anchor="center")
        self._tree.column("citation",   width=150, minwidth=90)
        self._tree.column("depth",      width=55,  minwidth=40,  anchor="center")

        vsb = ttk.Scrollbar(tree_frame, orient="vertical",
                            command=self._tree.yview, style=sb_style)
        self._tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y", pady=pad, padx=(0, pad))
        self._tree.pack(side="left", fill="both", expand=True, padx=(pad, 0),
                        pady=pad)

        self._tree.bind("<Double-1>", lambda _e: self._download_selected())

        # ── bottom button bar ────────────────────────────────────────
        bot = _ui_frame(self._win)
        bot.pack(fill="x", padx=12, pady=(0, 12))

        self._prev_btn = _ui_button(bot, "◀  Prev", command=self._go_prev,
                                    width=84)
        self._prev_btn.pack(side="left", padx=(0, 6))

        self._page_var = tk.StringVar(value="Page 1")
        _ui_label(bot, muted=True, textvariable=self._page_var).pack(side="left")

        self._next_btn = _ui_button(bot, "Next  ▶", command=self._go_next,
                                    width=84)
        self._next_btn.pack(side="left", padx=(6, 20))

        self._dl_btn = _ui_button(bot, "Download PDF", command=self._download_selected,
                                  primary=True, width=124)
        self._dl_btn.pack(side="right", padx=(6, 0))

        self._scholar_btn = _ui_button(bot, "Google Scholar", command=self._open_scholar,
                                       width=124)
        self._scholar_btn.pack(side="right")
        for b in (self._prev_btn, self._next_btn, self._dl_btn, self._scholar_btn):
            try:
                b.configure(state="disabled")
            except tk.TclError:
                pass

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def _page_num(self) -> int:
        return self._history_idx + 1

    def _go_next(self) -> None:
        next_cur = self._next_cursor
        if not next_cur:
            return
        # If we're at the end of history, append new cursor
        if self._history_idx + 1 >= len(self._cursor_history):
            self._cursor_history.append(next_cur)
        self._history_idx += 1
        self._load_page()

    def _go_prev(self) -> None:
        if self._history_idx <= 0:
            return
        self._history_idx -= 1
        self._load_page()

    def _current_cursor(self) -> Optional[str]:
        return self._cursor_history[self._history_idx]

    # ------------------------------------------------------------------
    # Data loading  (Phase 1 – search results)
    # ------------------------------------------------------------------

    def _on_close(self) -> None:
        self._bg_stop.set()
        self._win.destroy()

    def _on_destroy(self, event) -> None:
        if event.widget is self._win:
            self._bg_stop.set()

    def _cancel_bg_fetch(self) -> None:
        """Signal any running background fetch to stop and arm a fresh event."""
        self._bg_stop.set()
        self._bg_stop = threading.Event()

    def _set_buttons_loading(self) -> None:
        self._prev_btn.configure(state="disabled")
        self._next_btn.configure(state="disabled")
        self._dl_btn.configure(state="disabled")
        self._scholar_btn.configure(state="disabled")

    # ------------------------------------------------------------------
    # Data loading – single stage (citations endpoint → parallel cluster
    # fetches), falls back to plain search when depth data unavailable
    # ------------------------------------------------------------------

    def _load_page(self) -> None:
        self._set_buttons_loading()
        self._status_var.set("Loading…")
        self._cancel_bg_fetch()
        bg_stop = self._bg_stop   # capture this run's stop-event for closures
        cluster_id = self._cluster_id
        # Re-use the cited opinion ID resolved on page 1
        known_op_id = self._cited_op_id
        client = self._app._get_client()
        if client is None:
            return

        def fetch_case(entry: dict) -> Optional[dict]:
            if bg_stop.is_set():
                return None
            op_url = str(entry.get("citing_opinion", ""))
            citing_op_id = _extract_opinion_id(op_url)
            if citing_op_id is None:
                return None
            try:
                opinion = client.get_opinion(citing_op_id, fields="cluster")
                cid = _extract_cluster_id(str(opinion.get("cluster", "")))
                if cid is None:
                    return None
                cluster_rec = client.get_cluster(
                    int(cid), fields="case_name,citations,date_filed,docket"
                )
                cite_strs = _cluster_citations_to_strings(
                    cluster_rec.get("citations", [])
                )
                court_id = ""
                docket_url = str(cluster_rec.get("docket", ""))
                if docket_url:
                    docket_rec = client._get_url(docket_url, {"fields": "court"})
                    court_id = _extract_court_id(str(docket_rec.get("court", "")))
                return {
                    "caseName":   cluster_rec.get("case_name", ""),
                    "case_name":  cluster_rec.get("case_name", ""),
                    "citation":   cite_strs,
                    "dateFiled":  cluster_rec.get("date_filed", ""),
                    "date_filed": cluster_rec.get("date_filed", ""),
                    "cluster_id": cid,
                    "court":    court_id,
                    "court_id": court_id,
                    "_depth": entry.get("depth", 0),
                }
            except Exception:
                return None

        _FIRST_PAGE = 20  # number of cases to detail-fetch before showing results

        def start_bg_details(remaining: list[dict], loaded_so_far: int, total: int) -> None:
            """Resolve case details for entries beyond the first page in the background."""
            def run_bg() -> None:
                loaded = loaded_so_far
                # Process in batches matching the API page size so UI updates
                # progressively rather than all at once at the very end.
                batch_size = _FIRST_PAGE
                for start in range(0, len(remaining), batch_size):
                    if bg_stop.is_set():
                        return
                    chunk = remaining[start:start + batch_size]
                    with ThreadPoolExecutor(max_workers=8) as pool:
                        raw = list(pool.map(fetch_case, chunk))
                    if bg_stop.is_set():
                        return
                    batch = [r for r in raw if r is not None]
                    loaded += len(batch)
                    is_final = (start + batch_size) >= len(remaining)
                    self._win.after(
                        0, self._append_bg_results, batch, loaded, total, is_final
                    )
            threading.Thread(target=run_bg, daemon=True).start()

        def run() -> None:
            try:
                # ── Step 1: resolve cited opinion ID (once only) ──────
                op_id = known_op_id
                if op_id is None:
                    self._win.after(0, self._status_var.set, "Resolving opinion ID…")
                    cluster_rec = client.get_cluster(
                        int(cluster_id), fields="sub_opinions"
                    )
                    sub_ops = cluster_rec.get("sub_opinions") or []
                    ids = [_extract_opinion_id(u) for u in sub_ops]
                    ids = [i for i in ids if i is not None]
                    op_id = ids[0] if ids else None
                    self._cited_op_id = op_id

                if op_id is None:
                    # No opinion ID found; fall back to plain search
                    self._win.after(0, self._status_var.set, "Fetching (search fallback)…")
                    data = client.search(
                        f"cites:({cluster_id})", type="o", page_size=20
                    )
                    self._win.after(0, self._on_fallback_results, data)
                    return

                if bg_stop.is_set():
                    return

                # ── Step 2: fetch ALL pages to get the full depth-sorted list ──
                self._win.after(0, self._status_var.set, "Fetching citing opinions…")
                all_entries: list[dict] = []
                next_api_url: Optional[str] = None
                while True:
                    if next_api_url:
                        page_data = client._get_url(next_api_url)
                    else:
                        page_data = client.list_citing_opinions(cited_opinion_id=op_id)
                    all_entries.extend(page_data.get("results", []))
                    next_api_url = page_data.get("next")
                    self._win.after(
                        0, self._status_var.set,
                        f"Fetched {len(all_entries)} citing opinions…",
                    )
                    if not next_api_url:
                        break

                all_entries.sort(key=lambda e: e.get("depth", 0), reverse=True)
                total_count = len(all_entries)

                if bg_stop.is_set():
                    return

                if not all_entries:
                    self._win.after(0, self._on_page_ready, [], 0, None)
                    return

                # ── Step 3: resolve case details for the top N entries ────────
                first_page = all_entries[:_FIRST_PAGE]
                rest = all_entries[_FIRST_PAGE:]

                self._win.after(
                    0, self._status_var.set,
                    f"Fetching details for top {len(first_page)} cases…",
                )
                with ThreadPoolExecutor(max_workers=8) as pool:
                    raw = list(pool.map(fetch_case, first_page))

                if bg_stop.is_set():
                    return

                results = [r for r in raw if r is not None]
                results.sort(key=lambda r: r.get("_depth", 0), reverse=True)

                if rest:
                    # Show first batch immediately; resolve the rest in background
                    self._win.after(
                        0, self._on_first_batch_ready, results, total_count
                    )
                    start_bg_details(rest, len(results), total_count)
                else:
                    # Everything fit in the first batch
                    self._win.after(0, self._on_page_ready, results, total_count, None)

            except Exception as exc:
                import traceback; traceback.print_exc()
                self._win.after(0, self._status_var.set, f"Error: {exc}")
                self._win.after(0, self._restore_buttons)

        threading.Thread(target=run, daemon=True).start()

    def _on_page_ready(
        self,
        results: list[dict],
        total: int,
        next_url: Optional[str],
    ) -> None:
        """Populate treeview from citations-endpoint results (depth filled)."""
        self._page_results = results
        self._total_count = total
        self._next_cursor = next_url

        self._tree.delete(*self._tree.get_children())
        for i, item in enumerate(results):
            depth = item.get("_depth", 0)
            row = self._format_row(item, depth=str(depth))
            self._tree.insert("", "end", iid=str(i), values=row)

        self._update_status_and_nav()

    def _on_first_batch_ready(self, results: list[dict], total: int) -> None:
        """Display the first page of results while more are loading in the background."""
        self._page_results = list(results)
        self._total_count = total
        self._next_cursor = None

        self._tree.delete(*self._tree.get_children())
        for i, item in enumerate(results):
            depth = item.get("_depth", 0)
            row = self._format_row(item, depth=str(depth))
            self._tree.insert("", "end", iid=str(i), values=row)

        shown = len(results)
        self._page_var.set(f"Page {self._page_num()}")
        self._status_var.set(
            f"Showing {shown:,} of {total:,} citing opinions · Loading more…"
        )
        self._prev_btn.configure(state="normal" if self._history_idx > 0 else "disabled")
        self._next_btn.configure(state="disabled")
        has = bool(results)
        self._dl_btn.configure(state="normal" if has else "disabled")
        self._scholar_btn.configure(state="normal" if has else "disabled")

    def _append_bg_results(
        self, batch: list[dict], loaded: int, total: int, final: bool
    ) -> None:
        """Append a background-fetched batch of results to the treeview."""
        offset = len(self._page_results)
        self._page_results.extend(batch)
        for i, item in enumerate(batch):
            depth = item.get("_depth", 0)
            row = self._format_row(item, depth=str(depth))
            self._tree.insert("", "end", iid=str(offset + i), values=row)

        if final:
            self._status_var.set(
                f"Page {self._page_num()} · {loaded:,} of {total:,} citing opinions"
                if total else f"Page {self._page_num()} · {loaded:,} results"
            )
            has = bool(self._page_results)
            self._dl_btn.configure(state="normal" if has else "disabled")
            self._scholar_btn.configure(state="normal" if has else "disabled")
        else:
            self._status_var.set(
                f"Showing {loaded:,} of {total:,} citing opinions · Loading more…"
            )

    def _on_fallback_results(self, data: dict) -> None:
        """Populate treeview from plain search API results (no depth)."""
        results = data.get("results", [])
        self._total_count = data.get("count", len(results))
        self._next_cursor = data.get("next")

        for item in results:
            raw = item.get("citation")
            if isinstance(raw, list):
                item["citation"] = [re.sub(r"<[^>]+>", "", c).strip() for c in raw]
            elif raw:
                item["citation"] = re.sub(r"<[^>]+>", "", str(raw)).strip()

        self._page_results = results
        self._tree.delete(*self._tree.get_children())
        for i, item in enumerate(results):
            row = self._format_row(item, depth="–")
            self._tree.insert("", "end", iid=str(i), values=row)

        self._update_status_and_nav()

    def _update_status_and_nav(self) -> None:
        page = self._page_num()
        self._page_var.set(f"Page {page}")
        shown = len(self._page_results)
        total = self._total_count
        self._status_var.set(
            f"Page {page} · {shown} of {total:,} citing opinions"
            if total else f"Page {page} · {shown} results"
        )
        self._prev_btn.configure(state="normal" if self._history_idx > 0 else "disabled")
        self._next_btn.configure(state="normal" if self._next_cursor else "disabled")
        has = bool(self._page_results)
        self._dl_btn.configure(state="normal" if has else "disabled")
        self._scholar_btn.configure(state="normal" if has else "disabled")

    def _format_row(self, item: dict, depth: str = "") -> tuple:
        case_name = re.sub(
            r"<[^>]+>",
            "",
            item.get("caseName") or item.get("case_name") or "(unknown)",
        ).strip()
        court = item.get("court") or item.get("court_id") or ""
        date_filed = item.get("dateFiled") or item.get("date_filed") or ""
        cite_str = _pick_citation(item.get("citation", []))
        return (case_name, court, date_filed, cite_str, depth)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _get_selected(self) -> Optional[dict]:
        sel = self._tree.selection()
        if not sel:
            return None
        idx = int(sel[0])
        if 0 <= idx < len(self._page_results):
            return self._page_results[idx]
        return None

    def _download_selected(self) -> None:
        item = self._get_selected()
        if not item:
            messagebox.showinfo("No Selection", "Please select a case first.", parent=self._win)
            return

        client = self._app._get_client()
        if client is None:
            return

        safe_name = _build_default_filename(item)
        save_path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
            initialfile=f"{safe_name}.pdf",
            title="Save Opinion PDF",
            parent=self._win,
        )
        if not save_path:
            return

        self._set_buttons_loading()
        self._status_var.set("Resolving PDF URL…")

        def run() -> None:
            try:
                pdf_url = self._app._resolve_pdf_url(client, item)
                if not pdf_url:
                    cluster_id = item.get("cluster_id") or item.get("id")
                    if cluster_id:
                        self._win.after(0, self._status_var.set, "No PDF – fetching text…")
                        text = _assemble_case_text(client, item)
                        if text.strip():
                            txt_path = os.path.splitext(save_path)[0] + ".txt"
                            with open(txt_path, "w", encoding="utf-8") as f:
                                f.write(text)
                            self._win.after(0, self._on_dl_done, txt_path, True)
                            return
                    self._win.after(0, self._status_var.set,
                                    "No downloadable PDF or text found.")
                    self._win.after(0, self._restore_buttons)
                    return

                self._win.after(0, self._status_var.set, f"Downloading… {pdf_url}")
                fetched = _fetch_pdf_bytes(pdf_url, client=client, timeout=60)
                if fetched is None:
                    raise RuntimeError("The source returned something that isn't a PDF.")
                data, pdf_url = fetched
                with open(save_path, "wb") as f:
                    f.write(data)
                self._win.after(0, self._on_dl_done, save_path, False)
            except Exception as exc:
                self._win.after(0, self._status_var.set, f"Download failed: {exc}")
                self._win.after(0, self._restore_buttons)

        threading.Thread(target=run, daemon=True).start()

    def _on_dl_done(self, path: str, is_text: bool) -> None:
        self._restore_buttons()
        self._status_var.set(f"Saved: {path}")
        label = "Text Saved" if is_text else "Download Complete"
        msg = (
            f"Opinion text saved to:\n{path}\n\nOpen it now?"
            if is_text else
            f"PDF saved to:\n{path}\n\nOpen it now?"
        )
        if messagebox.askyesno(label, msg, parent=self._win):
            CourtListenerGUI._open_file(path)

    def _restore_buttons(self) -> None:
        has = bool(self._page_results)
        self._dl_btn.configure(state="normal" if has else "disabled")
        self._scholar_btn.configure(state="normal" if has else "disabled")
        self._prev_btn.configure(state="normal" if self._history_idx > 0 else "disabled")
        self._next_btn.configure(state="normal" if self._next_cursor else "disabled")

    # ------------------------------------------------------------------
    # Google Scholar  (reuses the main app's fetcher + text window)
    # ------------------------------------------------------------------

    def _open_scholar(self) -> None:
        item = self._get_selected()
        if not item:
            messagebox.showinfo("No Selection", "Please select a case first.",
                                parent=self._win)
            return

        fetcher = self._app._get_scholar()
        if fetcher is None:
            return
        client = self._app._get_client()

        self._scholar_btn.configure(state="disabled")
        self._status_var.set("Searching Google Scholar…")

        def status_cb(msg: str) -> None:
            try:
                self._win.after(0, self._status_var.set, msg)
            except tk.TclError:
                pass

        def run() -> None:
            try:
                result, cl_text, note = _find_scholar_for_item(
                    client, fetcher, item, status_cb
                )
            except Exception as exc:
                import traceback
                traceback.print_exc()
                result, cl_text, note = None, None, str(exc)
            try:
                self._win.after(0, self._on_scholar_done, result, item, cl_text, note)
            except tk.TclError:
                pass

        threading.Thread(target=run, daemon=True).start()

    def _on_scholar_done(
        self,
        result: Optional[tuple[str, str]],
        item: Optional[dict] = None,
        cl_text: Optional[str] = None,
        note: str = "",
    ) -> None:
        self._restore_buttons()
        if result is None:
            self._status_var.set("Google Scholar text unavailable.")
            messagebox.showwarning(
                "Scholar Text Unavailable",
                "Could not find a Google Scholar opinion matching this case.\n\n"
                + (f"({note})" if note else ""),
                parent=self._win,
            )
            return
        url, html = result
        self._status_var.set(
            f"Scholar text loaded — {note}" if note else f"Scholar text loaded from {url}"
        )
        _ScholarTextWindow(
            self._win, self._app, url, html, item=item, cl_text=cl_text, note=note
        )


def main() -> None:
    root = tk.Tk()

    # Run every cyclic-GC collection on the main thread.  Automatic GC fires
    # on whichever thread crosses the allocation threshold, and tkinter
    # finalizers (PhotoImage, Variable, Font, …) call into Tcl — which is only
    # safe on the main thread; a worker-thread collection can therefore
    # deadlock or abort the process (see the Tk-image lifecycle note above
    # _PdfPane).  Reference counting still frees non-cyclic objects instantly;
    # only cycle collection is deferred to this timer.
    gc.disable()

    def _gc_tick() -> None:
        gc.collect()
        _flush_tk_image_graveyard()
        root.after(5000, _gc_tick)

    root.after(5000, _gc_tick)

    app = CourtListenerGUI(root)
    eng_rep.warm()  # load the English Reports index in the background

    # Run in the background by default: rather than greeting the user with the
    # full search window, GetCases starts hidden and waits.  Ctrl+Space opens
    # the quick-search popup; 's' + Enter opens the full window; 'q' + Enter
    # quits.  When there's no terminal to drive it, fall back to showing the
    # window so the app stays discoverable.
    if _stdin_is_tty():
        root.withdraw()
        app._root_hidden = True
        app._print_background_help()

        # A background thread watches stdin so the user can open the window
        # ('s') or quit ('q') even while it is hidden.
        def _watch_stdin() -> None:
            try:
                for line in sys.stdin:
                    cmd = line.strip().lower()
                    if cmd == "q":
                        try:
                            root.after(0, root.destroy)
                        except Exception:
                            pass
                        return
                    if cmd == "s":
                        try:
                            root.after(0, app._show_main_window)
                        except Exception:
                            pass
            except Exception:
                pass

        threading.Thread(target=_watch_stdin, daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    main()

