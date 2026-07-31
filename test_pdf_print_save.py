"""Printing a PDF to a chosen printer, and saving what would be printed.

Print used to open the document in the system's PDF viewer and leave the
reader to press Ctrl/Cmd-P there.  It now asks for a printer up front: macOS
is asked for its own print dialog, and everywhere else the machine's printers
are listed in a small dialog of our own, which also opens the chosen printer's
*own* settings — where duplex, paper and quality live — and offers
double-sided directly wherever we can honour it (CUPS).  If none of that can
be done the document opens in the viewer, exactly as it used to.

Save now writes the same file: the pages as the viewer renders them — cropped,
re-centred, a redacted case.law scan whitened and its running head
re-lettered — rather than the document as it was fetched.

Lifted out of ``courtlistener_gui`` with ``ast`` (importing it needs tkinter,
absent on a headless run) and driven against stubs.
"""

import ast
import pathlib
import re
import sys
import tempfile
import typing
import unittest
from unittest import mock


SRC = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
TREE = ast.parse(SRC)


class _Tk:
    TclError = Exception
    Misc = Menu = Frame = Toplevel = object


def _load_functions(names, extra=None) -> dict:
    found = {n.name: ast.get_source_segment(SRC, n) for n in TREE.body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"module-level functions not found: {missing}")
    ns = {"tk": _Tk, "re": re, "sys": sys, "os": _FakeOS,
          "subprocess": _FakeSubprocess, "shutil": _FakeShutil,
          "webbrowser": _FakeBrowser, "json": __import__("json"),
          "Optional": typing.Optional}
    ns.update(extra or {})
    for name in names:
        exec(found[name], ns)
    return ns


def _module_value(name: str):
    """A module-level constant, evaluated from the source."""
    for node in TREE.body:
        targets = (
            node.targets if isinstance(node, ast.Assign)
            else [node.target] if isinstance(node, ast.AnnAssign) else []
        )
        if any(isinstance(t, ast.Name) and t.id == name for t in targets):
            return eval(ast.get_source_segment(SRC, node.value),  # noqa: S307
                        {"frozenset": frozenset})
    raise AssertionError(f"module-level constant not found: {name}")


def _source_of(cls: str, name: str) -> str:
    body = next(n.body for n in TREE.body
                if isinstance(n, ast.ClassDef) and n.name == cls)
    for node in body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(SRC, node)
    raise AssertionError(f"{cls} has no {name}")


# ---------------------------------------------------------------------------
# A fake operating system
# ---------------------------------------------------------------------------


class _Done:
    def __init__(self, stdout="", returncode=0):
        self.stdout, self.returncode = stdout, returncode
        self.stderr = ""


class _FakeSubprocess:
    """Records what was run and answers from a scripted table.

    ``answer`` is the one hook a test overrides — replacing ``run`` itself
    would leave the class holding the replacement, since restoring it from
    ``__dict__`` restores whatever is there *now*."""

    runs: list = []
    popens: list = []
    answer = None            # callable(cmd) -> _Done | None
    fail: set = set()        # commands whose Popen raises

    @classmethod
    def reset(cls):
        cls.runs, cls.popens, cls.fail = [], [], set()
        cls.answer = None

    @classmethod
    def run(cls, cmd, **kw):
        cls.runs.append(list(cmd))
        if cls.answer is not None:
            done = cls.answer(list(cmd))
            if done is not None:
                return done
        return _Done()

    @classmethod
    def Popen(cls, cmd, **kw):  # noqa: N802 - mirrors subprocess
        if cmd and cmd[0] in cls.fail:
            raise OSError("no such program")
        cls.popens.append(list(cmd))
        return mock.Mock()

    @classmethod
    def answering(cls, table: dict):
        """Answer any command whose first word (or any word) is a key."""
        def reply(cmd):
            for key, done in table.items():
                if cmd and (cmd[0] == key or key in cmd):
                    return done
            return None
        cls.answer = reply


class _FakeOS:
    startfiles: list = []
    fail_startfile = False
    path = __import__("os").path

    @classmethod
    def startfile(cls, path, operation=None, arguments=None):
        if cls.fail_startfile:
            raise OSError("no handler")
        cls.startfiles.append((path, operation, arguments))


class _FakeShutil:
    present: set = set()

    @classmethod
    def which(cls, name):
        return f"/usr/bin/{name}" if name in cls.present else None


class _FakeBrowser:
    opened: list = []

    @classmethod
    def open(cls, url):
        cls.opened.append(url)
        return True


NS = _load_functions([
    "_run_text", "_first_line", "_system_printers", "_open_printer_settings",
    "_two_sided_supported", "_send_pdf_to_printer", "_macos_print_dialog",
    "_open_pdf_externally", "_write_output_pdf", "_windows_program_path",
    "_windows_print_via_reader",
], {"_PRINT_TIMEOUT": 8,
    "_WINDOWS_PDF_PRINTERS": _module_value("_WINDOWS_PDF_PRINTERS")})


def _reset(platform="linux"):
    _FakeSubprocess.reset()
    _FakeOS.startfiles = []
    _FakeOS.fail_startfile = False
    _FakeShutil.present = set()
    _FakeBrowser.opened = []
    NS["sys"] = mock.Mock(platform=platform)
    return NS


# ---------------------------------------------------------------------------
# Finding the printers
# ---------------------------------------------------------------------------


class PrinterListTests(unittest.TestCase):
    def test_cups_printers_are_read_off_lpstat(self):
        ns = _reset("linux")
        _FakeSubprocess.answering({"lpstat": _Done(
            "HP_LaserJet accepting requests since Mon\n"
            "Brother_DCP accepting requests since Tue\n")})
        names, _default = ns["_system_printers"]()
        self.assertEqual(names[:2], ["HP_LaserJet", "Brother_DCP"])

    def test_the_default_printer_is_reported_with_them(self):
        ns = _reset("linux")

        def reply(cmd):
            if cmd[1:2] == ["-d"]:
                return _Done("system default destination: Brother_DCP\n")
            return _Done("HP_LaserJet accepting\nBrother_DCP accepting\n")

        _FakeSubprocess.answer = reply
        names, default = ns["_system_printers"]()
        self.assertEqual(default, "Brother_DCP")
        self.assertIn("Brother_DCP", names)

    def test_windows_asks_powershell(self):
        ns = _reset("win32")
        def reply(cmd):
            script = cmd[-1]
            if "Get-CimInstance" in script:
                return _Done("Office\n")
            return _Done("Office\nBack Room\n")

        _FakeSubprocess.answer = reply
        names, default = ns["_system_printers"]()
        self.assertEqual(names[:2], ["Office", "Back Room"])
        self.assertEqual(default, "Office")
        self.assertTrue(any("powershell" in c[0]
                            for c in _FakeSubprocess.runs))

    def test_a_machine_with_no_printers_reports_none(self):
        ns = _reset("linux")
        self.assertEqual(ns["_system_printers"](), ([], ""))

    def test_a_command_that_will_not_run_is_not_an_error(self):
        ns = _reset("linux")

        def boom(_cmd):
            raise OSError("nope")

        _FakeSubprocess.answer = boom
        self.assertEqual(ns["_run_text"](["lpstat"]), "")

    def test_duplicates_are_dropped_and_the_default_leads(self):
        ns = _reset("linux")

        def reply(cmd):
            if cmd[1:2] == ["-d"]:
                return _Done("system default destination: Elsewhere\n")
            return _Done("HP accepting\nHP accepting\n")

        _FakeSubprocess.answer = reply
        names, default = ns["_system_printers"]()
        self.assertEqual(names, ["Elsewhere", "HP"])
        self.assertEqual(default, "Elsewhere")


# ---------------------------------------------------------------------------
# Sending the document
# ---------------------------------------------------------------------------


class SendToPrinterTests(unittest.TestCase):
    def test_cups_is_told_which_printer(self):
        ns = _reset("linux")
        self.assertTrue(ns["_send_pdf_to_printer"]("/tmp/a.pdf", "HP"))
        self.assertEqual(_FakeSubprocess.runs[-1],
                         ["lp", "-d", "HP", "/tmp/a.pdf"])

    def test_double_sided_is_a_cups_option(self):
        ns = _reset("linux")
        ns["_send_pdf_to_printer"]("/tmp/a.pdf", "HP", True)
        self.assertIn("sides=two-sided-long-edge", _FakeSubprocess.runs[-1])

    def test_a_queue_that_refuses_reports_failure(self):
        ns = _reset("linux")
        _FakeSubprocess.answering({"lp": _Done("", returncode=1)})
        self.assertFalse(ns["_send_pdf_to_printer"]("/tmp/a.pdf", "HP"))

    def test_windows_prints_through_the_shell_verb(self):
        ns = _reset("win32")
        self.assertTrue(ns["_send_pdf_to_printer"]("C:/a.pdf", "Office"))
        self.assertEqual(_FakeOS.startfiles[-1],
                         ("C:/a.pdf", "printto", '"Office"'))

    def test_a_windows_handler_that_cannot_print_reports_failure(self):
        ns = _reset("win32")
        _FakeOS.fail_startfile = True
        self.assertFalse(ns["_send_pdf_to_printer"]("C:/a.pdf", "Office"))


class WindowsPrintFallbackTests(unittest.TestCase):
    """Windows has no general way to print a PDF to a chosen printer.

    The shell's "printto" verb works only if the program that owns PDFs
    registers it — and the one that owns them by default, Edge, does not,
    which is WinError 1155 ("no application is associated with the specified
    file for this operation").  So two more things are tried.
    """

    def _no_printto(self):
        """os.startfile refuses "printto" the way Edge makes it refuse."""
        ns = _reset("win32")
        refused = {"n": 0}

        def startfile(path, operation=None, arguments=None):
            if operation == "printto":
                refused["n"] += 1
                raise OSError(
                    1155, "No application is associated with the specified "
                          "file for this operation")
            _FakeOS.startfiles.append((path, operation, arguments))

        _FakeOS.startfile = startfile
        self.addCleanup(setattr, _FakeOS, "startfile",
                        _FakeOS.__dict__["startfile"])
        return ns, refused

    def test_a_reader_that_can_target_a_printer_is_used(self):
        ns, refused = self._no_printto()
        _FakeShutil.present = {"SumatraPDF.exe"}
        self.assertTrue(ns["_send_pdf_to_printer"]("C:/a.pdf", "Brother"))
        self.assertEqual(refused["n"], 1)
        self.assertEqual(_FakeSubprocess.popens[-1],
                         ["/usr/bin/SumatraPDF.exe", "-print-to", "Brother",
                          "-silent", "C:/a.pdf"])

    def test_adobe_is_asked_the_way_adobe_wants(self):
        ns, _refused = self._no_printto()
        _FakeShutil.present = {"AcroRd32.exe"}
        self.assertTrue(ns["_send_pdf_to_printer"]("C:/a.pdf", "Brother"))
        self.assertEqual(_FakeSubprocess.popens[-1],
                         ["/usr/bin/AcroRd32.exe", "/n", "/t", "C:/a.pdf",
                          "Brother"])

    def test_with_no_such_reader_the_default_printer_is_printed_to(self):
        # The plain Print verb goes to the default printer — honest only when
        # that is the printer that was asked for.
        ns, _refused = self._no_printto()
        _FakeSubprocess.answering({"powershell": _Done("Brother\n")})
        self.assertTrue(ns["_send_pdf_to_printer"]("C:/a.pdf", "Brother"))
        self.assertEqual(_FakeOS.startfiles[-1], ("C:/a.pdf", "print", None))

    def test_but_never_to_a_different_printer_than_the_one_chosen(self):
        ns, _refused = self._no_printto()
        _FakeSubprocess.answering({"powershell": _Done("Office\n")})
        self.assertFalse(ns["_send_pdf_to_printer"]("C:/a.pdf", "Brother"))
        self.assertEqual(_FakeOS.startfiles, [])

    def test_the_default_is_matched_without_fussing_over_case(self):
        ns, _refused = self._no_printto()
        _FakeSubprocess.answering({"powershell": _Done("  BROTHER \n")})
        self.assertTrue(ns["_send_pdf_to_printer"]("C:/a.pdf", "brother"))

    def test_a_machine_that_names_no_default_still_tries(self):
        ns, _refused = self._no_printto()
        self.assertTrue(ns["_send_pdf_to_printer"]("C:/a.pdf", "Brother"))
        self.assertEqual(_FakeOS.startfiles[-1], ("C:/a.pdf", "print", None))

    def test_a_reader_on_the_path_is_found(self):
        _reset("win32")
        _FakeShutil.present = {"FoxitReader.exe"}
        self.assertEqual(NS["_windows_program_path"]("FoxitReader.exe"),
                         "/usr/bin/FoxitReader.exe")

    def test_one_that_is_not_installed_is_not(self):
        _reset("win32")
        self.assertEqual(NS["_windows_program_path"]("SumatraPDF.exe"), "")

    def test_a_reader_that_will_not_start_is_passed_over(self):
        ns, _refused = self._no_printto()
        _FakeShutil.present = {"SumatraPDF.exe", "AcroRd32.exe"}
        _FakeSubprocess.fail = {"/usr/bin/SumatraPDF.exe"}
        self.assertTrue(ns["_send_pdf_to_printer"]("C:/a.pdf", "Brother"))
        self.assertEqual(_FakeSubprocess.popens[-1][0],
                         "/usr/bin/AcroRd32.exe")

    def test_every_reader_offered_is_asked_for_a_named_printer(self):
        # A switch that only says "print" would silently use the default.
        for exe, build in _module_value("_WINDOWS_PDF_PRINTERS"):
            argv = build(f"C:/{exe}", "C:/doc.pdf", "Brother")
            self.assertTrue(any("Brother" in arg for arg in argv), exe)
            self.assertIn("C:/doc.pdf", argv, exe)

    def test_unix_is_untouched_by_any_of_it(self):
        ns = _reset("linux")
        ns["_send_pdf_to_printer"]("/tmp/a.pdf", "HP")
        self.assertEqual(_FakeSubprocess.runs[-1][0], "lp")
        self.assertEqual(_FakeOS.startfiles, [])

    def test_the_dialog_says_what_stands_in_the_way(self):
        src = _source_of("_PrintDialog", "_print")
        self.assertIn('sys.platform.startswith("win")', src)
        self.assertIn("make this one the default printer", src)

    def test_double_sided_is_offered_only_where_it_can_be_honoured(self):
        # Windows prints by handing the file to whatever owns PDFs, which
        # takes no such instruction — duplex there lives in the printer's own
        # settings, which the dialog links to instead.
        ns = _reset("linux")
        _FakeShutil.present = {"lp"}
        self.assertTrue(ns["_two_sided_supported"]())
        ns = _reset("win32")
        _FakeShutil.present = {"lp"}
        self.assertFalse(ns["_two_sided_supported"]())

    def test_nor_without_cups(self):
        ns = _reset("linux")
        self.assertFalse(ns["_two_sided_supported"]())


class PrinterSettingsTests(unittest.TestCase):
    """A way into the printer's own settings, which the OS owns, not us."""

    def test_windows_opens_the_printing_preferences_for_that_queue(self):
        ns = _reset("win32")
        self.assertTrue(ns["_open_printer_settings"]("Office"))
        self.assertEqual(
            _FakeSubprocess.popens[-1],
            ["rundll32", "printui.dll,PrintUIEntry", "/e", "/n", "Office"])

    def test_linux_prefers_the_desktops_own_printer_tool(self):
        ns = _reset("linux")
        _FakeShutil.present = {"system-config-printer"}
        self.assertTrue(ns["_open_printer_settings"]("HP"))
        self.assertEqual(_FakeSubprocess.popens[-1],
                         ["system-config-printer", "--show-printer", "HP"])

    def test_and_falls_back_to_the_cups_page_for_it(self):
        ns = _reset("linux")
        self.assertTrue(ns["_open_printer_settings"]("HP LaserJet"))
        self.assertEqual(_FakeBrowser.opened[-1],
                         "http://localhost:631/printers/HP%20LaserJet")

    def test_macos_opens_the_printers_preference_pane(self):
        ns = _reset("darwin")
        self.assertTrue(ns["_open_printer_settings"]("Office"))
        self.assertIn("PrintAndScan.prefPane", _FakeSubprocess.popens[-1][-1])

    def test_no_printer_means_nothing_to_open(self):
        ns = _reset("linux")
        self.assertFalse(ns["_open_printer_settings"](""))

    def test_a_tool_that_will_not_start_is_reported_not_raised(self):
        ns = _reset("win32")
        _FakeSubprocess.fail = {"rundll32"}
        self.assertFalse(ns["_open_printer_settings"]("Office"))


class MacDialogTests(unittest.TestCase):
    def test_it_asks_preview_for_the_system_print_dialog(self):
        ns = _reset("darwin")
        self.assertTrue(ns["_macos_print_dialog"]("/tmp/Roe v. Wade.pdf"))
        script = _FakeSubprocess.runs[-1][-1]
        self.assertIn("with print dialog", script)
        self.assertIn("Roe v. Wade.pdf", script)

    def test_the_path_is_quoted_into_the_script(self):
        # A case name carries quotes and backslashes often enough.
        ns = _reset("darwin")
        ns["_macos_print_dialog"]('/tmp/O"Brien.pdf')
        self.assertIn(r'\"Brien', _FakeSubprocess.runs[-1][-1])

    def test_a_refusal_is_reported(self):
        ns = _reset("darwin")
        _FakeSubprocess.answering({"osascript": _Done("", returncode=1)})
        self.assertFalse(ns["_macos_print_dialog"]("/tmp/a.pdf"))


class ViewerFallbackTests(unittest.TestCase):
    """What is left when nothing else can be done — the old behaviour."""

    def test_linux_hands_the_file_to_the_desktop(self):
        ns = _reset("linux")
        self.assertTrue(ns["_open_pdf_externally"]("/tmp/a.pdf"))
        self.assertEqual(_FakeSubprocess.popens[-1], ["xdg-open", "/tmp/a.pdf"])

    def test_windows_uses_the_shell(self):
        ns = _reset("win32")
        self.assertTrue(ns["_open_pdf_externally"]("C:/a.pdf"))
        self.assertEqual(_FakeOS.startfiles[-1], ("C:/a.pdf", None, None))

    def test_a_desktop_with_no_opener_falls_through_to_the_browser(self):
        ns = _reset("linux")
        _FakeSubprocess.fail = {"xdg-open"}
        self.assertTrue(ns["_open_pdf_externally"]("/tmp/a.pdf"))
        self.assertEqual(_FakeBrowser.opened[-1], "file:///tmp/a.pdf")


class PrintRoutingTests(unittest.TestCase):
    """Which of the three paths _print_pdf_file takes."""

    def setUp(self):
        self.src = _source_of.__globals__["SRC"]
        self.body = next(
            ast.get_source_segment(SRC, n) for n in TREE.body
            if isinstance(n, ast.FunctionDef) and n.name == "_print_pdf_file")

    def test_macos_is_asked_for_its_own_dialog_first(self):
        self.assertIn('sys.platform == "darwin" and _macos_print_dialog(path)',
                      self.body)

    def test_everywhere_else_lists_the_printers_and_asks(self):
        self.assertIn("printers, default = _system_printers()", self.body)
        self.assertIn("_PrintDialog(parent, path, printers, default, status)",
                      self.body)

    def test_a_machine_with_no_printers_opens_the_viewer_as_before(self):
        self.assertLess(self.body.index("if printers:"),
                        self.body.index("_open_pdf_externally(path)"))
        self.assertIn("Ctrl/Cmd-P", self.body)

    def test_the_dialog_offers_the_viewer_too(self):
        dialog = _source_of("_PrintDialog", "_open_viewer")
        self.assertIn("_open_pdf_externally(self._path)", dialog)

    def test_a_queue_that_refuses_does_not_close_the_dialog(self):
        # The reader is left somewhere they can act, not with a vanished
        # dialog and nothing printed.
        printing = _source_of("_PrintDialog", "_print")
        self.assertIn("would not take the document", printing)
        self.assertLess(printing.index("if _send_pdf_to_printer"),
                        printing.index("would not take the document"))


# ---------------------------------------------------------------------------
# Saving what would be printed
# ---------------------------------------------------------------------------


class WriteOutputTests(unittest.TestCase):
    def setUp(self):
        self.write = NS["_write_output_pdf"]
        self.tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        self.tmp.close()
        self.path = self.tmp.name

    def tearDown(self):
        pathlib.Path(self.path).unlink(missing_ok=True)

    def test_the_pane_renders_what_is_written(self):
        pane = mock.Mock()
        self.assertTrue(self.write(self.path, b"%PDF-orig", pane))
        pane.export_pdf.assert_called_once_with(
            self.path, whiten_redactions=False, header_cite="")

    def test_the_redaction_treatment_goes_through(self):
        pane = mock.Mock()
        self.write(self.path, b"%PDF", pane, whiten=True, header="410 U.S. 113")
        self.assertEqual(
            pane.export_pdf.call_args.kwargs,
            {"whiten_redactions": True, "header_cite": "410 U.S. 113"})

    def test_with_no_pane_the_original_is_written(self):
        self.assertFalse(self.write(self.path, b"%PDF-orig", None))
        self.assertEqual(pathlib.Path(self.path).read_bytes(), b"%PDF-orig")

    def test_a_rendering_that_fails_falls_back_to_the_original(self):
        pane = mock.Mock()
        pane.export_pdf.side_effect = RuntimeError("no pdfium")
        self.assertFalse(self.write(self.path, b"%PDF-orig", pane))
        self.assertEqual(pathlib.Path(self.path).read_bytes(), b"%PDF-orig")


class SaveMatchesPrintTests(unittest.TestCase):
    """Save writes the file Print would produce, not the document as fetched."""

    def test_the_reader_saves_the_rendering(self):
        src = _source_of("_ScholarTextWindow", "_download_pdf")
        self.assertIn("_write_output_pdf(", src)
        self.assertNotIn('open(path, "wb")', src)

    def test_it_uses_the_same_redaction_treatment_printing_does(self):
        # One method answers for both, so they cannot drift apart.
        save = _source_of("_ScholarTextWindow", "_download_pdf")
        printing = _source_of("_ScholarTextWindow", "_print_pdf")
        self.assertIn("self._pdf_print_header(data)", save)
        self.assertIn("self._pdf_print_header(data)", printing)

    def test_the_floating_viewer_saves_the_page_it_is_showing(self):
        # Its pane, not the reader's — the two can be showing different scans.
        self.assertIn("self._on_save(self._pane)",
                      _source_of("_FloatingPdfWindow", "_save"))

    def test_a_cited_case_is_saved_the_same_way(self):
        src = _source_of("CourtListenerGUI", "_save_cited_pdf")
        self.assertIn("_write_output_pdf(", src)
        self.assertIn("_is_redacted_case_pdf(url)", src)

    def test_the_statute_viewer_too(self):
        src = _source_of("_PdfWindow", "_download")
        self.assertIn("_write_output_pdf(", src)

    def test_and_the_slip_opinion_viewer(self):
        src = _source_of("_SlipOpinionWindow", "_download")
        self.assertIn("_write_output_pdf(", src)
        self.assertNotIn("the official file, unmodified", src)


if __name__ == "__main__":
    unittest.main()
