"""Spotlight falls back to a subject search when its name filters find nothing.

Spotlight ranks Google Scholar and CourtListener hits on how close each case
*name* is to what was typed, and drops the rest.  That is right for a caption
("Roe v. Wade") and wrong for a subject ("qualified immunity clearly
established"), where every hit is dropped on the way in and the dropdown comes
back empty even though Scholar answered the query perfectly well.

So when the name pass leaves at most one row, Scholar's own first results page
— which is already a plain full-text search — is shown as it stands, in
Scholar's relevance order, and the status line says that is what happened.

Lifted out of ``courtlistener_gui`` with ``ast`` (importing it needs tkinter,
absent on a headless run) and driven against stubs.
"""

import ast
import pathlib
import re
import types
import typing
import unittest


SRC = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
TREE = ast.parse(SRC)


class _Tk:
    TclError = Exception
    Misc = Menu = Frame = Toplevel = Entry = object


def _load_functions(names, extra=None) -> dict:
    found = {n.name: ast.get_source_segment(SRC, n) for n in TREE.body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"module-level functions not found: {missing}")
    ns = {"tk": _Tk, "re": re, "Optional": typing.Optional}
    ns.update(extra or {})
    for name in names:
        exec(found[name], ns)
    return ns


def _module_value(name: str):
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


def _nested_source(cls: str, method: str, inner: str) -> str:
    """The source of a closure defined inside *method* — the spotlight
    dropdown is one long function of them."""
    outer = next(n for n in TREE.body
                 if isinstance(n, ast.ClassDef) and n.name == cls)
    outer = next(n for n in outer.body
                 if isinstance(n, ast.FunctionDef) and n.name == method)
    for node in ast.walk(outer):
        if isinstance(node, ast.FunctionDef) and node.name == inner:
            return ast.get_source_segment(SRC, node)
    raise AssertionError(f"{cls}.{method} has no {inner}")


NS = _load_functions(["_spotlight_phrase_page"],
                     {"_SPOTLIGHT_PHRASE_MAX_ROWS":
                      _module_value("_SPOTLIGHT_PHRASE_MAX_ROWS")})
phrase_page = NS["_spotlight_phrase_page"]

BUCKET_CAPS = _module_value("_BUCKET_CAPS")


def _hits(n: int) -> list:
    return [types.SimpleNamespace(title=f"Case {i}", url=f"u{i}",
                                  source="Supreme Court, 1973",
                                  snippet="", cited_by=0)
            for i in range(n)]


# ---------------------------------------------------------------------------
# When the fallback fires
# ---------------------------------------------------------------------------


class WhenItFiresTests(unittest.TestCase):
    def test_an_empty_dropdown_gets_scholars_page(self):
        page = _hits(8)
        self.assertEqual(phrase_page(page, 0, False), page)

    def test_so_does_a_dropdown_with_a_single_stray_row(self):
        # One survivor is as much a miss as none: a subject query that happens
        # to share a word with one case name should not be answered by it.
        page = _hits(8)
        self.assertEqual(phrase_page(page, 1, False), page)

    def test_two_rows_are_treated_as_a_real_answer(self):
        self.assertEqual(phrase_page(_hits(8), 2, False), [])

    def test_a_full_dropdown_is_left_alone(self):
        self.assertEqual(phrase_page(_hits(8), 9, False), [])

    def test_a_reporter_citation_is_never_read_as_a_phrase(self):
        # "410 U.S. 113" pins one case; finding none of it is a genuine miss,
        # not something the filters did.
        self.assertEqual(phrase_page(_hits(8), 0, True), [])

    def test_nothing_from_scholar_means_nothing_to_fall_back_to(self):
        self.assertEqual(phrase_page([], 0, False), [])
        self.assertEqual(phrase_page(None, 0, False), [])

    def test_the_page_is_copied_not_handed_over(self):
        page = _hits(3)
        out = phrase_page(page, 0, False)
        out.append("x")
        self.assertEqual(len(page), 3)

    def test_scholars_own_order_is_kept(self):
        # Relevance order is exactly what a subject search wants; the caption
        # ranking that reorders a name search has no business here.
        page = _hits(5)
        self.assertEqual([r.title for r in phrase_page(page, 0, False)],
                         [r.title for r in page])

    def test_the_threshold_is_stated_once(self):
        self.assertEqual(_module_value("_SPOTLIGHT_PHRASE_MAX_ROWS"), 1)


# ---------------------------------------------------------------------------
# How the rows are put up
# ---------------------------------------------------------------------------


class PhraseRowTests(unittest.TestCase):
    """The dropdown wiring, read off the source it is buried in."""

    def setUp(self):
        self.src = _source_of("CourtListenerGUI", "_show_spotlight_dropdown")

    def test_the_unfiltered_page_is_kept_as_scholar_ranked_it(self):
        self.assertIn("scholar_page[:] = results", self.src)
        self.assertLess(self.src.index("scholar_page[:] = results"),
                        self.src.index("_rank_scholar_spotlight_results"))

    def test_the_fallback_runs_only_once_everything_has_answered(self):
        # Firing while CourtListener is still working would fill the dropdown
        # with phrase hits that a name match was about to displace.
        status = _nested_source("CourtListenerGUI", "_show_spotlight_dropdown",
                                "_update_status")
        self.assertIn("_phrase_fallback()", status)
        self.assertLess(status.index("if search_done[0] >= total_searches:"),
                        status.index("_phrase_fallback()"))

    def test_and_only_once(self):
        self.assertIn("if phrase_done[0]:", self.src)
        self.assertIn("phrase_done[0] = True", self.src)

    def test_the_rows_say_they_are_phrase_matches(self):
        self.assertIn('"Scholar (phrase match)"', self.src)

    def test_they_open_the_way_any_scholar_hit_does(self):
        # Same opener as the name-matched rows, so a phrase hit behaves like
        # every other Scholar result once clicked.
        self.assertIn("self._scholar_result_opener(result, cite)", self.src)
        self.assertIn("self._scholar_result_opener(r, cite)", self.src)

    def test_the_status_line_says_what_happened(self):
        flat = re.sub(r'"\s*\n\s*"', "", self.src)
        self.assertIn("No case of that name", flat)
        self.assertIn("for this phrase", flat)

    def test_it_says_so_only_when_rows_were_actually_added(self):
        # The page may hold nothing the dropdown was not already showing.
        self.assertIn("if phrase_added[0]:", self.src)
        self.assertIn("phrase_added[0] = len(result_rows) - before", self.src)

    def test_the_phrase_page_may_fill_the_dropdown(self):
        # It is shown *because* nothing else was found, so it is not rationed
        # the way a source competing for room is.
        self.assertGreaterEqual(BUCKET_CAPS["phrase"], 10)
        self.assertGreater(BUCKET_CAPS["phrase"], BUCKET_CAPS["scholar"])


class ScholarOpenerTests(unittest.TestCase):
    """The opener both paths share."""

    def setUp(self):
        self.src = _source_of("CourtListenerGUI", "_scholar_result_opener")

    def test_it_fetches_the_opinion_page_off_the_ui_thread(self):
        self.assertIn("threading.Thread(target=run, daemon=True).start()",
                      self.src)
        self.assertIn("fetcher.fetch_by_url(result.url)", self.src)

    def test_a_page_that_will_not_load_falls_back_to_courtlistener(self):
        self.assertIn("self._scholar_case_fallback", self.src)

    def test_a_loaded_page_opens_the_reader(self):
        self.assertIn("_ScholarTextWindow(self.root, self, u, h, item=None)",
                      self.src)

    def test_it_is_defined_once_and_used_by_both_passes(self):
        dropdown = _source_of("CourtListenerGUI", "_show_spotlight_dropdown")
        self.assertNotIn("def make_opener(sr=", dropdown)
        self.assertEqual(dropdown.count("self._scholar_result_opener("), 2)


if __name__ == "__main__":
    unittest.main()
