"""The copy styles an opinion reader offers.

The quotation style is the one with real logic in it: a passage going inside
fresh double quotes has to have its own quotes demoted a level, without touching
apostrophes, in both the plain text and the RTF the clipboard carries.  Those
pieces are lifted out of ``courtlistener_gui`` with ``ast`` — the module imports
tkinter, which a headless run does not have.
"""

import ast
import json
import pathlib
import re
import tempfile
import unittest


def _load(*names):
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    wanted = set(names)
    found = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in wanted:
            found[node.name] = ast.get_source_segment(src, node)
    need_consts = {
        "_QUOTE_OPEN", "_QUOTE_CLOSE", "_SQUOTE_OPEN", "_SQUOTE_CLOSE",
        "_RTF_QUOTE_TOKEN_RE", "COPY_MODES", "COPY_MODE_LABELS",
        "_DEFAULT_COPY_MODE", "_CONFIG_PATH",
    }
    consts = {}

    def _names(target):
        """Names bound by an assignment target, tuple unpacking included."""
        if isinstance(target, ast.Name):
            return [target.id]
        if isinstance(target, (ast.Tuple, ast.List)):
            return [n for t in target.elts for n in _names(t)]
        return []

    for node in tree.body:
        targets = (node.targets if isinstance(node, ast.Assign)
                   else [node.target] if isinstance(node, ast.AnnAssign)
                   else [])
        bound = {n for t in targets for n in _names(t)}
        if bound & need_consts:
            consts[tuple(sorted(bound))] = ast.get_source_segment(src, node)
    still_missing = need_consts - {n for key in consts for n in key}
    if still_missing:
        raise AssertionError(f"constants not found: {sorted(still_missing)}")
    missing = wanted - set(found)
    if missing:
        raise AssertionError(f"not found at module level: {sorted(missing)}")
    ns = {"re": re, "json": json, "pathlib": pathlib, "Path": pathlib.Path}
    for body in consts.values():
        exec(body, ns)
    for name in names:
        exec(found[name], ns)
    return ns


NS = _load(
    "_QuoteFlipper", "_flip_quotes", "_rtf_escape", "_flip_rtf_quotes",
    "_parenthetical_plain", "_parenthetical_rtf",
    "_load_config", "_save_config", "_load_copy_mode", "_save_copy_mode",
    "_next_copy_mode",
)
flip = NS["_flip_quotes"]
flip_rtf = NS["_flip_rtf_quotes"]
esc = NS["_rtf_escape"]

LD, RD = "\u201c", "\u201d"      # “ ”
LS, RS = "\u2018", "\u2019"      # ‘ ’


class QuoteFlipTests(unittest.TestCase):
    """Doubles become singles, singles become doubles, apostrophes stay put."""

    def test_double_quotes_are_demoted_to_single(self):
        self.assertEqual(flip(f"the {LD}plain{RD} meaning"),
                         f"the {LS}plain{RS} meaning")

    def test_single_quotes_are_promoted_to_double(self):
        self.assertEqual(flip(f"the {LS}plain{RS} meaning"),
                         f"the {LD}plain{RD} meaning")

    def test_nested_quotes_swap_together(self):
        # What the reader sees, and what it must become one level in.
        self.assertEqual(
            flip(f"The Court said {LD}the statute is {LS}plain{RS} enough.{RD}"),
            f"The Court said {LS}the statute is {LD}plain{RD} enough.{RS}")

    def test_apostrophes_survive(self):
        for text in [f"the Government{RS}s argument",
                     f"Ass{RS}n of Data Processing",
                     f"O{RS}Connor, J., concurring",
                     f"don{RS}t and won{RS}t",
                     f"the States{RS} rights"]:
            with self.subTest(text=text):
                self.assertEqual(flip(text), text)

    def test_an_apostrophe_inside_a_quotation_still_survives(self):
        self.assertEqual(
            flip(f"{LD}the Government{RS}s theory{RD}"),
            f"{LS}the Government{RS}s theory{RS}")

    def test_a_closing_single_quote_flips_only_when_one_is_open(self):
        # First ’ closes the ‘ that opened; the second is a possessive.
        self.assertEqual(
            flip(f"{LS}plain{RS} and the Government{RS}s view"),
            f"{LD}plain{RD} and the Government{RS}s view")

    def test_straight_double_quotes_are_demoted(self):
        self.assertEqual(flip('the "plain" meaning'), "the 'plain' meaning")

    def test_text_without_quotes_is_unchanged(self):
        self.assertEqual(flip("no quotation marks here at all"),
                         "no quotation marks here at all")


class RtfQuoteFlipTests(unittest.TestCase):
    """The rich copy is the one that lands in a brief, so it flips too."""

    def test_escaped_quotes_are_flipped_in_place(self):
        body = "\\pard\\sa120 the " + esc(LD) + "plain" + esc(RD) + " meaning\\par\n"
        self.assertEqual(
            flip_rtf(body),
            "\\pard\\sa120 the " + esc(LS) + "plain" + esc(RS) + " meaning\\par\n")

    def test_markup_is_left_alone(self):
        body = "\\pard\\sa120 {\\i Roe}\\line plain\\par\n"
        self.assertEqual(flip_rtf(body), body)

    def test_non_quote_escapes_pass_through(self):
        body = "\\pard\\sa120 " + esc("§ 1782 — the em dash") + "\\par\n"
        self.assertEqual(flip_rtf(body), body)

    def test_apostrophes_survive_in_rtf(self):
        body = "\\pard\\sa120 the Government" + esc(RS) + "s view\\par\n"
        self.assertEqual(flip_rtf(body), body)

    def test_plain_and_rtf_flips_agree(self):
        text = (f"The Court held that {LD}the {LS}plain{RS} meaning of the "
                f"Government{RS}s statute{RD} controls.")
        rtf_flipped = flip_rtf(esc(text))
        self.assertEqual(rtf_flipped, esc(flip(text)))


class ParentheticalCopyTests(unittest.TestCase):
    def test_plain_citation_precedes_the_flipped_quotation(self):
        citation = "Smith v. Jones, 1 F.4th 2, 3 (2d Cir. 2020)."
        passage = f"The Court called it {LD}plain{RD} and O{RS}Connor agreed."
        self.assertEqual(
            NS["_parenthetical_plain"](citation, passage),
            (
                f"Smith v. Jones, 1 F.4th 2, 3 (2d Cir. 2020) "
                f"({LD}The Court called it {LS}plain{RS} and "
                f"O{RS}Connor agreed.{RD})."
            ),
        )

    def test_rtf_keeps_citation_first_and_period_after_parenthesis(self):
        citation = " {\\i Smith v. Jones}, 1 F.4th 2."
        passage = "\\pard The " + esc(LD) + "plain" + esc(RD) + "\\par\n"
        got = NS["_parenthetical_rtf"](citation, passage)
        self.assertEqual(
            got,
            (
                "{\\i Smith v. Jones}, 1 F.4th 2 ("
                + esc(LD) + "\\pard The " + esc(LS) + "plain"
                + esc(RS) + esc(RD) + ").\\par\n"
            ),
        )


class CopyModePersistenceTests(unittest.TestCase):
    def setUp(self):
        NS["_CONFIG_PATH"] = pathlib.Path(tempfile.mkdtemp()) / "config.json"

    def test_default_is_the_long_standing_behaviour(self):
        self.assertEqual(NS["_load_copy_mode"](), "cite")

    def test_each_mode_round_trips(self):
        for mode in NS["COPY_MODES"]:
            with self.subTest(mode=mode):
                NS["_save_copy_mode"](mode)
                self.assertEqual(NS["_load_copy_mode"](), mode)

    def test_an_unknown_mode_is_ignored(self):
        NS["_save_copy_mode"]("quote")
        NS["_save_copy_mode"]("nonsense")
        self.assertEqual(NS["_load_copy_mode"](), "quote")

    def test_a_corrupt_config_falls_back_to_the_default(self):
        NS["_CONFIG_PATH"].parent.mkdir(parents=True, exist_ok=True)
        NS["_CONFIG_PATH"].write_text("{not json")
        self.assertEqual(NS["_load_copy_mode"](), "cite")

    def test_saving_the_mode_keeps_other_settings(self):
        NS["_save_config"]({"api_token": "abc123"})
        NS["_save_copy_mode"]("quote")
        self.assertEqual(NS["_load_config"]()["api_token"], "abc123")

    def test_the_labels_cover_every_mode(self):
        self.assertEqual(tuple(v for v, _l in NS["COPY_MODE_LABELS"]),
                         NS["COPY_MODES"])
        self.assertIn(
            ("parenthetical", "Copy as parenthetical"),
            NS["COPY_MODE_LABELS"],
        )

    def test_x_cycle_visits_every_mode_and_wraps(self):
        mode = NS["COPY_MODES"][0]
        visited = []
        for _ in NS["COPY_MODES"]:
            visited.append(mode)
            mode = NS["_next_copy_mode"](mode)
        self.assertEqual(tuple(visited), NS["COPY_MODES"])
        self.assertEqual(mode, NS["COPY_MODES"][0])


class CopyMenuLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = pathlib.Path(__file__).with_name(
            "courtlistener_gui.py"
        ).read_text(encoding="utf-8")

    def test_copy_menu_is_appended_after_bookmarks(self):
        tree = ast.parse(self.source)
        node = next(
            item for item in tree.body
            if isinstance(item, ast.FunctionDef)
            and item.name == "_install_history_menubar"
        )
        body = ast.get_source_segment(self.source, node)
        self.assertLess(
            body.rfind("_add_bookmarks_cascade"),
            body.rfind("_add_copy_cascade"),
        )

    def test_citation_only_command_is_first_and_x_is_bound(self):
        menu = self.source.index('label="Copy citation to clipboard"')
        radios = self.source.index("for value, label in COPY_MODE_LABELS")
        self.assertLess(menu, radios)
        self.assertIn('bind("<KeyPress-x>", self._cycle_copy_mode)', self.source)


if __name__ == "__main__":
    unittest.main()
