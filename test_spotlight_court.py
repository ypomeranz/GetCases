"""Filling in a spotlight result's court when the search source gave none.

``_court_for_citation`` is lifted out of ``courtlistener_gui`` with ``ast``:
that module imports tkinter, which a headless test run does not have.  Both
sources it consults are stubbed, so the test covers the decision logic — which
source wins, what a miss returns — without touching the network.
"""

import ast
import pathlib
import re
import unittest


def _load(*names):
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    found = {n.name: ast.get_source_segment(src, n)
             for n in tree.body
             if isinstance(n, ast.FunctionDef) and n.name in names}
    missing = [n for n in names if n not in found]
    if missing:
        raise AssertionError(f"not found at module level: {missing}")
    consts = {}
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "_CAP_SCOTUS_RE"):
            consts["_CAP_SCOTUS_RE"] = ast.get_source_segment(src, node)
    ns = {"re": re, "_cl_item_for_citation": None, "_case_law_metadata": None}
    for body in consts.values():
        exec(body, ns)
    for name in names:
        exec(found[name], ns)
    return ns


NS = _load("_court_for_citation")


class _Client:
    """Stands in for a CourtListener client; only its identity matters."""


class CourtForCitationTests(unittest.TestCase):
    def setUp(self):
        self.cl_calls = []
        self.cap_calls = []
        NS["_cl_item_for_citation"] = self._cl
        NS["_case_law_metadata"] = self._cap
        self.cl_item = None
        self.cap_data = None
        self.cl_raises = False
        self.cap_raises = False

    def _cl(self, client, cite, name=""):
        self.cl_calls.append((cite, name))
        if self.cl_raises:
            raise RuntimeError("boom")
        return self.cl_item

    def _cap(self, cite):
        self.cap_calls.append(cite)
        if self.cap_raises:
            raise RuntimeError("boom")
        return self.cap_data

    def call(self, cite, name="", client=None):
        return NS["_court_for_citation"](client, cite, name)

    # -- CourtListener, the preferred source ------------------------------
    def test_courtlistener_court_id_wins(self):
        self.cl_item = {"court_id": "ca4"}
        self.cap_data = {"court": {"name_abbreviation": "Somewhere Else"}}
        self.assertEqual(self.call("819 F. 3d 880", client=_Client()),
                         ("ca4", ""))
        self.assertEqual(self.cap_calls, [], "case.law should not be consulted")

    def test_court_id_is_lowercased(self):
        self.cl_item = {"court_id": "CA4"}
        self.assertEqual(self.call("819 F. 3d 880", client=_Client())[0], "ca4")

    def test_pincite_is_stripped_before_lookup(self):
        self.cl_item = {"court_id": "ca4"}
        self.call("819 F. 3d 880@888", client=_Client())
        self.assertEqual(self.cl_calls[0][0], "819 F. 3d 880")

    def test_case_name_is_passed_through(self):
        self.cl_item = {"court_id": "ca4"}
        self.call("819 F. 3d 880", name="United States v. Carpenter",
                  client=_Client())
        self.assertEqual(self.cl_calls[0][1], "United States v. Carpenter")

    # -- static.case.law, the fallback ------------------------------------
    def test_no_client_falls_straight_through_to_case_law(self):
        self.cap_data = {"court": {"name_abbreviation": "S.D.N.Y."}}
        self.assertEqual(self.call("100 F. Supp. 3d 200"), ("", "S.D.N.Y."))
        self.assertEqual(self.cl_calls, [], "no client, no CourtListener call")

    def test_case_law_used_when_courtlistener_knows_nothing(self):
        self.cl_item = {}
        self.cap_data = {"court": {"name_abbreviation": "4th Cir."}}
        self.assertEqual(self.call("819 F. 3d 880", client=_Client()),
                         ("", "4th Cir."))

    def test_full_court_name_when_no_abbreviation(self):
        self.cap_data = {"court": {"name": "Court of Appeals of Ohio"}}
        self.assertEqual(self.call("44 Ohio App. 3d 12")[1],
                         "Court of Appeals of Ohio")

    def test_case_law_supreme_court_maps_to_the_scotus_id(self):
        # "U.S." is CAP's abbreviation for the Supreme Court; the app's own id
        # is what gives the badge its accent colour, so it is returned instead.
        for label in ("U.S.", "U. S.", "Supreme Court of the United States"):
            with self.subTest(label=label):
                self.cap_data = {"court": {"name_abbreviation": label}}
                self.assertEqual(self.call("585 U. S. 296"), ("scotus", ""))

    # -- misses and failures ----------------------------------------------
    def test_nothing_known_returns_empty(self):
        self.cl_item = None
        self.cap_data = None
        self.assertEqual(self.call("1 Nowhere 1", client=_Client()), ("", ""))

    def test_empty_citation_asks_nobody(self):
        self.assertEqual(self.call(""), ("", ""))
        self.assertEqual(self.cl_calls, [])
        self.assertEqual(self.cap_calls, [])

    def test_courtlistener_failure_falls_back_to_case_law(self):
        self.cl_raises = True
        self.cap_data = {"court": {"name_abbreviation": "4th Cir."}}
        self.assertEqual(self.call("819 F. 3d 880", client=_Client()),
                         ("", "4th Cir."))

    def test_case_law_failure_is_not_fatal(self):
        self.cap_raises = True
        self.assertEqual(self.call("819 F. 3d 880"), ("", ""))

    def test_malformed_case_law_court_block_is_ignored(self):
        for bad in ({"court": "4th Cir."}, {"court": None}, {},
                    {"court": {"name_abbreviation": "   "}}):
            with self.subTest(bad=bad):
                self.cap_data = bad
                self.assertEqual(self.call("819 F. 3d 880"), ("", ""))


if __name__ == "__main__":
    unittest.main()
