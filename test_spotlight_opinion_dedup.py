import json
import tempfile
import unittest
from pathlib import Path

from courtlistener_gui import (
    _case_signature,
    _prefer_source,
    _rank_scholar_spotlight_results,
    _same_case,
)
from google_scholar import GoogleScholarFetcher, ScholarResult
from opinion_db import OpinionDB, extract_record


RESULTS_FIXTURE = """
<div class="gs_r gs_or gs_scl">
  <h3 class="gs_rt">
    <a href="/scholar_case?case=12765554571324969627">
      Louisiana v. Callais
    </a>
  </h3>
  <div class="gs_a">Supreme Court, 2026 - Google Scholar</div>
  <div class="gs_rs">The motion to recall the judgment is denied.</div>
  <div class="gs_fl"></div>
</div>
<div class="gs_r gs_or gs_scl">
  <h3 class="gs_rt">
    <a href="/scholar_case?case=2691414146841290148">
      Louisiana v. Callais
    </a>
  </h3>
  <div class="gs_a">
    146 S. Ct. 1131, 608 U.S. ___ - Supreme Court, 2026 - Google Scholar
  </div>
  <div class="gs_rs">The opinion of the Court addresses the merits.</div>
  <div class="gs_fl">
    <a href="/scholar?cites=2691414146841290148">Cited by 45</a>
  </div>
</div>
"""


def opinion_html(
    *,
    cite: str = "",
    date: str = "April 29, 2026",
    docket: str = "Nos. 24-109 and 24-110",
    body: str,
) -> str:
    cite_block = f"<center>{cite}</center>" if cite else ""
    return (
        '<div id="gs_opinion">'
        f"{cite_block}"
        "<center>LOUISIANA v. PHILLIP CALLAIS, ET AL.</center>"
        f"<center>{docket}</center>"
        "<center>Supreme Court of the United States.</center>"
        f"<center>Decided {date}.</center>"
        f"<p>{body}</p>"
        "</div>"
    )


class ScholarResultOrderingTests(unittest.TestCase):
    def test_result_parser_captures_cited_by_count(self):
        results = GoogleScholarFetcher._parse_results(RESULTS_FIXTURE)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].cited_by, 0)
        self.assertEqual(results[1].cited_by, 45)

    def test_most_cited_same_case_scotus_writing_is_promoted(self):
        results = GoogleScholarFetcher._parse_results(RESULTS_FIXTURE)

        ranked = _rank_scholar_spotlight_results(
            "Louisiana v. Callais", results, 6,
        )

        self.assertEqual(
            ranked[0].url,
            "https://scholar.google.com/"
            "scholar_case?case=2691414146841290148",
        )
        self.assertEqual(ranked[1].cited_by, 0)


class SpotlightOpinionIdentityTests(unittest.TestCase):
    def test_unreported_scotus_writings_are_not_caption_year_deduped(self):
        reported = _case_signature(
            "Louisiana v. Callais",
            "146 S. Ct. 1131",
            "2026",
            "scotus",
            opinion_id="scholar:2691414146841290148",
        )
        unreported = _case_signature(
            "Louisiana v. Callais",
            "",
            "2026",
            "scotus",
            opinion_id="scholar:12765554571324969627",
        )

        self.assertFalse(_same_case(reported, unreported))

    def test_exact_scholar_id_dedupes_live_and_saved_rows(self):
        live = _case_signature(
            "Louisiana v. Callais",
            "146 S. Ct. 1131",
            "2026",
            "scotus",
            opinion_id="scholar:2691414146841290148",
        )
        saved = _case_signature(
            "Louisiana v. Callais",
            "",
            "2026",
            "scotus",
            opinion_id="scholar:2691414146841290148",
        )

        self.assertTrue(_same_case(live, saved))
        self.assertTrue(_prefer_source("scholar", "opiniondb"))

    def test_non_scotus_caption_year_behavior_is_unchanged(self):
        cited = _case_signature(
            "Example v. State", "10 F.4th 20", "2024", "ca5",
        )
        uncited = _case_signature(
            "Example v. State", "", "2024", "ca5",
        )

        self.assertTrue(_same_case(cited, uncited))


class ReportedVersionUpgradeTests(unittest.TestCase):
    def setUp(self):
        common = (
            "Justice Alito delivered the opinion of the Court. "
            "These cases concern whether the congressional map is an "
            "unconstitutional racial gerrymander. "
        )
        self.old_html = opinion_html(body=common * 80)
        self.new_html = opinion_html(
            cite="146 S. Ct. 1131",
            body=(
                "Counsel appeared for all parties. "
                + common * 80
                + "The judgment is reversed."
            ),
        )
        self.other_html = opinion_html(
            cite="146 S. Ct. 1200",
            body=(
                "Justice Example dissented. The Court should have dismissed "
                "these appeals for want of jurisdiction. "
            ) * 80,
        )
        self.old_url = (
            "https://scholar.google.com/"
            "scholar_case?case=2971768245956332168"
        )
        self.new_url = (
            "https://scholar.google.com/"
            "scholar_case?case=2691414146841290148"
        )

    def test_extracts_exact_date_dockets_and_scotus_from_unreported_page(self):
        record = extract_record(self.old_url, self.old_html)

        self.assertEqual(record["court"], "scotus")
        self.assertEqual(record["date_filed"], "2026-04-29")
        self.assertEqual(record["dockets"], ["24-109", "24-110"])

    def test_new_reported_page_replaces_matching_unreported_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = OpinionDB(root / "opinions.jsonl", root / "opinions.db")
            try:
                self.assertTrue(db.add_opinion(self.old_url, self.old_html))
                self.assertTrue(db.add_opinion(self.new_url, self.new_html))

                self.assertEqual(db.count(), 1)
                self.assertIsNone(
                    db.get_by_scholar_id("2971768245956332168")
                )
                reported = db.get_by_scholar_id("2691414146841290148")
                self.assertEqual(reported["cites"], ["146 S. Ct. 1131"])
                lines = [
                    json.loads(line)
                    for line in (root / "opinions.jsonl").read_text(
                        encoding="utf-8"
                    ).splitlines()
                ]
                self.assertEqual(
                    [line["scholar_id"] for line in lines],
                    ["2691414146841290148"],
                )
            finally:
                db.close()

    def test_same_date_and_docket_do_not_merge_different_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = OpinionDB(root / "opinions.jsonl", root / "opinions.db")
            try:
                self.assertTrue(db.add_opinion(self.old_url, self.old_html))
                self.assertTrue(db.add_opinion(
                    "https://scholar.google.com/"
                    "scholar_case?case=9999999999999999999",
                    self.other_html,
                ))

                self.assertEqual(db.count(), 2)
            finally:
                db.close()

    def test_existing_old_and_new_rows_are_reconciled_on_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            jsonl = root / "opinions.jsonl"
            index = root / "opinions.db"
            old = extract_record(self.old_url, self.old_html)
            new = extract_record(self.new_url, self.new_html)
            db = OpinionDB(jsonl, index)
            try:
                # Generic/imported records can predate the add_opinion upgrade
                # path and therefore coexist until the next reconciliation.
                self.assertTrue(db.add(old))
                self.assertTrue(db.add(new))
                self.assertEqual(db.count(), 2)
            finally:
                db.close()

            reopened = OpinionDB(jsonl, index)
            try:
                self.assertEqual(reopened.count(), 1)
                self.assertIsNotNone(
                    reopened.get_by_scholar_id("2691414146841290148")
                )
            finally:
                reopened.close()


if __name__ == "__main__":
    unittest.main()
