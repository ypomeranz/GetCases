"""Late U.S. Reports citation recovery for older S. Ct.-only cases."""

import datetime as dt
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GETCASES_SKIP_DEPENDENCY_PROMPT", "1")

import courtlistener_gui as gui
from google_scholar import GoogleScholarFetcher
from opinion_db import OpinionDB


class AgeGateTests(unittest.TestCase):
    def test_case_must_be_strictly_more_than_four_years_old(self):
        today = dt.date(2026, 7, 29)
        self.assertTrue(gui._more_than_four_years_old(
            {"dateFiled": "2022-07-28"}, today,
        ))
        self.assertFalse(gui._more_than_four_years_old(
            {"dateFiled": "2022-07-29"}, today,
        ))
        self.assertFalse(gui._more_than_four_years_old(
            {"dateFiled": "2022-07-30"}, today,
        ))

    def test_bare_year_is_used_conservatively(self):
        today = dt.date(2026, 7, 29)
        self.assertTrue(gui._more_than_four_years_old({"year": "2021"}, today))
        self.assertFalse(gui._more_than_four_years_old({"year": "2022"}, today))
        self.assertFalse(gui._more_than_four_years_old(
            {"dateFiled": "not-a-date"}, today,
        ))


class ScholarParallelCitationTests(unittest.TestCase):
    class Fetcher:
        def __init__(self, results):
            self.results = results
            self.calls = []

        def search_cases(self, query, **kwargs):
            self.calls.append((query, kwargs))
            return self.results

    def test_reads_parallel_cite_from_result_header_not_snippet(self):
        fetcher = self.Fetcher([
            SimpleNamespace(
                title="Ramos v. Louisiana",
                source="590 U.S. 83, 140 S. Ct. 1390 - Supreme Court, 2020",
                snippet="An unrelated case cites 410 U.S. 113.",
            ),
        ])
        self.assertEqual(
            gui._us_reports_cite_from_scholar_parallel(
                fetcher, "Ramos v. Louisiana", "140 S. Ct. 1390",
            ),
            "590 U.S. 83",
        )
        query, kwargs = fetcher.calls[0]
        self.assertIn("Ramos v. Louisiana", query)
        self.assertIn('"140 S. Ct. 1390"', query)
        self.assertEqual(kwargs["courts"], {"scotus"})
        self.assertFalse(kwargs["allow_db_fallback"])

    def test_rejects_wrong_case_and_snippet_only_us_cite(self):
        fetcher = self.Fetcher([
            SimpleNamespace(
                title="Ramos v. Nielsen",
                source="140 S. Ct. 1390 - Supreme Court, 2020",
                snippet="Ramos v. Louisiana, 590 U.S. 83.",
            ),
            SimpleNamespace(
                title="Ramos v. Louisiana",
                source="140 S. Ct. 1390 - Supreme Court, 2020",
                snippet="Ramos v. Louisiana, 590 U.S. 83.",
            ),
        ])
        self.assertIsNone(
            gui._us_reports_cite_from_scholar_parallel(
                fetcher, "Ramos v. Louisiana", "140 S. Ct. 1390",
            )
        )

    def test_online_only_search_does_not_substitute_database_results(self):
        fetcher = object.__new__(GoogleScholarFetcher)
        fetcher._search_cache = {}
        fetcher._get = mock.Mock(side_effect=RuntimeError("blocked"))
        fetcher._db_search_results = mock.Mock(return_value=[
            SimpleNamespace(title="local result"),
        ])
        self.assertEqual(
            fetcher.search_cases(
                "Ramos v. Louisiana",
                courts={"scotus"},
                allow_db_fallback=False,
            ),
            [],
        )
        fetcher._db_search_results.assert_not_called()


class CitingTextTests(unittest.TestCase):
    def test_accepts_parallel_citation_beside_target_sct_cite(self):
        text = (
            "The rule follows Ramos v. Louisiana, "
            "590 U. S. 83, 90, 140 S. Ct. 1390 (2020)."
        )
        self.assertEqual(
            gui._us_reports_cite_in_citing_text(
                text, "Ramos v. Louisiana", "140 S. Ct. 1390",
            ),
            "590 U.S. 83",
        )

    def test_accepts_target_short_name_when_sct_cite_was_dropped(self):
        self.assertEqual(
            gui._us_reports_cite_in_citing_text(
                "See Ramos, 590 U.S. 83, 90 (2020).",
                "Ramos v. Louisiana",
                "140 S. Ct. 1390",
            ),
            "590 U.S. 83",
        )

    def test_rejects_unrelated_us_cite_after_semicolon(self):
        self.assertIsNone(
            gui._us_reports_cite_in_citing_text(
                "Ramos v. Louisiana, 140 S. Ct. 1390; "
                "Roe v. Wade, 410 U.S. 113.",
                "Ramos v. Louisiana",
                "140 S. Ct. 1390",
            )
        )


class FakeCitingClient:
    def __init__(self, pages, texts):
        self.pages = pages
        self.texts = texts
        self.search_calls = []
        self.text_calls = []

    def get_cluster(self, _cluster_id, fields=None):
        return {"sub_opinions": []}

    def search(self, query, **kwargs):
        self.search_calls.append((query, kwargs))
        cursor = kwargs.get("cursor")
        return self.pages[cursor]

    def get_opinion_text(self, opinion_id):
        self.text_calls.append(opinion_id)
        return self.texts[opinion_id]


def _citing_result(opinion_id, date, target_id=900):
    return {
        "cluster_id": opinion_id + 1000,
        "dateFiled": date,
        "opinions": [{
            "id": opinion_id,
            "cites": [target_id],
            "type": "combined-opinion",
        }],
    }


class CitingOpinionPaginationTests(unittest.TestCase):
    target = {
        "cluster_id": 42,
        "opinions": [{"id": 900, "type": "combined-opinion"}],
    }

    def test_checks_each_batch_newest_first(self):
        results = [
            _citing_result(1, "2024-01-01"),
            _citing_result(2, "2025-01-01"),
        ]
        client = FakeCitingClient(
            {None: {"results": results, "next": None}},
            {
                2: "Ramos v. Louisiana, 140 S. Ct. 1390 (2020).",
                1: "See Ramos, 590 U.S. 83, 90 (2020).",
            },
        )
        self.assertEqual(
            gui._us_reports_cite_from_citing_opinions(
                client, self.target, "Ramos v. Louisiana",
                "140 S. Ct. 1390",
            ),
            "590 U.S. 83",
        )
        self.assertEqual(client.text_calls, [2, 1])
        query, kwargs = client.search_calls[0]
        self.assertEqual(query, "cites:900")
        self.assertEqual(kwargs["page_size"], 10)
        self.assertEqual(kwargs["extra"], {"order_by": "dateFiled desc"})

    def test_stops_after_fifty_opinions(self):
        pages = {}
        texts = {}
        cursors = [None, "p2", "p3", "p4", "p5", "p6"]
        opinion_id = 1
        for page_index, cursor in enumerate(cursors):
            results = []
            for offset in range(10):
                results.append(_citing_result(
                    opinion_id,
                    f"202{5 - min(page_index, 5)}-01-{offset + 1:02d}",
                ))
                texts[opinion_id] = (
                    "Ramos v. Louisiana, 140 S. Ct. 1390 (2020)."
                )
                opinion_id += 1
            next_cursor = cursors[page_index + 1] if page_index < 5 else None
            pages[cursor] = {
                "results": results,
                "next": (
                    f"https://www.courtlistener.com/api/rest/v4/search/"
                    f"?cursor={next_cursor}"
                    if next_cursor else None
                ),
            }
        client = FakeCitingClient(pages, texts)
        self.assertIsNone(
            gui._us_reports_cite_from_citing_opinions(
                client, self.target, "Ramos v. Louisiana",
                "140 S. Ct. 1390",
            )
        )
        self.assertEqual(len(client.search_calls), 5)
        self.assertEqual(len(client.text_calls), 50)
        self.assertNotIn(51, client.text_calls)


class FallbackOrderingTests(unittest.TestCase):
    old_item = {
        "caseName": "Ramos v. Louisiana",
        "dateFiled": "2020-04-20",
    }
    cites = ["140 S. Ct. 1390"]

    def test_scholar_success_skips_citing_opinions(self):
        with (
            mock.patch.object(
                gui, "_us_reports_cite_from_scholar_parallel",
                return_value="590 U.S. 83",
            ) as scholar,
            mock.patch.object(
                gui, "_us_reports_cite_from_citing_opinions",
            ) as citing,
        ):
            self.assertEqual(
                gui._late_scotus_us_reports_cite(
                    object(), object(), self.old_item, self.cites,
                    today=dt.date(2026, 7, 29),
                ),
                "590 U.S. 83",
            )
        scholar.assert_called_once()
        citing.assert_not_called()

    def test_recent_case_runs_neither_fallback(self):
        item = dict(self.old_item, dateFiled="2024-04-20")
        with (
            mock.patch.object(
                gui, "_us_reports_cite_from_scholar_parallel",
            ) as scholar,
            mock.patch.object(
                gui, "_us_reports_cite_from_citing_opinions",
            ) as citing,
        ):
            self.assertIsNone(gui._late_scotus_us_reports_cite(
                object(), object(), item, self.cites,
                today=dt.date(2026, 7, 29),
            ))
        scholar.assert_not_called()
        citing.assert_not_called()


class RecoveredCitationPersistenceTests(unittest.TestCase):
    @staticmethod
    def _record() -> dict:
        return {
            "v": 6,
            "scholar_id": "123456",
            "url": (
                "https://scholar.google.com/"
                "scholar_case?case=123456"
            ),
            "name": "Ramos v. Louisiana",
            "parties": ["ramos", "louisiana"],
            "cites": ["140 S. Ct. 1390"],
            "court": "scotus",
            "year": "2020",
            "date_filed": "2020-04-20",
            "added_at": 1.0,
            "source": "scholar",
            "snippet": "",
            "html_gz": "",
        }

    def test_database_adds_parallel_cite_and_indexes_it(self):
        with tempfile.TemporaryDirectory() as folder:
            db = OpinionDB(
                Path(folder) / "opinions.jsonl",
                Path(folder) / "opinions.index.db",
            )
            try:
                self.assertTrue(db.add(self._record()))
                self.assertTrue(
                    db.save_parallel_citation("123456", "590 U. S. 83")
                )
                self.assertFalse(
                    db.save_parallel_citation("123456", "590 U.S. 83")
                )
                self.assertEqual(
                    db.stored_citations("123456"),
                    ["140 S. Ct. 1390", "590 U.S. 83"],
                )
                hits = db.find_by_citation(590, "U.S.", 83)
                self.assertEqual(
                    [hit["scholar_id"] for hit in hits],
                    ["123456"],
                )
                self.assertEqual(
                    db.stored_record("123456")["cites"],
                    ["140 S. Ct. 1390", "590 U.S. 83"],
                )
            finally:
                db.close()

    def test_pdf_item_reuses_saved_parallel_cite(self):
        with tempfile.TemporaryDirectory() as folder:
            db = OpinionDB(
                Path(folder) / "opinions.jsonl",
                Path(folder) / "opinions.index.db",
            )
            try:
                db.add(self._record())
                db.save_parallel_citation("123456", "590 U.S. 83")

                window = object.__new__(gui._ScholarTextWindow)
                window._app = SimpleNamespace(
                    _get_opinion_db=lambda wait=False: db,
                )
                window._item = {}
                window._scholar_url = self._record()["url"]
                window._is_scotus = True
                window._blocks = []
                window._header_cites = ["140 S. Ct. 1390"]
                window._bb = {
                    "name": "Ramos v. Louisiana",
                    "cite": "140 S. Ct. 1390",
                }

                item = window._pdf_item()

                self.assertEqual(item["scholar_id"], "123456")
                self.assertEqual(
                    item["citation"],
                    ["140 S. Ct. 1390", "590 U.S. 83"],
                )
            finally:
                db.close()


class ResolverIntegrationTests(unittest.TestCase):
    class Session:
        headers = {}

        def head(self, _url, **_kwargs):
            return SimpleNamespace(status_code=200)

    def test_recovered_cite_retries_official_pdf_paths(self):
        app = object.__new__(gui.CourtListenerGUI)
        app._get_scholar = lambda: object()
        opinion_db = SimpleNamespace(
            stored_citations=lambda _sid: [],
            save_parallel_citation=mock.Mock(return_value=True),
        )
        app._get_opinion_db = lambda: opinion_db
        item = {
            "caseName": "Ramos v. Louisiana",
            "court_id": "scotus",
            "dateFiled": "2020-04-20",
            "citation": ["140 S. Ct. 1390"],
            "scholar_id": "123456",
        }
        expected = (
            "https://tile.loc.gov/storage-services/service/ll/"
            "usrep590/usrep590083/usrep590083.pdf"
        )
        with (
            mock.patch.object(gui, "_anon_session", self.Session()),
            mock.patch.object(
                gui, "_us_reports_loc_url",
                side_effect=lambda cite: (
                    expected if gui._normalized_us_cite(cite) == "590 U.S. 83"
                    else None
                ),
            ),
            mock.patch.object(
                gui, "_us_reports_govinfo_url", return_value=None,
            ),
            mock.patch.object(
                gui.us_reports_pdf, "extract_citation", return_value=None,
            ),
            mock.patch.object(
                gui, "_scotus_slip_pdf_url", return_value=None,
            ),
            mock.patch.object(
                gui, "_courtlistener_pdf_fallback_item", return_value=None,
            ),
            mock.patch.object(
                gui, "_late_scotus_us_reports_cite",
                return_value="590 U.S. 83",
            ),
        ):
            url = gui.CourtListenerGUI._resolve_pdf_url(app, None, item)
        self.assertEqual(url, expected)
        self.assertEqual(item["_us_reports_cite"], "590 U.S. 83")
        opinion_db.save_parallel_citation.assert_called_once_with(
            "123456", "590 U.S. 83",
        )

    def test_saved_parallel_cite_is_used_before_late_fallback(self):
        app = object.__new__(gui.CourtListenerGUI)
        opinion_db = SimpleNamespace(
            stored_citations=lambda _sid: [
                "140 S. Ct. 1390", "590 U.S. 83",
            ],
            save_parallel_citation=mock.Mock(return_value=False),
        )
        app._get_opinion_db = lambda: opinion_db
        item = {
            "caseName": "Ramos v. Louisiana",
            "court_id": "scotus",
            "dateFiled": "2020-04-20",
            "citation": ["140 S. Ct. 1390"],
            "scholar_id": "123456",
        }
        expected = (
            "https://tile.loc.gov/storage-services/service/ll/"
            "usrep590/usrep590083/usrep590083.pdf"
        )
        with (
            mock.patch.object(gui, "_anon_session", self.Session()),
            mock.patch.object(
                gui, "_us_reports_loc_url",
                side_effect=lambda cite: (
                    expected if gui._normalized_us_cite(cite) == "590 U.S. 83"
                    else None
                ),
            ),
            mock.patch.object(
                gui, "_us_reports_govinfo_url", return_value=None,
            ),
            mock.patch.object(
                gui.us_reports_pdf, "extract_citation", return_value=None,
            ),
            mock.patch.object(
                gui, "_late_scotus_us_reports_cite",
            ) as late_fallback,
        ):
            url = gui.CourtListenerGUI._resolve_pdf_url(app, None, item)

        self.assertEqual(url, expected)
        self.assertIn("590 U.S. 83", item["citation"])
        late_fallback.assert_not_called()

    def test_recovered_cite_is_saved_even_when_pdf_probe_fails(self):
        app = object.__new__(gui.CourtListenerGUI)
        app._get_scholar = lambda: object()
        opinion_db = SimpleNamespace(
            stored_citations=lambda _sid: [],
            save_parallel_citation=mock.Mock(return_value=True),
        )
        app._get_opinion_db = lambda: opinion_db
        item = {
            "caseName": "Ramos v. Louisiana",
            "court_id": "scotus",
            "dateFiled": "2020-04-20",
            "citation": ["140 S. Ct. 1390"],
            "scholar_id": "123456",
        }
        with (
            mock.patch.object(gui, "_us_reports_loc_url", return_value=None),
            mock.patch.object(
                gui, "_us_reports_govinfo_url", return_value=None,
            ),
            mock.patch.object(
                gui.us_reports_pdf, "extract_citation", return_value=None,
            ),
            mock.patch.object(
                gui, "_scotus_slip_pdf_url", return_value=None,
            ),
            mock.patch.object(
                gui, "_courtlistener_pdf_fallback_item", return_value=None,
            ),
            mock.patch.object(
                gui, "_late_scotus_us_reports_cite",
                return_value="590 U.S. 83",
            ),
        ):
            url = gui.CourtListenerGUI._resolve_pdf_url(app, None, item)

        self.assertIsNone(url)
        self.assertEqual(item["_us_reports_cite"], "590 U.S. 83")
        opinion_db.save_parallel_citation.assert_called_once_with(
            "123456", "590 U.S. 83",
        )


if __name__ == "__main__":
    unittest.main()
