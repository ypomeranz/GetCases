import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from citations import detect_links
from courtlistener import CourtListenerClient
from courtlistener_gui import (
    _courtlistener_full_opinion_preview,
    _courtlistener_opinion_id,
    _courtlistener_opinion_preview,
    _courtlistener_result_snippet,
    _courtlistener_search_pages,
    _main_view_scotus_order,
    _parse_statute_query,
)
from google_scholar import (
    GoogleScholarFetcher,
    scholar_jurisdiction_value,
)
from opinion_db import OpinionDB, extract_record


SCHOLAR_RESULTS = """
<div class="gs_r gs_or gs_scl">
  <h3 class="gs_rt">
    <a href="/scholar_case?case=123456789">Example v. Respondent</a>
  </h3>
  <div class="gs_a">Supreme Court, 2025 - Google Scholar</div>
  <div class="gs_rs">A search result snippet from Scholar.</div>
</div>
"""


class CourtListenerSearchTests(unittest.TestCase):
    def test_search_sends_page_size_to_api(self):
        client = object.__new__(CourtListenerClient)
        client._get = lambda endpoint, params: (endpoint, params)

        endpoint, params = client.search("qualified immunity", page_size=17)

        self.assertEqual(endpoint, "search/")
        self.assertEqual(params["page_size"], 17)

    def test_result_snippet_prefers_main_opinion_and_removes_html(self):
        item = {
            "snippet": "top-level",
            "opinions": [
                {"cites": [], "snippet": "minor"},
                {
                    "cites": ["1 U.S. 1", "2 U.S. 2"],
                    "snippet": "<mark>The&nbsp;main</mark> opinion text",
                },
            ],
        }

        self.assertEqual(
            _courtlistener_result_snippet(item),
            "The main opinion text",
        )

    def test_selected_result_total_slices_api_twenty_item_page(self):
        class Client:
            def __init__(self):
                self.calls = []

            def search(self, query, **kwargs):
                self.calls.append((query, kwargs))
                return {
                    "count": 100,
                    "results": [{"id": value} for value in range(20)],
                    "next": "https://example.test/search/?cursor=second",
                }

        client = Client()
        pages = list(_courtlistener_search_pages(
            client, "example", max_results=7, type="o",
        ))

        self.assertEqual(len(pages), 1)
        page, done, exhausted = pages[0]
        self.assertEqual(len(page["results"]), 7)
        self.assertTrue(done)
        self.assertFalse(exhausted)
        self.assertEqual(client.calls[0][1]["page_size"], 20)

    def test_selected_result_total_follows_pages_and_stops_exactly(self):
        class Client:
            def __init__(self):
                self.cursors = []

            def search(self, query, **kwargs):
                cursor = kwargs.get("cursor")
                self.cursors.append(cursor)
                start = 0 if cursor is None else 20
                return {
                    "count": 100,
                    "results": [
                        {"id": value} for value in range(start, start + 20)
                    ],
                    "next": (
                        "https://example.test/search/?cursor=second"
                        if cursor is None else
                        "https://example.test/search/?cursor=third"
                    ),
                }

        client = Client()
        pages = list(_courtlistener_search_pages(
            client, "example", max_results=35, type="o",
        ))

        self.assertEqual([len(page[0]["results"]) for page in pages], [20, 15])
        self.assertEqual(client.cursors, [None, "second"])
        self.assertTrue(pages[-1][1])
        self.assertFalse(pages[-1][2])

    def test_all_results_follows_cursor_until_api_is_exhausted(self):
        class Client:
            def __init__(self):
                self.cursors = []

            def search(self, query, **kwargs):
                cursor = kwargs.get("cursor")
                self.cursors.append(cursor)
                if cursor is None:
                    start, size, next_cursor = 0, 20, "second"
                elif cursor == "second":
                    start, size, next_cursor = 20, 20, "third"
                else:
                    start, size, next_cursor = 40, 5, None
                return {
                    "count": 45,
                    "results": [
                        {"id": value} for value in range(start, start + size)
                    ],
                    "next": (
                        f"https://example.test/search/?cursor={next_cursor}"
                        if next_cursor else None
                    ),
                }

        client = Client()
        pages = list(_courtlistener_search_pages(
            client, "example", max_results=None, type="o",
        ))

        self.assertEqual(
            [len(page[0]["results"]) for page in pages], [20, 20, 5],
        )
        self.assertEqual(client.cursors, [None, "second", "third"])
        self.assertTrue(pages[-1][1])
        self.assertTrue(pages[-1][2])

    def test_nested_main_opinion_id_is_extracted(self):
        item = {
            "opinions": [
                {"id": 11, "cites": []},
                {
                    "resource_uri": "/api/rest/v4/opinions/22/",
                    "cites": ["a", "b"],
                },
            ],
        }

        self.assertEqual(_courtlistener_opinion_id(item), 22)

    def test_full_opinion_preview_skips_caption_and_author_byline(self):
        opinion = """
        <opinion>
          <center>EXAMPLE v. RESPONDENT</center>
          <author>Justice Example delivered the opinion of the Court.</author>
          <p>The first real paragraph explains the dispute and the governing
          legal question presented to the Court in this appeal.</p>
          <p>The second paragraph should not be needed.</p>
        </opinion>
        """

        self.assertEqual(
            _courtlistener_opinion_preview(opinion),
            (
                "The first real paragraph explains the dispute and the "
                "governing legal question presented to the Court in this "
                "appeal."
            ),
        )

    def test_full_opinion_preview_uses_cluster_sub_opinion_fallback(self):
        class Client:
            def get_cluster(self, cluster_id, fields=None):
                self.cluster_request = (cluster_id, fields)
                return {
                    "sub_opinions": [
                        "https://www.courtlistener.com/api/rest/v4/opinions/44/"
                    ],
                }

            def get_opinion_text(self, opinion_id):
                self.opinion_id = opinion_id
                return (
                    "EXAMPLE v. RESPONDENT\n\n"
                    "The actual opinion begins with a meaningful explanation "
                    "of the facts and procedural history."
                )

        client = Client()
        preview = _courtlistener_full_opinion_preview(
            client, {"cluster_id": 33},
        )

        self.assertEqual(client.cluster_request, (33, "sub_opinions"))
        self.assertEqual(client.opinion_id, 44)
        self.assertTrue(preview.startswith("The actual opinion begins"))

    def test_main_scotus_order_uses_age_appropriate_signal(self):
        today = dt.date(2026, 7, 29)
        old_merits = {
            "court_id": "scotus",
            "dateFiled": "2024-01-01",
            "citeCount": 10,
            "opinions": [{"cites": []}],
        }
        old_order = {
            "court_id": "scotus",
            "dateFiled": "2024-01-01",
            "citeCount": 0,
            "opinions": [{"cites": []}],
        }
        recent_merits = {
            "court_id": "scotus",
            "dateFiled": "2026-03-01",
            "citeCount": 0,
            "opinions": [{"cites": ["a", "b", "c"]}],
        }
        recent_order = {
            "court_id": "scotus",
            "dateFiled": "2026-03-01",
            "citeCount": 50,
            "opinions": [{"cites": ["a"]}],
        }

        self.assertFalse(_main_view_scotus_order(old_merits, today))
        self.assertTrue(_main_view_scotus_order(old_order, today))
        self.assertFalse(_main_view_scotus_order(recent_merits, today))
        self.assertTrue(_main_view_scotus_order(recent_order, today))


class GoogleScholarJurisdictionTests(unittest.TestCase):
    def test_court_ids_translate_to_scholar_form_values(self):
        self.assertEqual(scholar_jurisdiction_value(), "2006")
        self.assertEqual(scholar_jurisdiction_value({"scotus"}), "4,60")
        self.assertEqual(
            scholar_jurisdiction_value({"ca9", "cal"}),
            "4,129,114,104",
        )
        self.assertIsNone(scholar_jurisdiction_value({"cavet"}))

    def test_search_url_and_cache_are_scoped_by_jurisdiction(self):
        fetcher = object.__new__(GoogleScholarFetcher)
        fetcher._search_cache = {}
        fetcher._opinion_db = None
        requested = []

        class Response:
            text = SCHOLAR_RESULTS

        def fake_get(url):
            requested.append(url)
            return Response()

        fetcher._get = fake_get

        fetcher.search_cases("example", courts={"ca9"})
        fetcher.search_cases("example", courts={"scotus"})
        fetcher.search_cases("example", courts={"ca9"})

        self.assertEqual(len(requested), 2)
        first = parse_qs(urlparse(requested[0]).query)
        second = parse_qs(urlparse(requested[1]).query)
        self.assertEqual(first["as_sdt"], ["4,129,114"])
        self.assertEqual(second["as_sdt"], ["4,60"])


class OpinionSnippetTests(unittest.TestCase):
    def test_snippet_is_saved_and_searchable(self):
        body = (
            "The Court begins with the question presented and the facts. "
            + "Additional analysis follows. " * 30
        )
        html = (
            '<div id="gs_opinion">'
            "<center>EXAMPLE v. RESPONDENT</center>"
            "<center>Supreme Court of the United States.</center>"
            "<center>Decided January 2, 2025.</center>"
            f"<p>{body}</p>"
            "</div>"
        )
        url = "https://scholar.google.com/scholar_case?case=123456789"
        record = extract_record(url, html)

        self.assertTrue(record["snippet"].startswith("The Court begins"))
        self.assertLessEqual(len(record["snippet"]), 300)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = OpinionDB(root / "opinions.jsonl", root / "opinions.db")
            try:
                self.assertTrue(db.add(record))
                hit = db.search_names("Example Respondent")[0]
                self.assertEqual(hit["snippet"], record["snippet"])
                full = db.get_by_scholar_id("123456789")
                self.assertEqual(full["snippet"], record["snippet"])
            finally:
                db.close()

    def test_old_jsonl_record_derives_snippet_during_indexing(self):
        html = (
            '<div id="gs_opinion">'
            "<center>LEGACY v. RECORD</center>"
            "<p>Legacy opinion prose should become a preview snippet.</p>"
            "</div>"
        )
        record = extract_record(
            "https://scholar.google.com/scholar_case?case=987654321",
            html,
        )
        record.pop("snippet")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = OpinionDB(root / "opinions.jsonl", root / "opinions.db")
            try:
                self.assertTrue(db.add(record))
                hit = db.search_names("Legacy Record")[0]
                self.assertEqual(
                    hit["snippet"],
                    "Legacy opinion prose should become a preview snippet.",
                )
            finally:
                db.close()


class CitationRoutingTests(unittest.TestCase):
    def test_cfr_spotlight_query_parses_subsection(self):
        self.assertEqual(
            _parse_statute_query("29 C.F.R. § 1614.105(a)"),
            ("cfr", "29:1614.105:a"),
        )

    def test_wl_only_case_uses_recap_name_court_and_exact_date(self):
        text = (
            "Pecos River Talc LLC and Its Affiliated Debtors v. Emory, "
            "2025 WL 1249947, at *7 (E.D. Va. Apr. 30, 2025)."
        )
        actions = [
            action for _start, _end, action in detect_links(text)
            if action[0] == "recap"
        ]

        self.assertEqual(len(actions), 1)
        spec = json.loads(actions[0][1])
        self.assertEqual(spec["cite"], "2025 WL 1249947")
        self.assertEqual(spec["court"], "vaed")
        self.assertEqual(spec["date"], "2025-04-30")
        self.assertEqual(
            spec["name"],
            "Pecos River Talc LLC and Its Affiliated Debtors v. Emory",
        )

    def test_wl_case_prefers_printed_docket(self):
        text = (
            "Example Holdings, LLC v. Smith, No. 2:24-cv-00777, "
            "2025 WL 7654321 (S.D.N.Y. Jan. 15, 2025)."
        )
        action = next(
            action for _start, _end, action in detect_links(text)
            if action[0] == "recap"
        )
        spec = json.loads(action[1])

        self.assertEqual(spec["docket"], "2:24-cv-00777")
        self.assertEqual(spec["court"], "nysd")
        self.assertEqual(spec["date"], "2025-01-15")


if __name__ == "__main__":
    unittest.main()
