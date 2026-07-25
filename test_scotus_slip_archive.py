import unittest
from unittest.mock import Mock, patch

import scotus_recent
from courtlistener_gui import (
    CourtListenerGUI,
    _ScholarTextWindow,
    _courtlistener_pdf_fallback_item,
    _scotus_slip_pdf_url,
)
from google_scholar import Block, Span


ARCHIVE_FIXTURE = """
<table>
  <tbody>
    <tr>
      <th>R-</th><th>Date</th><th>Docket</th><th>Name</th>
      <th>J.</th><th>Citation</th>
    </tr>
    <tr>
      <td>67</td>
      <td>6/30/25</td>
      <td>24-809</td>
      <td>
        <a href="/opinions/24pdf/606us2r67_8nka.pdf">Goldey v. Fields</a>
      </td>
      <td>PC</td>
      <td>606 U.S. 942</td>
    </tr>
    <tr>
      <td>29</td>
      <td>4/29/25</td>
      <td>23-715</td>
      <td>
        <a href="/opinions/24pdf/604us2r29_8nj9.pdf">
          Advocate Christ Medical Center v. Kennedy
        </a>
        <span>Revisions:
          <a href="/opinions/24pdf/604us2r29_revision.pdf">5/02/25</a>
        </span>
      </td>
      <td>AB</td>
      <td>605 U.S. 1</td>
    </tr>
  </tbody>
</table>
"""


class SlipArchiveParsingTests(unittest.TestCase):
    def test_archive_rows_expose_matching_metadata_and_main_pdf(self):
        opinions = scotus_recent.parse_slip_opinions(
            ARCHIVE_FIXTURE, "24"
        )

        self.assertEqual(len(opinions), 2)
        self.assertEqual(opinions[0].date, "2025-06-30")
        self.assertEqual(opinions[0].docket, "24-809")
        self.assertEqual(opinions[0].citation, "606 U.S. 942")
        self.assertEqual(
            opinions[0].opinion_url,
            "https://www.supremecourt.gov/opinions/"
            "24pdf/606us2r67_8nka.pdf",
        )
        self.assertEqual(
            opinions[1].opinion_url,
            "https://www.supremecourt.gov/opinions/"
            "24pdf/604us2r29_8nj9.pdf",
        )

    def test_exact_docket_and_date_match_across_unicode_dash(self):
        record = scotus_recent.SlipOpinion(
            term="20",
            release="35",
            date="2021-06-23",
            docket="20-107",
            name="Cedar Point Nursery v. Hassid",
            opinion_url=(
                "https://www.supremecourt.gov/opinions/"
                "20pdf/20-107_ihdj.pdf"
            ),
            citation="594 U.S. 139",
        )

        with patch(
            "scotus_recent.fetch_slip_opinions",
            side_effect=lambda term, **_kw: [record] if term == "20" else [],
        ):
            match = scotus_recent.find_slip_opinion(
                name="Cedar Point Nursery v. Hassid",
                docket="No. 20–107",
                date_filed="2021-06-23",
            )

        self.assertIs(match, record)

    def test_exact_docket_is_rejected_when_decision_dates_differ(self):
        record = scotus_recent.SlipOpinion(
            term="24",
            release="67",
            date="2025-06-30",
            docket="24-809",
            name="Goldey v. Fields",
            opinion_url=(
                "https://www.supremecourt.gov/opinions/"
                "24pdf/606us2r67_8nka.pdf"
            ),
            citation="606 U.S. 942",
        )

        with patch(
            "scotus_recent.fetch_slip_opinions",
            side_effect=lambda term, **_kw: [record] if term == "24" else [],
        ):
            match = scotus_recent.find_slip_opinion(
                name="Goldey v. Fields",
                docket="24-809",
                date_filed="2025-07-01",
                citations=("606 U.S. 942",),
            )

        self.assertIsNone(match)

    def test_name_without_year_or_docket_is_not_guessed(self):
        with patch("scotus_recent.fetch_slip_opinions") as fetch:
            match = scotus_recent.find_slip_opinion(
                name="Smith v. United States"
            )

        self.assertIsNone(match)
        fetch.assert_not_called()

    def test_name_and_exact_decision_date_can_match_without_docket(self):
        record = scotus_recent.SlipOpinion(
            term="24",
            release="67",
            date="2025-06-30",
            docket="24-809",
            name="Goldey v. Fields",
            opinion_url=(
                "https://www.supremecourt.gov/opinions/"
                "24pdf/606us2r67_8nka.pdf"
            ),
            citation="606 U.S. 942",
        )

        with patch(
            "scotus_recent.fetch_slip_opinions",
            side_effect=lambda term, **_kw: [record] if term == "24" else [],
        ):
            match = scotus_recent.find_slip_opinion(
                name="Goldey v. Fields",
                date_filed="2025-06-30",
            )

        self.assertIs(match, record)

    def test_year_only_date_does_not_probe_archive(self):
        with patch("scotus_recent.fetch_slip_opinions") as fetch:
            match = scotus_recent.find_slip_opinion(
                name="Goldey v. Fields",
                docket="24-809",
                date_filed="2025",
            )

        self.assertIsNone(match)
        fetch.assert_not_called()

    def test_exact_us_reports_citation_matches_without_name(self):
        record = scotus_recent.SlipOpinion(
            term="24",
            release="67",
            date="2025-06-30",
            docket="24-809",
            name="Goldey v. Fields",
            opinion_url=(
                "https://www.supremecourt.gov/opinions/"
                "24pdf/606us2r67_8nka.pdf"
            ),
            citation="606 U.S. 942",
        )

        with patch(
            "scotus_recent.fetch_slip_opinions",
            side_effect=lambda term, **_kw: [record] if term == "24" else [],
        ):
            match = scotus_recent.find_slip_opinion(
                date_filed="2025-06-30",
                citations=("606 U. S. 942",),
            )

        self.assertIs(match, record)


class SlipArchiveResolverTests(unittest.TestCase):
    def test_saved_scholar_window_supplies_exact_date_court_and_docket(self):
        window = object.__new__(_ScholarTextWindow)
        window._item = {"dateFiled": "2021-06-21"}
        window._bb = {
            "name": "Cedar Point Nursery v. Hassid",
            "cite": "594 U.S. 139",
            "year": "2021",
        }
        window._is_scotus = True
        window._header_cites = []
        window._blocks = [
            Block(
                kind="center",
                spans=[Span(text="No. 20–107. Decided June 23, 2021.")],
            )
        ]

        item = window._pdf_item()

        self.assertEqual(item["court_id"], "scotus")
        self.assertEqual(item["dateFiled"], "2021-06-23")
        self.assertEqual(item["docketNumber"], "20-107")

    def test_recent_scotus_helper_returns_archive_match(self):
        record = scotus_recent.SlipOpinion(
            term="24", release="67", date="2025-06-30",
            docket="24-809", name="Goldey v. Fields",
            opinion_url=(
                "https://www.supremecourt.gov/opinions/"
                "24pdf/606us2r67_8nka.pdf"
            ),
            citation="606 U.S. 942",
        )
        item = {
            "court_id": "scotus",
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
            "docketNumber": "24-809",
        }

        with patch(
            "scotus_recent.find_slip_opinion", return_value=record
        ) as find:
            url = _scotus_slip_pdf_url(item, ["606 U.S. 942"])

        self.assertEqual(url, record.opinion_url)
        self.assertEqual(find.call_args.kwargs["docket"], "24-809")
        self.assertEqual(
            find.call_args.kwargs["citations"], ("606 U.S. 942",)
        )

    def test_slip_helper_requires_an_exact_decision_date(self):
        item = {
            "court_id": "scotus",
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025",
            "docketNumber": "24-809",
        }

        with patch("scotus_recent.find_slip_opinion") as find:
            url = _scotus_slip_pdf_url(item, ["606 U.S. 942"])

        self.assertIsNone(url)
        find.assert_not_called()

    def test_pre_2021_case_does_not_probe_slip_archive(self):
        item = {
            "court_id": "scotus",
            "caseName": "Older Case v. United States",
            "dateFiled": "2020-06-01",
            "docketNumber": "19-123",
        }

        with patch("scotus_recent.find_slip_opinion") as find:
            url = _scotus_slip_pdf_url(item, [])

        self.assertIsNone(url)
        find.assert_not_called()

    def test_courtlistener_name_fallback_requires_date_and_docket(self):
        item = {
            "court_id": "scotus",
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
            "docketNumber": "24-809",
        }
        correct = {
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
            "docketNumber": "24-809",
        }
        candidates = [
            {
                "caseName": "Goldey v. Fields",
                "dateFiled": "2024-06-30",
                "docketNumber": "23-809",
            },
            {
                "caseName": "Goldey v. Fields",
                "dateFiled": "2025-07-01",
                "docketNumber": "24-809",
            },
            {
                "caseName": "Goldey v. Fields",
                "dateFiled": "2025-06-30",
                "docketNumber": "24-810",
            },
            correct,
        ]

        with patch(
            "courtlistener_gui._cl_name_search",
            return_value=candidates,
        ):
            matched = _courtlistener_pdf_fallback_item(
                Mock(), item, [],
            )

        self.assertIs(matched, correct)

    def test_courtlistener_citation_fallback_rejects_different_date(self):
        item = {
            "court_id": "scotus",
            "dateFiled": "2025-07-01",
        }
        merits_opinion = {
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
            "docketNumber": "24-809",
        }

        with patch(
            "courtlistener_gui._cl_item_for_citation",
            return_value=merits_opinion,
        ):
            matched = _courtlistener_pdf_fallback_item(
                Mock(), item, ["606 U.S. 942"],
            )

        self.assertIsNone(matched)

    def test_resolver_uses_archive_only_after_existing_sources_miss(self):
        resolver = object.__new__(CourtListenerGUI)
        slip_url = (
            "https://www.supremecourt.gov/opinions/"
            "24pdf/606us2r67_8nka.pdf"
        )
        item = {
            "court_id": "scotus",
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
        }

        with (
            patch("courtlistener_gui._gather_all_citations", return_value=[]),
            patch(
                "courtlistener_gui._scotus_slip_pdf_url",
                return_value=slip_url,
            ) as slip,
            patch(
                "courtlistener_gui._courtlistener_pdf_fallback_item"
            ) as cl_fallback,
        ):
            resolved = resolver._resolve_pdf_url(None, item)

        self.assertEqual(resolved, slip_url)
        slip.assert_called_once()
        cl_fallback.assert_not_called()

    def test_existing_download_url_keeps_priority_over_archive(self):
        resolver = object.__new__(CourtListenerGUI)
        existing = "https://example.test/opinion.pdf"
        response = Mock(status_code=200)
        item = {
            "court_id": "scotus",
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
            "download_url": existing,
        }

        with (
            patch("courtlistener_gui._gather_all_citations", return_value=[]),
            patch("courtlistener_gui._anon_session.head", return_value=response),
            patch("courtlistener_gui._scotus_slip_pdf_url") as slip,
        ):
            resolved = resolver._resolve_pdf_url(None, item)

        self.assertEqual(resolved, existing)
        slip.assert_not_called()

    def test_courtlistener_metadata_match_is_final_fallback(self):
        resolver = object.__new__(CourtListenerGUI)
        courtlistener_url = (
            "https://storage.courtlistener.com/pdf/2025/example.pdf"
        )
        sparse = {
            "court_id": "scotus",
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
        }
        matched = {
            "court_id": "scotus",
            "caseName": "Goldey v. Fields",
            "dateFiled": "2025-06-30",
            "download_url": courtlistener_url,
        }
        session = Mock()
        session.headers = {}
        session.head.return_value = Mock(status_code=200)
        client = Mock(_session=session)

        with (
            patch("courtlistener_gui._gather_all_citations", return_value=[]),
            patch(
                "courtlistener_gui._scotus_slip_pdf_url", return_value=None
            ),
            patch(
                "courtlistener_gui._courtlistener_pdf_fallback_item",
                return_value=matched,
            ) as fallback,
        ):
            resolved = resolver._resolve_pdf_url(client, sparse)

        self.assertEqual(resolved, courtlistener_url)
        fallback.assert_called_once()


if __name__ == "__main__":
    unittest.main()
