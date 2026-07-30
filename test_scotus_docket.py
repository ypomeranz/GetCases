import unittest
from pathlib import Path

import scotus_docket


OFFICIAL_FIXTURE = """
<table>
  <tr>
    <td class="ProceedingDate">Oct 11 2022</td>
    <td>
      Application (22A100) to extend the time to file a petition for a writ
      of certiorari.
      <span class="documentlinks">
        <a href="https://court.test/extension.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Nov 10 2022</td>
    <td>
      Petition for a writ of certiorari filed. (Response due December 15, 2022)
      <span class="documentlinks">
        <a href="https://court.test/petition.pdf">Petition</a>
        <a href="https://court.test/appendix.pdf">Appendix</a>
        <a href="https://court.test/certificate.pdf">
          Certificate of Word Count
        </a>
        <a href="https://court.test/proof.pdf">Proof of Service</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Dec 6 2022</td>
    <td>
      Brief amicus curiae of New England Legal Foundation filed.
      <span class="documentlinks">
        <a href="https://court.test/cert-amicus.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Jan 17 2023</td>
    <td>
      Brief of respondents Gina Raimondo, et al. in opposition filed.
      <span class="documentlinks">
        <a href="https://court.test/opposition.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Mar 3 2023</td>
    <td>
      Reply of petitioners Loper Bright Enterprises, et al. filed. (Distributed)
      <span class="documentlinks">
        <a href="https://court.test/cert-reply.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">May 1 2023</td>
    <td>Petition GRANTED.</td>
  </tr>
  <tr>
    <td class="ProceedingDate">Jul 24 2023</td>
    <td>
      Joint appendix filed.
      <span class="documentlinks">
        <a href="https://court.test/joint-appendix.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Jul 24 2023</td>
    <td>
      Brief of petitioners Loper Bright Enterprises, et al. filed.
      <span class="documentlinks">
        <a href="https://court.test/petitioner.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Jul 31 2023</td>
    <td>
      Brief amici curiae of Cato Institute, et al. filed.
      <span class="documentlinks">
        <a href="https://court.test/petitioner-amicus.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Sep 22 2023</td>
    <td>
      Brief of respondents Gina Raimondo, et al. filed.
      <span class="documentlinks">
        <a href="https://court.test/respondent.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Sep 29 2023</td>
    <td>
      Brief amicus curiae of Public Citizen filed.
      <span class="documentlinks">
        <a href="https://court.test/respondent-amicus.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Oct 20 2023</td>
    <td>
      Reply of petitioners Loper Bright Enterprises, et al. filed.
      <span class="documentlinks">
        <a href="https://court.test/merits-reply.pdf">Main Document</a>
      </span>
    </td>
  </tr>
  <tr>
    <td class="ProceedingDate">Jan 17 2024</td>
    <td>
      Transcript of oral argument filed.
      <span class="documentlinks">
        <a href="https://court.test/transcript.pdf">Main Document</a>
      </span>
    </td>
  </tr>
</table>
"""


ARCHIVE_FIXTURE = """
<main>
  <div class="prose max-w-none">
    <h4>Supplemental Merits Briefs</h4>
    <ul>
      <li><a href="/files/appellant-supp.pdf">
        Supplemental brief of appellant Citizens United
      </a></li>
      <li><a href="/files/appellee-supp.pdf">
        Supplemental brief of appellee Federal Election Commission
      </a></li>
      <li><a href="/files/reply-supp.pdf">
        Supplemental reply brief of appellant Citizens United
      </a></li>
    </ul>
    <h4>Supplemental Amicus Briefs</h4>
    <p><em>Neither party</em></p>
    <ul>
      <li><a href="/files/neither.pdf">
        Brief of Independent Sector in Support of Neither Party
      </a></li>
    </ul>
    <p><em>Appellee</em></p>
    <ul>
      <li><a href="/files/appellee-amicus.pdf">
        Brief Amicus Curiae of Campaign Legal Center
      </a></li>
    </ul>
    <h4>Merits Briefs</h4>
    <ul>
      <li><a href="/files/appellant.pdf">Brief for Appellant Citizens United</a></li>
      <li><a href="/files/appellee.pdf">Brief for Appellee United States</a></li>
      <li><a href="/files/reply.pdf">Reply Brief for Appellant Citizens United</a></li>
    </ul>
    <h4>Cert Stage</h4>
    <ul>
      <li><a href="/files/jurisdiction.pdf">
        Jurisdictional Statement of Citizens United
      </a></li>
      <li><a href="/files/cert-appendix.pdf">
        Appendix to Jurisdictional Statement
      </a></li>
      <li><a href="http://not-a-real-document">
        Amicus Curiae Brief with a broken migrated URL
      </a></li>
      <li><a href="/files/dismiss.pdf">FEC's Motion to Dismiss or Affirm</a></li>
      <li><a href="/files/extension.pdf">
        Motion for extension of time to file jurisdictional statement
      </a></li>
    </ul>
    <h2>Links and Further Information</h2>
    <ul><li><a href="/news.pdf">Unrelated news</a></li></ul>
  </div>
</main>
"""


CURRENT_BLOG_FIXTURE = """
<main>
  <a href="https://court.test/petition.pdf"
     class="bg-brief-white flex items-center">
    <span>Nov 10, 2022</span>
    <span>Petition for a writ of certiorari filed.</span>
  </a>
  <div class="bg-brief-white flex items-center">
    <span>May 1, 2023</span><span>Petition GRANTED.</span>
  </div>
  <a href="https://blog.test/merits.pdf"
     class="bg-brief-light-blue flex items-center">
    <span>Jul 24, 2023</span>
    <span>Brief of petitioners on the merits filed.</span>
  </a>
  <a href="https://www.scotusblog.com/cases/example/"
     class="bg-brief-cream flex items-center">
    <span>Jul 25, 2023</span>
    <span>Motion for leave to file amicus brief not accepted for filing.
      (Duplicate submission)</span>
  </a>
  <a href="https://court.test/Certificate%20of%20Compliance.pdf"
     class="bg-brief-dark-green flex items-center">
    <span>Sep 29, 2023</span>
    <span>Brief amicus curiae of Public Citizen filed.</span>
  </a>
  <a href="https://www.supremecourt.gov/oral_arguments/argument_transcripts/2023/22-451_114p.pdf"></a>
</main>
"""


class OfficialDocketParsingTests(unittest.TestCase):
    def test_filters_procedural_filings_and_keeps_requested_documents(self):
        documents = scotus_docket.parse_official_docket(
            OFFICIAL_FIXTURE, "22-451"
        )

        self.assertEqual(len(documents), 12)
        self.assertEqual(
            [document.kind for document in documents[:5]],
            [
                "cert_petition",
                "cert_appendix",
                "cert_amicus",
                "cert_opposition",
                "cert_reply",
            ],
        )
        self.assertEqual(documents[0].cover, "white")
        self.assertEqual(documents[1].label, "Appendix to petition for a writ of certiorari")
        self.assertEqual(documents[2].cover, "cream")
        self.assertEqual(documents[3].cover, "orange")
        self.assertEqual(documents[4].cover, "tan")
        self.assertNotIn("extension.pdf", {document.url for document in documents})
        self.assertNotIn("certificate.pdf", {document.url for document in documents})
        self.assertNotIn("proof.pdf", {document.url for document in documents})
        transcript = documents[-1]
        self.assertEqual(transcript.kind, "oral_argument_transcript")
        self.assertEqual(transcript.cover, "plain")
        self.assertEqual(transcript.url, "https://court.test/transcript.pdf")

    def test_merits_colors_follow_party_and_filing_sequence(self):
        documents = scotus_docket.parse_official_docket(
            OFFICIAL_FIXTURE, "22-451"
        )
        colors = {document.kind: document.cover for document in documents}

        self.assertEqual(colors["joint_appendix"], "tan")
        self.assertEqual(colors["merits_petitioner"], "light_blue")
        self.assertEqual(colors["merits_amicus_petitioner"], "light_green")
        self.assertEqual(colors["merits_respondent"], "light_red")
        self.assertEqual(colors["merits_amicus_respondent"], "dark_green")
        self.assertEqual(colors["merits_reply"], "yellow")


class SCOTUSblogParsingTests(unittest.TestCase):
    def test_finds_only_the_exact_docket_case_result(self):
        search_html = """
        <a href="/cases/smith-v-united-states/">
          Smith v. United States No. 21-100
        </a>
        <a href="/cases/loper-bright-enterprises-v-raimondo/">
          Loper Bright Enterprises v. Raimondo No. 22-451 · OT2023
        </a>
        """

        url = scotus_docket.find_scotusblog_case_url(search_html, "22\u2013451")

        self.assertEqual(
            url,
            "https://www.scotusblog.com/cases/"
            "loper-bright-enterprises-v-raimondo/",
        )

    def test_finds_exact_docket_in_client_search_payload(self):
        payload = {
            "hits": [
                {"document": {
                    "docket_number": "21-100",
                    "slug": "smith-v-united-states",
                }},
                {"document": {
                    "docket_number": "08-205",
                    "docket_number_compact": "08205",
                    "slug": "citizens-united-v-federal-election-commission",
                }},
            ]
        }

        url = scotus_docket.find_scotusblog_case_url_json(
            payload, "08\u2011205"
        )

        self.assertEqual(
            url,
            "https://www.scotusblog.com/cases/"
            "citizens-united-v-federal-election-commission/",
        )

    def test_parses_archived_headed_brief_lists_and_side_hints(self):
        documents = scotus_docket.parse_scotusblog_case(
            ARCHIVE_FIXTURE,
            "https://www.scotusblog.com/cases/citizens-united/",
            "08-205",
        )
        by_name = {document.url.rsplit("/", 1)[-1]: document for document in documents}

        self.assertEqual(by_name["jurisdiction.pdf"].stage, "cert")
        self.assertEqual(by_name["jurisdiction.pdf"].cover, "white")
        self.assertEqual(by_name["cert-appendix.pdf"].kind, "cert_appendix")
        self.assertEqual(
            by_name["cert-appendix.pdf"].label,
            "Appendix to jurisdictional statement",
        )
        self.assertEqual(by_name["dismiss.pdf"].cover, "orange")
        self.assertEqual(by_name["appellant-supp.pdf"].cover, "light_blue")
        self.assertEqual(by_name["appellee-supp.pdf"].cover, "light_red")
        self.assertEqual(by_name["reply-supp.pdf"].cover, "yellow")
        self.assertEqual(by_name["neither.pdf"].cover, "light_green")
        self.assertEqual(by_name["appellee-amicus.pdf"].cover, "dark_green")
        self.assertNotIn("extension.pdf", by_name)
        self.assertNotIn("news.pdf", by_name)
        self.assertNotIn("not-a-real-document", by_name)
        self.assertEqual(documents[0].stage, "cert")

    def test_parses_current_timeline_and_ignores_non_document_rows(self):
        documents = scotus_docket.parse_scotusblog_case(
            CURRENT_BLOG_FIXTURE,
            "https://www.scotusblog.com/cases/example/",
            "22-451",
        )

        self.assertEqual(len(documents), 3)
        self.assertEqual(documents[0].kind, "cert_petition")
        self.assertEqual(documents[0].date, "Nov 10, 2022")
        self.assertEqual(documents[1].kind, "merits_petitioner")
        self.assertEqual(documents[2].kind, "oral_argument_transcript")
        self.assertEqual(documents[2].cover, "plain")
        self.assertIn("/argument_transcripts/", documents[2].url)


class _Response:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def json(self):
        return self.text if isinstance(self.text, dict) else {}


class _Session:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, timeout, headers=None):
        self.calls.append((url, timeout, headers))
        return _Response(self.responses.get(url, ""), 200)


class DocketFetchAndPanelTests(unittest.TestCase):
    def test_merges_sources_and_deduplicates_the_same_court_pdf(self):
        official_url = scotus_docket.official_docket_url("22-451")
        search_url = scotus_docket.SCOTUSBLOG_SEARCH_URL.format(query="22-451")
        blog_url = "https://www.scotusblog.com/cases/loper-bright/"
        search_html = (
            f'<a href="{blog_url}">Loper Bright v. Raimondo No. 22-451</a>'
        )
        session = _Session({
            official_url: OFFICIAL_FIXTURE,
            search_url: search_html,
            blog_url: CURRENT_BLOG_FIXTURE,
        })

        result = scotus_docket.fetch_case_docket("22-451", session=session)

        self.assertEqual(len(result.documents), 14)
        self.assertEqual(
            sum(
                document.url == "https://court.test/petition.pdf"
                for document in result.documents
            ),
            1,
        )
        self.assertEqual(result.scotusblog_url, blog_url)
        self.assertTrue(all(
            document.stage == "cert"
            for document in result.documents[:5]
        ))

    def test_right_panel_exposes_docket_mode_and_cover_color_tags(self):
        source = Path("courtlistener_gui.py").read_text(encoding="utf-8")

        self.assertIn('mode_values.append("Docket")', source)
        self.assertIn('if sel == "Docket":', source)
        self.assertIn('elif mode == "docket":', source)
        self.assertIn('f"docket_{cover}"', source)
        self.assertIn('f"docket_{document.cover}"', source)
        self.assertIn('"docket_plain", foreground="#25232e"', source)


if __name__ == "__main__":
    unittest.main()
