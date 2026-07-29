"""Regression tests for eCFR subsection structure and indentation."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import ecfr
from us_code import CFR_HIERARCHY, infer_enum_level


class EcfrStructureTests(unittest.TestCase):
    _SITE_HTML = """
    <div class="section" id="420.304">
      <h4>§ 420.304 Procedures for obtaining access.</h4>
      <p class="indent-1" data-title="420.304(a)">
        <span class="paragraph-hierarchy">(a)</span>
        <em class="paragraph-heading">Contents of the request.</em>
        Requests must be in writing.
      </p>
      <p class="indent-2" data-title="420.304(a)(1)">
        <span class="paragraph-hierarchy">(1)</span>
        First requirement.
      </p>
      <p class="indent-3" data-title="420.304(a)(1)(i)">
        <span class="paragraph-hierarchy">(i)</span>
        <strong>Mandatory text.</strong>
      </p>
    </div>
    """

    def test_site_html_preserves_authoritative_indent_and_emphasis(self):
        paras, styles = ecfr.parse_section_html(
            self._SITE_HTML, "420.304",
        )

        self.assertEqual(
            [indent for kind, indent, _text in paras if kind == "body"],
            [0, 1, 2],
        )
        self.assertEqual(
            paras[1][2],
            "(a) Contents of the request. Requests must be in writing.",
        )
        italic = styles[1][0]
        self.assertEqual(italic[2], "italic")
        self.assertEqual(
            paras[1][2][italic[0]:italic[1]],
            "Contents of the request.",
        )
        bold = styles[3][0]
        self.assertEqual(bold[2], "bold")
        self.assertEqual(
            paras[3][2][bold[0]:bold[1]],
            "Mandatory text.",
        )

    def test_load_section_prefers_site_formatting_and_keeps_xml_credit(self):
        xml = b"""
        <DIV8 N="420.304" TYPE="SECTION">
          <HEAD>Section heading from XML.</HEAD>
          <P>(a) Body from XML.</P>
          <CITA>[50 FR 12345]</CITA>
        </DIV8>
        """

        class Response:
            status_code = 200

            def __init__(self, *, text="", content=b""):
                self.text = text
                self.content = content

            def raise_for_status(self):
                return None

        def fake_get(url, **_kwargs):
            if "/current/" in url:
                return Response(text=self._SITE_HTML)
            return Response(content=xml)

        key = ("42", "420.304")
        ecfr._cache.pop(key, None)
        fake_requests = SimpleNamespace(get=fake_get)
        try:
            with patch.object(ecfr, "_issue_date", return_value="2026-07-27"):
                with patch.dict(sys.modules, {"requests": fake_requests}):
                    doc = ecfr.load_section(*key)
        finally:
            ecfr._cache.pop(key, None)

        self.assertTrue(doc.site_formatting)
        self.assertEqual(
            [indent for kind, indent, _text in doc.paras if kind == "body"],
            [0, 1, 2],
        )
        self.assertEqual(doc.paras[-1], ("credit", 0, "50 FR 12345"))
        self.assertEqual(doc.para_styles[1][0][2], "italic")

    def test_stacked_opening_markers_keep_following_siblings_nested(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <DIV8 N="1.36B-2" TYPE="SECTION">
          <HEAD>§ 1.36B-2 Eligibility for premium tax credit.</HEAD>
          <P>(a) <I>In general.</I> Introductory text—</P>
          <P>(1) First requirement; and</P>
          <P>(2) Second requirement.</P>
          <P>(b) <I>Applicable taxpayer</I>—(1) <I>In general.</I> Text.</P>
          <P>(2) <I>Joint return</I>—(i) <I>In general.</I> Text.</P>
          <P>(ii) <I>Victims.</I> Text—</P>
          <P>(A) First condition;</P>
          <P>(B) Second condition; and</P>
          <P>(iii) <I>Domestic abuse.</I> Text.</P>
        </DIV8>"""

        body = [(indent, text) for kind, indent, text
                in ecfr.parse_section_xml(xml) if kind == "body"]

        self.assertEqual([indent for indent, _text in body],
                         [0, 1, 1, 0, 1, 2, 3, 3, 2])
        self.assertEqual(
            ecfr._structural_enums(
                "(b) Applicable taxpayer—(1) In general. Text.",
            ),
            ["b", "1"],
        )

    def test_deep_treasury_hierarchy_repeats_arabic_and_roman_levels(self):
        xml = """<DIV8 N="1.36B-2" TYPE="SECTION">
          <HEAD>§ 1.36B-2 Eligibility.</HEAD>
          <P>(c) Coverage—(3) Employer coverage—(v) Affordable coverage—(A)
             In general—(<I>1</I>) Affordability for employee.</P>
          <P>(<I>2</I>) Affordability for related individual.</P>
          <P>(<I>6</I>) Cafeteria plans—</P>
          <P>(<I>i</I>) First requirement;</P>
          <P>(<I>ii</I>) Second requirement.</P>
        </DIV8>"""

        levels = [indent for kind, indent, _text
                  in ecfr.parse_section_xml(xml) if kind == "body"]

        self.assertEqual(levels, [0, 4, 4, 5, 5])
        self.assertEqual(
            ecfr._structural_enums(
                "(c) Coverage—(3) Employer coverage—(v) Affordable—"
                "(A) In general—(1) Employee.",
            ),
            ["c", "3", "v", "A", "1"],
        )
        self.assertEqual(CFR_HIERARCHY, ("a", "1", "i", "A", "1", "i"))

    def test_example_blocks_are_preserved_without_closing_main_stack(self):
        xml = """<DIV8 N="1.36B-2" TYPE="SECTION">
          <HEAD>§ 1.36B-2 Eligibility.</HEAD>
          <P>(c) Coverage—(2) Government coverage—(vi) <I>Examples.</I></P>
          <EXAMPLE>
            <HED>Example 1. Delay in coverage effectiveness.</HED>
            <PSPACE>Taxpayer D applies for coverage.</PSPACE>
          </EXAMPLE>
          <P>(vii) <I>Next subdivision.</I> The main hierarchy continues.</P>
          <CITA>[T.D. 9590, 77 FR 30385]</CITA>
        </DIV8>"""

        paras = ecfr.parse_section_xml(xml)

        self.assertIn(("head", 3,
                       "Example 1. Delay in coverage effectiveness."), paras)
        self.assertIn(("body", 4,
                       "Taxpayer D applies for coverage."), paras)
        self.assertIn(("body", 2,
                       "(vii) Next subdivision. The main hierarchy continues."),
                      paras)
        self.assertIn(("credit", 0, "T.D. 9590, 77 FR 30385"), paras)


class CfrHierarchyTests(unittest.TestCase):
    def test_deep_sequence_infers_all_six_levels(self):
        stack = []
        levels = [
            infer_enum_level([enum], stack, CFR_HIERARCHY)
            for enum in ("a", "1", "i", "A", "1", "i")
        ]
        self.assertEqual(levels, [0, 1, 2, 3, 4, 5])


if __name__ == "__main__":
    unittest.main()
