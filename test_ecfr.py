"""Regression tests for eCFR subsection structure and indentation."""

import unittest

import ecfr
from us_code import CFR_HIERARCHY, infer_enum_level


class EcfrStructureTests(unittest.TestCase):
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
