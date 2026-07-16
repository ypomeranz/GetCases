import unittest

from google_scholar import Block, Span, segment_blocks


def _block(text: str) -> Block:
    return Block(kind="para", spans=[Span(text=text)])


class JoinedSeparateOpinionTests(unittest.TestCase):
    def test_cohen_blackmun_joinder_byline_starts_dissent(self):
        parts = segment_blocks([
            _block("MR. JUSTICE HARLAN delivered the opinion of the Court."),
            _block("The judgment below is reversed."),
            _block(
                "MR. JUSTICE BLACKMUN, with whom THE CHIEF JUSTICE and "
                "MR. JUSTICE BLACK join."
            ),
            _block("I dissent, and I do so for two reasons:"),
            _block("Cohen's antic, in my view, was mainly conduct."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "dissent"])
        self.assertIn("BLACKMUN", parts[1].label)
        self.assertTrue(parts[1].blocks[0].text().startswith("MR. JUSTICE BLACKMUN"))

    def test_joined_byline_can_start_concurrence(self):
        parts = segment_blocks([
            _block("MR. JUSTICE SMITH delivered the opinion of the Court."),
            _block("The judgment is affirmed."),
            _block("JUSTICE JONES, with whom JUSTICE BROWN joins."),
            _block("I concur in the Court's judgment."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "concurrence"])

    def test_syllabus_announcement_is_not_a_separate_boundary(self):
        parts = segment_blocks([
            _block(
                "HARLAN, J., delivered the opinion of the Court. BLACKMUN, J., "
                "filed a dissenting opinion."
            ),
            _block("MR. JUSTICE HARLAN delivered the opinion of the Court."),
            _block("The judgment below is reversed."),
            _block(
                "MR. JUSTICE BLACKMUN, with whom THE CHIEF JUSTICE and "
                "MR. JUSTICE BLACK join."
            ),
            _block("I dissent, and I do so for two reasons:"),
        ])

        self.assertEqual(
            [part.kind for part in parts],
            ["header", "majority", "dissent"],
        )

    def test_vote_note_does_not_create_a_second_dissent(self):
        parts = segment_blocks([
            _block("MR. JUSTICE HARLAN delivered the opinion of the Court."),
            _block("The judgment below is reversed."),
            _block(
                "MR. JUSTICE BLACKMUN, with whom THE CHIEF JUSTICE and "
                "MR. JUSTICE BLACK join."
            ),
            _block("I dissent, and I do so for two reasons:"),
            _block(
                "MR. JUSTICE WHITE concurs in Paragraph 2 of MR. JUSTICE "
                "BLACKMUN'S dissenting opinion."
            ),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "dissent"])
        self.assertIn("WHITE concurs", parts[1].blocks[-1].text())


class SpelledOutRoleBylineTests(unittest.TestCase):
    def test_alabama_style_justice_bylines_start_separate_opinions(self):
        # Ex parte Murphy, 886 So. 2d 90 (Ala. 2003): the role is spelled
        # out after the name and the vote is a parenthetical — "LYONS,
        # Justice (dissenting)." — while the vote lines above it ("LYONS,
        # J., dissents.") are not boundaries.
        parts = segment_blocks([
            _block("STUART, Justice."),
            _block("James R. Murphy and Mary J. Murphy Benvenuto divorced."),
            _block("HOUSTON, SEE, BROWN, and WOODALL, JJ., concur."),
            _block(
                "HARWOOD, J., concurs in the rationale in part and concurs "
                "in the result."
            ),
            _block("LYONS, J., dissents."),
            _block(
                "HARWOOD, Justice (concurring in the rationale in part and "
                "concurring in the result)."
            ),
            _block("I concur in all aspects of the opinion except one."),
            _block("LYONS, Justice (dissenting)."),
            _block("I must respectfully dissent."),
        ])

        self.assertEqual(
            [part.kind for part in parts],
            ["majority", "concurrence", "dissent"],
        )
        self.assertIn("HARWOOD", parts[1].label)
        self.assertIn("LYONS", parts[2].label)
        # The vote lines stay with the majority opinion.
        self.assertIn("JJ., concur", parts[0].blocks[-3].text())


if __name__ == "__main__":
    unittest.main()
