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


class TitleCommaBylineTests(unittest.TestCase):
    def test_stray_comma_after_justice_still_bounds_opinions(self):
        # Alleyne v. United States, 570 U.S. 99 (2013): Scholar's text
        # prints "Justice, BREYER," / "Chief Justice, ROBERTS," — a stray
        # comma between title and name that must not hide the bylines.
        parts = segment_blocks([
            _block("Justice THOMAS announced the judgment of the Court."),
            _block("Mandatory minimum sentences increase the penalty."),
            _block("Justice SOTOMAYOR, with whom Justice GINSBURG and "
                   "Justice KAGAN join, concurring."),
            _block("I join the opinion of the Court in full."),
            _block("Justice, BREYER, concurring in part and concurring in "
                   "the judgment."),
            _block("I cannot accept the majority's rationale."),
            _block("Chief Justice, ROBERTS, with whom Justice, SCALIA and "
                   "Justice, KENNEDY join, dissenting."),
            _block("I would adhere to our precedent."),
            _block("Justice, ALITO, dissenting."),
            _block("The Court overrules a well-entrenched case."),
        ])

        self.assertEqual(
            [part.kind for part in parts],
            ["majority", "concurrence", "concurrence", "dissent", "dissent"],
        )
        self.assertIn("BREYER", parts[2].label)
        self.assertIn("ROBERTS", parts[3].label)

    def test_comma_after_title_in_majority_attribution(self):
        # Armstrong v. Exceptional Child Ctr.: "Justice, SCALIA, delivered
        # the opinion of the Court, except as to Part IV."
        parts = segment_blocks([
            _block("Justice, SCALIA, delivered the opinion of the Court, "
                   "except as to Part IV."),
            _block("Medicaid is a federal-state program."),
            _block("Justice, SOTOMAYOR, with whom Justice KENNEDY joins, "
                   "dissenting."),
            _block("I respectfully dissent from the Court's holding."),
        ])

        self.assertEqual([part.kind for part in parts],
                         ["majority", "dissent"])


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


class GluedDispositionBylineTests(unittest.TestCase):
    def test_disposition_glued_to_byline_without_period(self):
        # King v. Burwell, 759 F.3d 358 (4th Cir. 2014): Scholar glues the
        # mandate to the concurrence byline with no period — "AFFIRMED
        # DAVIS, Senior Circuit Judge, concurring:".
        parts = segment_blocks([
            _block("GREGORY, Circuit Judge:"),
            _block("The plaintiffs challenge the IRS rule."),
            _block("AFFIRMED DAVIS, Senior Circuit Judge, concurring:"),
            _block("I write separately because the statute is clear."),
        ])

        self.assertEqual([p.kind for p in parts], ["majority", "concurrence"])
        self.assertTrue(
            parts[1].blocks[0].text().startswith("DAVIS"),
            parts[1].blocks[0].text(),
        )
        # The mandate stays with the majority opinion.
        self.assertIn("AFFIRMED", parts[0].blocks[-1].text())

    def test_disposition_blocks_do_not_become_phantom_parts(self):
        # Rothery Storage & Van Co. v. Atlas Van Lines, 792 F.2d 210 (D.C.
        # Cir. 1986): the disposition sentences precede the byline as their
        # own blocks; the 3-block joined window must not turn them into
        # phantom one-line concurrences.
        parts = segment_blocks([
            _block("BORK, Circuit Judge:"),
            _block("The antitrust claims fail as a matter of law."),
            _block("The judgment of the district court is"),
            _block("Affirmed."),
            _block("WALD, Circuit Judge, concurring:"),
            _block("I concur in the result and in much of the reasoning."),
        ])

        self.assertEqual([p.kind for p in parts], ["majority", "concurrence"])
        self.assertTrue(parts[1].blocks[0].text().startswith("WALD"))

    def test_so_ordered_glued_byline_splits(self):
        # Lorenzo v. SEC, 872 F.3d 578 (D.C. Cir. 2017): "So ordered.
        # KAVANAUGH, Circuit Judge, dissenting:" shares one block.
        parts = segment_blocks([
            _block("SRINIVASAN, Circuit Judge:"),
            _block("Substantial evidence supports the Commission."),
            _block("So ordered. KAVANAUGH, Circuit Judge, dissenting:"),
            _block("The Commission overreached in this case."),
        ])

        self.assertEqual([p.kind for p in parts], ["majority", "dissent"])
        self.assertTrue(parts[1].blocks[0].text().startswith("KAVANAUGH"))

    def test_page_break_running_head_is_not_a_byline(self):
        # Intel Corp. v. AMD, 542 U.S. 241 (2004): the facing-page running
        # head "BREYER, J., dissenting" rides in on a page marker with no
        # terminal punctuation; the majority's closing lines must not become
        # a phantom dissent.
        blocks = [
            _block("JUSTICE GINSBURG delivered the opinion of the Court."),
            _block("Section 1782 authorizes the discovery."),
            Block(kind="para", spans=[
                Span(text="*267 ", pagenum=True),
                Span(text="BREYER, J., dissenting"),
            ]),
            _block("For the reasons stated, the judgment is Affirmed."),
            _block("JUSTICE BREYER, dissenting."),
            _block("I cannot agree with the Court's reading."),
        ]
        parts = segment_blocks(blocks)

        self.assertEqual([p.kind for p in parts], ["majority", "dissent"])
        self.assertTrue(parts[1].blocks[0].text().startswith("JUSTICE BREYER"))


class HistoricalAndSignatureBoundaryTests(unittest.TestCase):
    def test_unresolved_roleless_opinion_remains_neutral(self):
        parts = segment_blocks([
            _block("MR. JUSTICE MARSHALL delivered the opinion of the Court."),
            _block("The Court resolves the question presented."),
            _block("Separate opinion of MR. JUSTICE STORY."),
            _block("The historical materials point in several directions."),
            _block("The statutory language supplies another consideration."),
            _block("That is sufficient to resolve the issue before us."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "separate"])

    def test_long_roleless_opinion_uses_explicit_closing_dissent(self):
        # Osborn v. Bank of the United States, 22 U.S. (9 Wheat.) 738
        # (1824): Johnson's neutral heading does not identify his vote; his
        # conclusion does.
        parts = segment_blocks([
            _block("Mr. Chief Justice MARSHALL delivered the opinion of the Court."),
            _block("The decree of the Circuit Court is affirmed."),
            _block("Mr. Justice JOHNSON."),
            _block("The argument in this cause presents three questions."),
            _block("The first question concerns the statutory grant."),
            _block("The second concerns the constitutional power."),
            _block("The final question concerns the exercise of jurisdiction."),
            _block(
                "Upon the whole, I feel compelled to dissent from the Court, "
                "on the point of jurisdiction."
            ),
            _block("Decree affirmed."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "dissent"])
        self.assertIn("JOHNSON", parts[1].label.upper())

    def test_roleless_separate_opinion_of_headings_use_disposition_structure(self):
        # Steward Machine Co. v. Davis, 301 U.S. 548 (1937), gives
        # McReynolds and Sutherland noun-first headings with no role.  The
        # Court affirms; Sutherland would reverse and says he concurs with
        # McReynolds's immediately preceding writing.
        parts = segment_blocks([
            _block("MR. JUSTICE CARDOZO delivered the opinion of the Court."),
            _block("The judgment is"),
            _block("Affirmed."),
            _block("Separate opinion of MR. JUSTICE McREYNOLDS."),
            _block("The legislation, I think, exceeds the power of Congress."),
            _block("The federal plan imperils the independence of the States."),
            _block("Separate opinion of MR. JUSTICE SUTHERLAND."),
            _block(
                "With most of what is said in the opinion just handed down, "
                "I concur."
            ),
            _block("The administrative provisions invade reserved powers."),
            _block(
                "For the foregoing reasons, I think the judgment below "
                "should be reversed."
            ),
            _block("MR. JUSTICE BUTLER, dissenting."),
            _block("The objections in both separate opinions are well taken."),
        ])

        self.assertEqual(
            [part.kind for part in parts],
            ["majority", "dissent", "dissent", "dissent"],
        )
        self.assertIn("McREYNOLDS", parts[1].label)
        self.assertIn("SUTHERLAND", parts[2].label)

    def test_chief_justice_old_style_byline_opens_majority(self):
        # Fletcher v. Peck, 10 U.S. (6 Cranch) 87 (1810), uses the older
        # "Ch. J." title and gives Johnson's disagreement a bare byline.
        parts = segment_blocks([
            _block("Marshall, Ch. J."),
            _block("The question whether the legislature could repeal the act remains."),
            _block("Johnson, J."),
            _block(
                "In this case I entertain an opinion different from that "
                "which has been delivered by the court."
            ),
            _block("I therefore rest my opinion on another ground."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "dissent"])
        self.assertTrue(parts[0].blocks[0].text().startswith("Marshall"))
        self.assertTrue(parts[1].blocks[0].text().startswith("Johnson"))

    def test_repeated_court_opinion_is_not_called_a_concurrence(self):
        parts = segment_blocks([
            _block("Marshall, Ch. J."),
            _block("delivered the opinion of the court upon the pleadings, as follows:"),
            _block("The first issue is resolved."),
            _block("Marshall, Ch. J."),
            _block("delivered the opinion of the court as follows:"),
            _block("The amended pleadings present a second issue."),
            _block("Johnson, J."),
            _block("I entertain an opinion different from that delivered by the court."),
        ])

        self.assertEqual(
            [part.kind for part in parts], ["majority", "majority", "dissent"]
        )

    def test_ocr_seriatim_justice_bylines_remain_separate(self):
        parts = segment_blocks([
            _block("The Court,"),
            _block("delivered their opinions, feriatim, as follow:"),
            _block("Chace, Jujtice."),
            _block("I consider the first question."),
            _block("This writing continues for several paragraphs."),
            _block("Paterson, Juftke."),
            _block("My opinion rests on the treaty."),
            _block("This writing also continues."),
            _block("R~DELL,’7ujh~?~. *"),
            _block("I take a different route through the treaty."),
            _block("This seriatim writing continues."),
            _block("Wilson, JuJiice."),
            _block("I shall be concise in delivering my opinion."),
            _block("The judgment should be reversed."),
        ])

        self.assertEqual(
            [part.kind for part in parts],
            ["header", "majority", "majority", "majority", "majority"],
        )

    def test_vote_signatures_do_not_create_phantom_concurrences(self):
        # State v. Gregory, 427 P.3d 621 (Wash. 2018), ends the lead writing
        # and the true concurrence with several one-line vote signatures.
        parts = segment_blocks([
            _block("FAIRHURST, C.J."),
            _block("We hold the death penalty is unconstitutional as administered."),
            _block("WIGGINS, J."),
            _block("YU, J."),
            _block("JOHNSON, J."),
            _block("I concur in the result reached by the majority."),
            _block("The record supplies an additional ground."),
            _block("OWENS, J."),
            _block("MADSEN, J."),
            _block("STEPHENS, J."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "concurrence"])
        self.assertIn("JOHNSON", parts[1].label)
        self.assertIn("STEPHENS, J.", parts[1].blocks[-1].text())

    def test_public_domain_paragraph_number_does_not_hide_byline(self):
        parts = segment_blocks([
            _block("¶1 ANN WALSH BRADLEY, J. The question presented is narrow."),
            _block("¶2 We affirm the court of appeals."),
            _block(
                "¶73 PATIENCE DRAKE ROGGENSACK, J. (concurring). Although I "
                "agree with the mandate, I write separately."
            ),
            _block("¶74 The text supplies a different rationale."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "concurrence"])
        self.assertIn("ROGGENSACK", parts[1].label)

    def test_filing_banner_can_share_dissent_byline(self):
        parts = segment_blocks([
            _block("The emergency motion for a stay is granted."),
            _block("The order will remain in effect pending appeal."),
            _block(
                "6 FILED Duncan v. Bonta OCT 10 2023 MOLLY C. DWYER, CLERK "
                "R. NELSON, Circuit Judge, dissenting: U.S. COURT OF APPEALS"
            ),
            _block("I join Judge Bumatay's dissent."),
            _block(
                "2 FILED OCT 10 2023 MOLLY C. DWYER, CLERK BUMATAY, Circuit "
                "Judge, joined by IKUTA, R. NELSON, and"
            ),
            _block("VANDYKE, Circuit Judges, dissenting:"),
            _block("The majority applies the wrong standard."),
        ])

        self.assertEqual([part.kind for part in parts], ["majority", "dissent", "dissent"])


if __name__ == "__main__":
    unittest.main()
