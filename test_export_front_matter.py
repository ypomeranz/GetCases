"""The front matter a full-document export lifts into a section of its own.

A report prints a syllabus — or, in the early volumes, the reporter's
statement of the case and the arguments of counsel — ahead of the opinion of
the Court, and the export gives that material its own page and its own
running head.  What it must *not* do is promote an ordinary caption (docket
number, argument and decision dates, counsel and amici listings), which is
most of what sits above an opinion, or a header that segmentation has swept
the opinion itself into.

``_front_matter_section_label`` and ``_export_section_list`` are lifted out of
``courtlistener_gui`` with ``ast`` (it imports tkinter, absent on a headless
run) and driven against fake blocks and part regions.  The samples are the
shapes the real corpus has: Cedar Point's syllabus, Copperweld's four
thousand characters of counsel listings, Ramos' fractured-majority syllabus,
Dartmouth College's swept-in opinion, De Lima's arguments of counsel.
"""

import ast
import pathlib
import re
import typing
import unittest


def _load(fns, methods_of=None, consts=()):
    src = pathlib.Path(__file__).with_name("courtlistener_gui.py").read_text()
    tree = ast.parse(src)
    ns = {"re": re, "Optional": typing.Optional}
    wanted = set(fns)
    for node in tree.body:
        targets = (node.targets if isinstance(node, ast.Assign)
                   else [node.target] if isinstance(node, ast.AnnAssign)
                   else [])
        names = [t.id for t in targets if isinstance(t, ast.Name)]
        if any(n.startswith(("_FM_", "_FRONT_MATTER_")) or n in consts
               for n in names):
            exec(ast.get_source_segment(src, node), ns)
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            exec(ast.get_source_segment(src, node), ns)
            wanted.discard(node.name)
    if methods_of:
        cls, method_names = methods_of
        body = next(n.body for n in tree.body
                    if isinstance(n, ast.ClassDef) and n.name == cls)
        for node in body:
            if isinstance(node, ast.FunctionDef) and node.name in method_names:
                exec(ast.get_source_segment(src, node), ns)
                wanted.discard(node.name)
    if wanted:
        raise AssertionError(f"not found: {sorted(wanted)}")
    missing = [c for c in consts if c not in ns]
    if missing:
        raise AssertionError(f"constants not found: {missing}")
    return ns


NS = _load(
    ["_front_matter_section_label", "_latex_escape", "_export_section_list",
     "_build_export_latex"],
    methods_of=("_ScholarTextWindow",
                ["_export_section_list", "_build_export_latex"]),
    consts=("_CITE_PARSE_RE", "_LATEX_PREAMBLE", "_LATEX_CHAR_MAP"),
)
label_of = NS["_front_matter_section_label"]


class _Block:
    def __init__(self, kind, text):
        self.kind = kind
        self._text = text

    def text(self):
        return self._text


class _Part:
    def __init__(self, kind, blocks=(), label=""):
        self.kind = kind
        self.label = label
        self.blocks = [_Block(k, t) for k, t in blocks]


# --- Sample front matter, in the shapes the reports actually print ---------

CAPTION = [
    ("center", "141 S.Ct. 2063 (2021)"),
    ("center", "CEDAR POINT NURSERY, et al., Petitioners v. Victoria HASSID"),
    ("center", "No. 20-107."),
    ("center", "Supreme Court of United States."),
    ("center", "Argued March 22, 2021."),
    ("center", "Decided June 23, 2021."),
    ("para", "ON WRIT OF CERTIORARI TO THE UNITED STATES COURT OF APPEALS "
             "FOR THE NINTH CIRCUIT."),
]
COUNSEL = [
    ("para", "Joshua P. Thompson, Sacramento, CA, for Petitioners."),
    ("para", "*2069 Michael J. Mongan, Solicitor General, for Respondents."),
    ("para", "Howard A. Sagaser, Ian B. Wieland, Sagaser, Watkins & Wieland "
             "PC, Fresno, CA, Damien M. Schiff, Wencong Fa, Christopher M. "
             "Kieser, Pacific Legal Foundation, Sacramento, CA, Counsel of "
             "Record for Petitioners."),
    ("para", "A brief of amici curiae urging affirmance was filed for the "
             "State of Alabama et al. by Robert K. Corbin, Attorney General "
             "of Arizona, and Richard A. Alcorn and Charles L. Eger, "
             "Assistant Attorneys General; and briefs of amici curiae were "
             "filed for the Canadian Manufacturers Association et al. by "
             "John DeQ. Briggs III and Jan Schneider."),
]
SYLLABUS = [
    ("heading", "*2066 Syllabus"),
    ("para", "A California regulation grants labor organizations a “right "
             "to take access” to an agricultural employer's property in "
             "order to solicit support for unionization. The regulation "
             "mandates that agricultural employers allow union organizers "
             "onto their property for up to three hours per day, 120 days "
             "per year."),
    ("para", "Held: California's access regulation constitutes a per se "
             "physical taking. Pp. 2070-2080."),
    ("para", "(a) The growers' complaint states a claim for an uncompensated "
             "taking in violation of the Fifth and Fourteenth Amendments. "
             "The Takings Clause, applicable to the States through the "
             "Fourteenth Amendment, provides that private property shall not "
             "be taken for public use without just compensation. Pp. "
             "2070-2078."),
    ("para", "923 F.3d 524, reversed and remanded."),
]
LINEUP = [
    ("para", "ROBERTS, C. J., delivered the opinion of the Court, in which "
             "THOMAS, ALITO, GORSUCH, KAVANAUGH, and BARRETT, JJ., joined. "
             "KAVANAUGH, J., filed a concurring opinion. BREYER, J., filed a "
             "dissenting opinion, in which SOTOMAYOR and KAGAN, JJ., "
             "joined."),
]


def front(*chunks):
    return _Part("header", [row for chunk in chunks for row in chunk])


class FrontMatterLabelTests(unittest.TestCase):
    def test_a_supreme_court_syllabus_is_named_as_one(self):
        self.assertEqual(
            label_of([front(CAPTION, COUNSEL, SYLLABUS, LINEUP)]), "Syllabus")

    def test_a_bare_caption_is_not_front_matter(self):
        self.assertEqual(label_of([front(CAPTION)]), "")

    def test_counsel_listings_alone_are_not_front_matter(self):
        # Copperweld's header runs past four thousand characters without a
        # word of syllabus in it; length alone must not promote it.
        listings = COUNSEL * 7
        self.assertGreater(sum(len(t) for _k, t in listings), 4000)
        self.assertEqual(label_of([front(CAPTION, listings)]), "")

    def test_the_syllabus_line_up_alone_is_not_front_matter(self):
        # Older U.S. Reports pages carry the line-up but no syllabus text
        # (Nixon v. United States), and there is nothing to lift out.
        self.assertEqual(label_of([front(CAPTION, LINEUP, COUNSEL)]), "")

    def test_a_syllabus_without_a_heading_is_read_from_its_prose(self):
        unheaded = [row for row in SYLLABUS if row[0] != "heading"]
        self.assertEqual(
            label_of([front(CAPTION, COUNSEL, unheaded * 2, LINEUP)]),
            "Syllabus")

    def test_a_state_syllabus_by_the_court_is_a_syllabus(self):
        state = [
            ("heading", "*348 Syllabus by the Court"),
            ("para", "1. When unclaimed property transferred to the State "
                     "under the Minnesota Unclaimed Property Act is "
                     "interest-bearing, the interest that accrues while the "
                     "State holds the property belongs to the owner, and the "
                     "State must pay it over when the property is claimed."),
            ("para", "2. Because the notice provided by and under the Act to "
                     "owners of interest-bearing property valued over $100 "
                     "is sufficient to inform those owners that their "
                     "property is held by the State and may be reclaimed, "
                     "the claimants' due process challenge fails."),
        ]
        self.assertEqual(label_of([front(CAPTION, COUNSEL, state)]),
                         "Syllabus")

    def test_headmatter_headnotes_keep_their_own_name(self):
        # CourtListener renders CAP headmatter with a heading per element.
        headmatter = [
            ("heading", "Headnotes"),
            ("para", "1. Contracts — Consideration. A promise to do what the "
                     "promisor is already bound by law to do is no "
                     "consideration for a return promise, and an agreement "
                     "resting on such a promise alone is unenforceable, "
                     "however formally it may have been executed."),
            ("para", "2. Sales — Warranty. An implied warranty of "
                     "merchantability attaches to every sale by a dealer in "
                     "goods of that description, survives acceptance of the "
                     "goods by the buyer, and is not displaced by an express "
                     "warranty covering other qualities of the goods."),
            ("heading", "Counsel"),
            ("para", "Charles Lee, for the applicants."),
        ]
        self.assertEqual(label_of([front(CAPTION, headmatter)]), "Headnotes")

    def test_two_header_parts_are_read_as_one_front_matter(self):
        self.assertEqual(
            label_of([_Part("header", CAPTION + COUNSEL),
                      _Part("header", SYLLABUS + LINEUP)]),
            "Syllabus")

    def test_the_arguments_of_counsel_are_named(self):
        # De Lima v. Bidwell prints the whole argument, colloquy and all.
        argument = [
            ("para", "Mr. Frederick R. Coudert, Jr., for plaintiff in error."),
            ("para", "The plaintiffs in error contended that the island of "
                     "Porto Rico, having been ceded to the United States by "
                     "the treaty of peace, ceased upon the exchange of "
                     "ratifications to be a foreign country, and that duties "
                     "exacted upon goods brought from it were therefore "
                     "exacted without warrant of law."),
            ("para", "It was insisted for the defendant in error that the "
                     "collection of duties continued lawful until Congress "
                     "should provide otherwise, and that the practice of the "
                     "government under earlier cessions had uniformly been "
                     "to that effect."),
            ("para", "MR. JUSTICE HARLAN. Does that apply to revenue cases?"),
            ("para", "THE SOLICITOR GENERAL. This is not a revenue case, so "
                     "opposing counsel insist; they urge that the tax is a "
                     "local one, laid for the benefit of the island itself, "
                     "and that the uniformity clause of the Constitution "
                     "therefore has no application to it whatever."),
        ]
        self.assertEqual(label_of([front(CAPTION, argument * 2)]),
                         "Argument of Counsel")

    def test_a_header_the_opinion_was_swept_into_is_not_front_matter(self):
        # Dartmouth College: segmentation missed "The opinion of the court
        # was delivered by MARSHALL, Ch. J." and kept the opinion in the
        # header.  Labelling that "Argument of Counsel" would put the wrong
        # running head over the Court's own words.
        swept = [
            ("para", "March 10th and 11th, 1818. Webster, for the plaintiffs "
                     "in error, contended that the charter was a contract."),
            ("para", "February 2d, 1819. The opinion of the court was "
                     "delivered by MARSHALL, Ch. J."),
            ("para", "This is an action of trover, brought by the Trustees "
                     "of Dartmouth College against William H. Woodward, in "
                     "the state court of New Hampshire, for the book of "
                     "records, corporate seal, and other corporate property "
                     "of the college."),
            ("para", "It can require no argument to prove that the "
                     "circumstances of this case constitute a contract. An "
                     "application is made to the crown for a charter to "
                     "incorporate a religious and literary institution."),
        ]
        self.assertEqual(label_of([front(CAPTION, swept * 3)]), "")

    def test_a_bare_byline_in_the_header_is_the_opinion_starting(self):
        swept = [
            ("heading", "OPINION"),
            ("para", "Justice SAYLOR."),
            ("para", "The civil action underlying this appeal was selected "
                     "as a test case for the admissibility of expert opinion "
                     "evidence to the effect that each and every exposure to "
                     "asbestos contributes substantially to the development "
                     "of mesothelioma."),
            ("para", "In February 2005, Charles Simikian commenced a product "
                     "liability action against Allied Signal, Inc., Ford "
                     "Motor Company, and others, asserting claims in strict "
                     "liability and negligence."),
        ]
        self.assertEqual(label_of([front(CAPTION, COUNSEL, swept * 3)]), "")

    def test_a_fractured_majority_syllabus_survives_its_own_attributions(self):
        # Ramos: once the syllabus has begun, "Justice GORSUCH delivered the
        # opinion of the Court with respect to Parts I, II-A …, concluding
        # that …" is the reporter summarizing, not the opinion starting.
        fractured = [
            ("heading", "Syllabus"),
            ("para", "Held: The Sixth Amendment right to a jury trial, as "
                     "incorporated against the States by way of the "
                     "Fourteenth Amendment, requires a unanimous verdict to "
                     "convict a defendant of a serious offense. Pp. "
                     "1394-1397."),
            ("para", "231 So.3d 44, reversed."),
            ("para", "Justice GORSUCH delivered the opinion of the Court "
                     "with respect to Parts I, II-A, III, and IV-B-1, "
                     "concluding that the Sixth Amendment right to a jury "
                     "trial requires a unanimous verdict."),
            ("para", "(a) The Constitution's text and structure clearly "
                     "indicate that the Sixth Amendment term “trial by "
                     "an impartial jury” carries with it some "
                     "meaning about the content and requirements of a jury "
                     "trial, and one of those requirements is unanimity."),
            ("para", "(b) Louisiana's and Oregon's unconventional schemes "
                     "were first confronted in Apodaca v. Oregon, which "
                     "yielded no majority opinion and no controlling "
                     "rationale for the courts below to follow."),
        ]
        self.assertEqual(label_of([front(CAPTION, COUNSEL, fractured)]),
                         "Syllabus")


class ExportSectionListTests(unittest.TestCase):
    """The sections the RTF and LaTeX exports lay out, in order."""

    class _FakeText:
        def index(self, spec):
            return "999.0" if spec.startswith("end") else spec

    def _window(self, parts, regions):
        win = type("W", (), {})()
        win._text = self._FakeText()
        win._parts = parts
        win._rendered_parts = parts
        win._part_region_indices = lambda: regions
        win._majority_author = lambda part: "Roberts, C. J."
        win._writer_parenthetical = lambda part: "Kagan, J., dissenting"
        win._export_section_list = NS["_export_section_list"].__get__(win)
        return win

    def test_a_syllabus_leads_as_its_own_section(self):
        parts = [front(CAPTION, COUNSEL, SYLLABUS, LINEUP),
                 _Part("majority", [("para", "Chief Justice ROBERTS "
                                             "delivered the opinion.")]),
                 _Part("dissent", [("para", "Justice KAGAN, dissenting.")])]
        win = self._window(parts, [("1.0", "20.0", 0), ("20.0", "60.0", 1),
                                   ("60.0", "90.0", 2)])
        self.assertEqual(
            win._export_section_list(),
            [("Syllabus", "1.0", "20.0", "frontmatter"),
             ("Roberts, C. J.", "20.0", "60.0", "majority"),
             ("Kagan, J., dissenting", "60.0", "90.0", "dissent")],
        )

    def test_a_bare_caption_still_rides_with_the_opinion(self):
        parts = [front(CAPTION, COUNSEL),
                 _Part("majority", [("para", "Chief Justice ROBERTS "
                                             "delivered the opinion.")])]
        win = self._window(parts, [("1.0", "20.0", 0), ("20.0", "60.0", 1)])
        self.assertEqual(win._export_section_list(),
                         [("Roberts, C. J.", "1.0", "60.0", "majority")])

    def test_both_header_parts_go_into_the_front_matter_section(self):
        parts = [_Part("header", CAPTION + COUNSEL),
                 _Part("header", SYLLABUS + LINEUP),
                 _Part("majority", [("para", "Chief Justice ROBERTS "
                                             "delivered the opinion.")])]
        win = self._window(parts, [("1.0", "10.0", 0), ("10.0", "20.0", 1),
                                   ("20.0", "60.0", 2)])
        self.assertEqual(
            win._export_section_list(),
            [("Syllabus", "1.0", "20.0", "frontmatter"),
             ("Roberts, C. J.", "20.0", "60.0", "majority")],
        )

    def test_a_header_with_no_opinion_after_it_is_left_alone(self):
        parts = [front(CAPTION, COUNSEL, SYLLABUS, LINEUP)]
        win = self._window(parts, [("1.0", "20.0", 0)])
        self.assertEqual(win._export_section_list(),
                         [("", "1.0", "20.0", "majority")])

    def test_a_plain_text_view_is_one_section(self):
        win = self._window([], [])
        self.assertEqual(win._export_section_list(),
                         [("", "1.0", "999.0", "majority")])


class LatexRunningHeadTests(unittest.TestCase):
    """What the reader is after: the syllabus named in the header of the
    pages it occupies, and the opinion starting on a page of its own."""

    class _FakeText:
        def index(self, spec):
            return "999.0" if spec.startswith("end") else spec

        def tag_ranges(self, _tag):
            return ()

    def _tex(self, parts, regions):
        win = type("W", (), {})()
        win._text = self._FakeText()
        win._bb = {"cite": "594 U.S. 139"}
        win._parts = parts
        win._rendered_parts = parts
        win._part_region_indices = lambda: regions
        win._fn_link_map = lambda: {}
        win._citation_name = lambda: "Cedar Point Nursery v. Hassid"
        win._bluebook_citation = lambda _pin: (
            "Cedar Point Nursery v. Hassid, 594 U.S. 139 (2021).", "")
        win._majority_author = lambda part: "Roberts, C. J."
        win._writer_parenthetical = lambda part: "Kagan, J., dissenting"
        win._latex_section_body = lambda rs, rend, links, title_block: (
            f"%% body {rs}-{rend} title={title_block}\n")
        win._export_section_list = NS["_export_section_list"].__get__(win)
        win._build_export_latex = NS["_build_export_latex"].__get__(win)
        return win._build_export_latex()

    def _sections(self, tex):
        return re.findall(r"\\renewcommand\{\\opinionhead\}\{(.*)\}", tex)

    def test_the_syllabus_is_named_in_its_running_head(self):
        parts = [front(CAPTION, COUNSEL, SYLLABUS, LINEUP),
                 _Part("majority", [("para", "Chief Justice ROBERTS "
                                             "delivered the opinion.")])]
        tex = self._tex(parts, [("1.0", "20.0", 0), ("20.0", "60.0", 1)])
        heads = self._sections(tex)
        self.assertEqual(len(heads), 2)
        self.assertTrue(heads[0].endswith(" --- Syllabus"), heads[0])
        self.assertTrue(heads[1].endswith(" --- Roberts, C. J."), heads[1])
        # The opinion starts a fresh page, as a separate opinion would.
        self.assertEqual(tex.count("\\clearpage"), 1)

    def test_a_bare_caption_leaves_one_head_for_the_opinion(self):
        parts = [front(CAPTION, COUNSEL),
                 _Part("majority", [("para", "Chief Justice ROBERTS "
                                             "delivered the opinion.")])]
        tex = self._tex(parts, [("1.0", "20.0", 0), ("20.0", "60.0", 1)])
        self.assertEqual(len(self._sections(tex)), 1)
        self.assertNotIn("Syllabus", tex)
        self.assertNotIn("\\clearpage", tex)


if __name__ == "__main__":
    unittest.main()
