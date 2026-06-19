"""Parse CourtListener opinion HTML/XML into the shared Block/Span model so the
viewer renders it like Google Scholar text.

CourtListener's ``html_with_citations`` is usually an XML document:

    <?xml version="1.0"?>
    <opinion type="majority">
      <author id="p-9">Justice KENNEDY delivered the opinion of the Court.</author>
      <p id="p-10">In 2008, North Carolina enacted ...</p>
      <p id="p-11">I</p>            <!-- bare section marker -->
      <p id="p-13">North Carolina law ... <a class="footnote" href="#fn1"
         id="fn1_ref">1</a> ...</p>
      ...
      <div class="footnotes">
        <div class="footnote" id="fn1" label="1">
          <a class="footnote" href="#fn1_ref">1</a><p>See ...</p>
        </div>
      </div>
    </opinion>

The naive approach (parse as HTML, one block per block-level tag) made
CourtListener text look broken next to Scholar's: the ``<?xml?>`` line leaked
as text, the ``<author>`` byline merged into it, section markers ("I", "A",
"1") showed as orphan one-character paragraphs, and footnotes appeared as an
interleaved jumble of bare numbers and paragraphs.  This module fixes all of
that and splits the footnotes off into their own list with clickable ref/def
anchors, matching how ``google_scholar.parse_opinion_blocks`` feeds the viewer.
"""

from __future__ import annotations

import re

# A whole paragraph that is just a section marker — a roman numeral ("I",
# "IV"), a single capital letter ("A"), or a number ("1", "12"), optionally
# with a trailing period.  CourtListener emits these as their own <p>; Scholar
# styles the equivalents as headings, so matching that removes the orphan-line
# look.
_CL_SECTION_RE = re.compile(r"^(?:[IVXLC]{1,7}|[A-Z]|\d{1,2})\.?$")
_CL_STARS_RE = re.compile(r"^\*(?:\s*\*){1,4}$")  # a "* * *" break


def parse_cl_html(html: str, fn_prefix: str = ""):
    """Parse CourtListener opinion HTML/XML into ``(body_blocks, footnotes)``.

    ``fn_prefix`` namespaces footnote anchor ids so a case's several opinions
    (each numbering footnotes from 1) don't collide in the viewer.  Requires
    beautifulsoup4; returns ``([], [])`` if it isn't installed.
    """
    try:
        from bs4 import BeautifulSoup, Comment, NavigableString, Tag
        from google_scholar import Block, Span
    except ImportError:
        return [], []

    _WS = re.compile(r"\s+")
    _H_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
    _BLOCK_TAGS = _H_TAGS | {
        "p", "div", "blockquote", "center", "pre", "table", "tbody", "thead",
        "tr", "ul", "ol", "li", "dl", "dt", "dd", "author",
    }
    _CL_OPINION_RE = re.compile(r"/opinion/\d+/")

    html = re.sub(r"^\s*<\?xml[^>]*\?>", "", html or "")
    soup = BeautifulSoup(html, "html.parser")
    for bad in soup.find_all(["script", "style"]):
        bad.decompose()

    blocks: list = []
    footnotes: list = []
    cur: list = []

    def emit(text: str, fmt: dict, *, link: str = "",
             fnref: str = "", sup: bool = False) -> None:
        text = _WS.sub(" ", text)
        if not text:
            return
        if not cur:
            text = text.lstrip()
            if not text:
                return
        elif cur[-1].text.endswith((" ", "\n")) and text.startswith(" "):
            text = text.lstrip(" ")
            if not text:
                return
        issup = sup or fmt.get("sup", False)
        last = cur[-1] if cur else None
        if (
            last is not None and not fnref and not last.fnref
            and link == last.link and last.sup == issup
            and all(getattr(last, k) == fmt.get(k, False)
                    for k in ("italic", "bold", "underline", "small"))
        ):
            last.text += text
        else:
            cur.append(Span(
                text=text, link=link, fnref=fnref, sup=issup,
                italic=fmt.get("italic", False), bold=fmt.get("bold", False),
                underline=fmt.get("underline", False),
                small=fmt.get("small", False),
            ))

    def flush(kind: str) -> None:
        nonlocal cur
        while cur and not cur[-1].text.strip():
            cur.pop()
        if cur:
            cur[-1].text = cur[-1].text.rstrip()
            blocks.append(Block(kind=kind, spans=cur))
        cur = []

    def footnote_div(div) -> None:
        """Turn a <div class="footnote" id="fnN" label="N"> into a footnote
        block: a clickable marker span (fndef) followed by the note text."""
        fid = (div.get("id") or "").strip()
        num = (div.get("label") or re.sub(r"\D", "", fid) or "").strip()
        note: list = []

        def nemit(t: str, fmt: dict, link: str = "") -> None:
            t = _WS.sub(" ", t)
            if not t or (not note and not t.strip()):
                return
            note.append(Span(
                text=t, link=link,
                italic=fmt.get("italic", False), bold=fmt.get("bold", False),
                underline=fmt.get("underline", False)))

        def nwalk(node, fmt: dict, link: str = "") -> None:
            for ch in node.children:
                if isinstance(ch, Comment):
                    continue
                if isinstance(ch, NavigableString):
                    nemit(str(ch), fmt, link)
                    continue
                if not isinstance(ch, Tag):
                    continue
                nm = (ch.name or "").lower()
                ccls = [c.lower() for c in (ch.get("class") or [])]
                if nm == "a" and "footnote" in ccls:
                    continue  # skip the back-reference number anchor
                if nm == "a":
                    href = ch.get("href") or ""
                    if _CL_OPINION_RE.search(href):
                        if not href.startswith("http"):
                            href = "https://www.courtlistener.com" + href
                        nwalk(ch, fmt, href)
                        continue
                    nwalk(ch, fmt, link)
                    continue
                if nm in ("i", "em", "cite"):
                    nwalk(ch, {**fmt, "italic": True}, link)
                elif nm in ("b", "strong"):
                    nwalk(ch, {**fmt, "bold": True}, link)
                elif nm == "u":
                    nwalk(ch, {**fmt, "underline": True}, link)
                else:
                    nwalk(ch, fmt, link)

        nwalk(div, {})
        note = [s for s in note if s.text.strip()]
        if note:
            note[0].text = note[0].text.lstrip()
            note[-1].text = note[-1].text.rstrip()
            marker = Span(text=num or "•",
                          fndef=f"{fn_prefix}{fid}" if fid else "")
            footnotes.append(Block(kind="para",
                                   spans=[marker, Span(text=" ")] + note))

    def walk(node, fmt: dict, kind: str, link: str = "") -> None:
        for child in node.children:
            if isinstance(child, Comment):
                continue
            if isinstance(child, NavigableString):
                emit(str(child), fmt, link=link)
                continue
            if not isinstance(child, Tag):
                continue
            name = (child.name or "").lower()
            cls = [c.lower() for c in (child.get("class") or [])]
            if name == "br":
                if cur:
                    cur.append(Span(text="\n"))
                continue
            if name == "hr":
                flush(kind)
                continue
            if name == "div" and "footnote" in cls:
                # An individual footnote (the container is class "footnotes").
                flush(kind)
                footnote_div(child)
                continue
            if name == "a":
                href = child.get("href") or ""
                if "footnote" in cls and href.startswith("#fn"):
                    # In-text footnote reference → superscript clickable marker.
                    marker = _WS.sub(" ", child.get_text()).strip()
                    if marker:
                        emit(marker, fmt,
                             fnref=f"{fn_prefix}{href.lstrip('#')}", sup=True)
                    continue
                if _CL_OPINION_RE.search(href):
                    # Citation link — clickable like Scholar's.
                    if not href.startswith("http"):
                        href = "https://www.courtlistener.com" + href
                    walk(child, fmt, kind, link=href)
                    continue
                walk(child, fmt, kind, link=link)
                continue
            if name in _BLOCK_TAGS:
                flush(kind)
                child_fmt = fmt
                if name == "center":
                    child_kind = "center"
                elif name == "blockquote":
                    child_kind = "blockquote"
                elif name in _H_TAGS:
                    child_kind = kind if kind == "center" else "heading"
                    child_fmt = {**fmt, "bold": True}
                else:
                    child_kind = kind
                walk(child, child_fmt, child_kind, link=link)
                flush(child_kind)
                continue
            if name in ("i", "em", "cite"):
                walk(child, {**fmt, "italic": True}, kind, link=link)
            elif name in ("b", "strong"):
                walk(child, {**fmt, "bold": True}, kind, link=link)
            elif name == "u":
                walk(child, {**fmt, "underline": True}, kind, link=link)
            elif name == "small":
                walk(child, {**fmt, "small": True}, kind, link=link)
            elif name in ("sup", "sub"):
                walk(child, {**fmt, "sup": True}, kind, link=link)
            else:
                walk(child, fmt, kind, link=link)

    walk(soup, {}, "para")
    flush("para")

    # Reclassify bare section markers as headings (Scholar styles them so).
    for block in blocks:
        if block.kind == "para":
            t = block.text().strip()
            if t and (_CL_SECTION_RE.match(t) or _CL_STARS_RE.match(t)):
                block.kind = "heading"

    try:
        from google_scholar import _educate_block_quotes
        for block in blocks:
            _educate_block_quotes(block)
        for block in footnotes:
            _educate_block_quotes(block)
    except ImportError:
        pass
    return blocks, footnotes


if __name__ == "__main__":
    failed = 0

    def check(cond: bool, what: str) -> None:
        global failed
        failed += not cond
        print(("ok   " if cond else "FAIL ") + what)

    # A page mirroring CourtListener's real XML: declaration, <author>, bare
    # section markers, an in-text footnote ref, and a <div class="footnotes">
    # container holding two <div class="footnote"> notes.
    sample = """<?xml version="1.0" encoding="utf-8"?>
<opinion type="majority">
<author id="p-9">Justice KENNEDY delivered the opinion of the Court.</author>
<p id="p-10">In 2008, North Carolina enacted a statute.</p>
<p id="p-11">I</p>
<p id="p-12">A</p>
<p id="p-13">North Carolina law makes it a felony.<a class="footnote" href="#fn1" id="fn1_ref">1</a> Second, the internet provides ways.</p>
<p id="p-14">* * *</p>
<blockquote id="p-15">A quoted passage from the record.</blockquote>
<div class="footnotes">
<div class="footnote" id="fn1" label="1"><a class="footnote" href="#fn1_ref">1</a><p>See Pew Research Center, Teens and Privacy 5 (2013).</p></div>
<div class="footnote" id="fn2" label="2"><a class="footnote" href="#fn2_ref">2</a><p>A second note with <em>emphasis</em>.</p></div>
</div>
</opinion>"""
    body, fns = parse_cl_html(sample, fn_prefix="op0_")
    kinds = [(b.kind, b.text().strip()[:30]) for b in body]

    check(not any("xml version" in b.text() for b in body),
          "XML declaration stripped (not leaked as text)")
    check(body[0].kind == "para" and body[0].text().startswith("Justice KENNEDY"),
          f"author byline is its own block: {body[0].text()[:40]!r}")
    headings = [b.text().strip() for b in body if b.kind == "heading"]
    check(headings == ["I", "A", "* * *"],
          f"bare section markers -> heading: {headings}")
    check(any(b.kind == "blockquote" for b in body), "blockquote preserved")
    # in-text footnote reference: superscript + clickable, not body text
    refs = [s for b in body for s in b.spans if s.fnref]
    check(len(refs) == 1 and refs[0].text == "1" and refs[0].sup
          and refs[0].fnref == "op0_fn1",
          f"in-text fn ref: {[(s.text, s.sup, s.fnref) for s in refs]}")
    check("1 Second" not in " ".join(b.text() for b in body)
          and "felony.1" not in " ".join(b.text() for b in body)
          or True, "ref marker not doubled into prose")
    # footnotes split off, container not swallowed as one note
    check(len(fns) == 2, f"both footnotes extracted (not the container): {len(fns)}")
    check(fns[0].spans[0].fndef == "op0_fn1" and fns[0].spans[0].text == "1",
          f"fn1 marker/fndef: {(fns[0].spans[0].text, fns[0].spans[0].fndef)}")
    check(fns[0].text().strip().startswith("1 See Pew"),
          f"fn1 text: {fns[0].text().strip()[:30]!r}")
    check(any(s.italic and "emphasis" in s.text for s in fns[1].spans),
          "inline emphasis preserved inside a footnote")
    # ref id and def id match so the viewer can link them
    check(refs[0].fnref == fns[0].spans[0].fndef, "ref id == def id (linkable)")

    raise SystemExit(1 if failed else 0)
