"""Structured opinion rendering for the PySide6 front end."""

from __future__ import annotations

import html
import re
import urllib.parse

from citations import detect_links
from google_scholar import (
    Block,
    OpinionPart,
    Span,
    link_footnotes_by_marker,
    parse_opinion_blocks,
    segment_blocks,
)


def render_scholar_opinion_body(title: str, source_url: str, opinion_html: str) -> str:
    """Render Scholar/CourtListener opinion HTML into app-styled HTML."""
    blocks = parse_opinion_blocks(opinion_html)
    parts = segment_blocks(blocks)
    link_footnotes_by_marker(parts)
    if not parts and blocks:
        parts = [OpinionPart("Opinion", "majority", blocks)]
    return render_opinion_parts_body(
        title,
        parts,
        source_url=source_url,
        source_label="Google Scholar source" if source_url else "",
    )


def render_opinion_parts_body(
    title: str,
    parts: list[OpinionPart],
    *,
    source_url: str = "",
    source_label: str = "",
    note: str = "",
) -> str:
    """Render pre-parsed opinion parts into app-styled HTML."""
    body = [
        f"<h1>{html.escape(title or 'Opinion')}</h1>",
        '<div class="opinion-meta">',
    ]
    if source_url:
        body.append(
            f'<span><a href="{html.escape(source_url, quote=True)}">'
            f"{html.escape(source_label or 'Source')}</a></span>"
        )
    if note:
        body.append(f"<span>{html.escape(note)}</span>")
    body.append("</div>")

    if not parts:
        body.append("<p class=\"muted\">No opinion text was found.</p>")
        return "\n".join(body)

    for part in parts:
        body.append(_render_part(part))
    return "\n".join(body)


def render_oyez_case_details(case) -> str:
    """Render an Oyez case summary as an insertable HTML fragment."""
    if not case or not getattr(case, "is_substantive", False):
        return ""

    out = ['<section id="case-details" class="case-details">']
    out.append("<h2>Supreme Court Details</h2>")
    meta = " | ".join(
        value
        for value in (
            getattr(case, "citation", ""),
            getattr(case, "court", ""),
            f"Docket {getattr(case, 'docket', '')}" if getattr(case, "docket", "") else "",
        )
        if value
    )
    if meta:
        out.append(f'<p class="case-details-meta">{html.escape(meta)}</p>')

    _append_detail_text(out, "Summary", getattr(case, "description", ""))
    _append_detail_text(out, "Question", getattr(case, "question", ""))
    _append_detail_text(out, "Conclusion", getattr(case, "conclusion", ""))

    voted = list(getattr(case, "voted_decisions", []) or [])
    if voted:
        out.append('<div class="case-details-votes">')
        for decision in voted:
            out.append('<div class="vote-lineup">')
            heading = "Decision"
            vote_line = getattr(decision, "vote_line", "")
            decision_type = getattr(decision, "decision_type", "")
            if vote_line or decision_type:
                heading += ": " + " ".join(p for p in (vote_line, decision_type) if p)
            out.append(f"<h3>{html.escape(heading)}</h3>")
            _append_detail_text(out, "Holding", getattr(decision, "description", ""))
            winner = getattr(decision, "winning_party", "")
            if winner:
                out.append(f"<p><strong>Winning party:</strong> {html.escape(winner)}</p>")
            majority = _justice_labels(getattr(decision, "majority", []))
            dissent = _justice_labels(getattr(decision, "dissent", []))
            other = _justice_labels(getattr(decision, "other", []))
            if majority:
                out.append(f"<p><strong>Majority:</strong> {html.escape(majority)}</p>")
            if dissent:
                out.append(f"<p><strong>Dissent:</strong> {html.escape(dissent)}</p>")
            if other:
                out.append(f"<p><strong>Other:</strong> {html.escape(other)}</p>")
            out.append("</div>")
        out.append("</div>")
    else:
        _append_opinion_authors(out, case)

    arguments = list(getattr(case, "oral_arguments", []) or [])
    if arguments:
        out.append("<h3>Oral Argument</h3>")
        out.append("<ul>")
        for argument in arguments:
            title = html.escape(getattr(argument, "title", "") or "Oral Argument")
            url = html.escape(getattr(argument, "url", "") or getattr(case, "web_url", ""), quote=True)
            out.append(f'<li><a href="{url}">{title}</a></li>')
        out.append("</ul>")

    web_url = getattr(case, "web_url", "")
    if web_url:
        out.append(
            '<p class="case-details-source">'
            f'<a href="{html.escape(web_url, quote=True)}">View full details on Oyez</a>'
            "</p>"
        )
    out.append("</section>")
    return "\n".join(out)


def _render_part(part: OpinionPart) -> str:
    part_kind = _class_token(part.kind or "opinion")
    out = [
        f'<section class="opinion-part {part_kind}">',
        f'<h2 class="part-label">{html.escape(part.label or "Opinion")}</h2>',
    ]
    out.extend(_render_block(block) for block in part.blocks)
    if part.footnotes:
        out.append('<section class="footnotes">')
        out.append("<h3>Footnotes</h3>")
        out.extend(_render_block(block, footnote=True) for block in part.footnotes)
        out.append("</section>")
    out.append("</section>")
    return "\n".join(out)


def _render_block(block: Block, *, footnote: bool = False) -> str:
    kind = _class_token(block.kind or "para")
    cls = f"opinion-block {kind}" + (" footnote" if footnote else "")
    tag = "p"
    if kind == "heading":
        tag = "h3"
    return f'<{tag} class="{cls}">' + "".join(_render_span(s) for s in block.spans) + f"</{tag}>"


def _render_span(span: Span) -> str:
    if not span.text:
        return ""
    anchor_id = _anchor_id(span.fndef) if span.fndef else ""
    attrs = f' id="{anchor_id}"' if anchor_id else ""

    if span.link:
        href = _scholar_href(span.link, span.text)
        return f'<a{attrs} class="opinion-link" href="{href}">{_styled(span, span.text)}</a>'
    if span.fnref:
        href = "#" + _anchor_id(span.fnref)
        return f'<a{attrs} class="fn-ref" href="{href}">{_styled(span, span.text)}</a>'
    if span.pagenum:
        attrs = f' id="{_page_anchor_id(span.text)}"'
        return f'<span{attrs} class="pagenum">{_styled(span, span.text)}</span>'

    # Add app links for unlinked source/case citations inside plain spans.
    links = detect_links(span.text)
    if not links:
        return f"<span{attrs}>{_styled(span, span.text)}</span>" if attrs else _styled(span, span.text)

    out: list[str] = []
    pos = 0
    first = True
    for start, end, action in links:
        if start < pos:
            continue
        if start > pos:
            piece_attrs = attrs if first else ""
            out.append(_plain_span(span, span.text[pos:start], piece_attrs))
            first = False
        kind, value = action
        href = "getcases://open?" + urllib.parse.urlencode({"kind": kind, "value": value})
        piece_attrs = attrs if first else ""
        out.append(
            f'<a{piece_attrs} class="cite" href="{href}">'
            f"{_styled(span, span.text[start:end])}</a>"
        )
        first = False
        pos = end
    if pos < len(span.text):
        piece_attrs = attrs if first else ""
        out.append(_plain_span(span, span.text[pos:], piece_attrs))
    return "".join(out)


def _plain_span(span: Span, text: str, attrs: str = "") -> str:
    rendered = _styled(span, text)
    return f"<span{attrs}>{rendered}</span>" if attrs else rendered


def _styled(span: Span, text: str) -> str:
    rendered = html.escape(text).replace("\n", "<br>")
    if span.small:
        rendered = f"<small>{rendered}</small>"
    if span.sup:
        rendered = f"<sup>{rendered}</sup>"
    if span.underline:
        rendered = f"<u>{rendered}</u>"
    if span.italic:
        rendered = f"<em>{rendered}</em>"
    if span.bold:
        rendered = f"<strong>{rendered}</strong>"
    return rendered


def _scholar_href(url: str, title: str) -> str:
    return "getcases://scholar?" + urllib.parse.urlencode({
        "url": url,
        "title": re.sub(r"\s+", " ", title or "").strip(),
    })


def _anchor_id(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value or "").strip("-")
    return "fn-" + (safe or "note")


def _page_anchor_id(value: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_.-]+", "", value or "").lstrip("*")
    return "page-" + (safe or "marker")


def _class_token(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", (value or "").lower()).strip("-") or "para"


def _append_detail_text(out: list[str], label: str, value: str) -> None:
    text = re.sub(r"\s+", " ", value or "").strip()
    if text:
        out.append(f"<p><strong>{html.escape(label)}:</strong> {html.escape(text)}</p>")


def _justice_labels(justices) -> str:
    return ", ".join(getattr(justice, "label", "") for justice in justices if getattr(justice, "label", ""))


def _append_opinion_authors(out: list[str], case) -> None:
    rows: list[str] = []
    for kind, label in (
        ("majority", "Majority"),
        ("concurrence", "Concurrence"),
        ("dissent", "Dissent"),
    ):
        try:
            authors = case.opinions_of(kind)
        except Exception:
            authors = []
        names = ", ".join(getattr(opinion, "last", "") for opinion in authors if getattr(opinion, "last", ""))
        if names:
            rows.append(f"<p><strong>{label}:</strong> {html.escape(names)}</p>")
    if rows:
        out.append("<h3>Opinion Authors</h3>")
        out.extend(rows)
