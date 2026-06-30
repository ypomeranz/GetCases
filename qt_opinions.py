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

    body = [
        f"<h1>{html.escape(title or 'Opinion')}</h1>",
        '<div class="opinion-meta">',
    ]
    if source_url:
        body.append(
            f'<span><a href="{html.escape(source_url, quote=True)}">'
            "Google Scholar source</a></span>"
        )
    body.append("</div>")

    if not parts:
        body.append("<p class=\"muted\">No opinion text was found.</p>")
        return "\n".join(body)

    for part in parts:
        body.append(_render_part(part))
    return "\n".join(body)


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


def _class_token(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", (value or "").lower()).strip("-") or "para"
