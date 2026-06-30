"""Structured CourtListener opinion text assembly.

This module is the reusable, non-Tk version of the text assembly logic that
used to live only inside ``courtlistener_gui.py``.  It builds the same
``google_scholar.OpinionPart`` / ``Block`` / ``Span`` model used by the Qt
opinion renderer, so Scholar and CourtListener text share one display path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from case_utils import case_name, cluster_citations_to_strings
from cl_parse import parse_cl_html
from google_scholar import (
    Block,
    OpinionPart,
    Span,
    blocks_to_text,
    educate_quotes,
    link_footnotes_by_marker,
    parse_opinion_blocks,
    segment_blocks,
)


_OPINION_TYPE_LABELS: dict[str, str] = {
    "010combined": "Opinion",
    "015unamimous": "Unanimous Opinion",
    "020lead": "Lead Opinion",
    "025plurality": "Plurality Opinion",
    "030concurrence": "Concurrence",
    "035concurrenceinpart": "Concurrence in Part",
    "040dissent": "Dissent",
    "050addendum": "Addendum",
    "060remittitur": "Remittitur",
    "070rehearing": "Rehearing",
    "080onthemerits": "On the Merits",
    "090onmotiontoamend": "On Motion to Amend",
}

_CL_TYPE_KIND: dict[str, str] = {
    "010combined": "majority",
    "015unamimous": "majority",
    "020lead": "majority",
    "025plurality": "majority",
    "030concurrence": "concurrence",
    "035concurrenceinpart": "concurrence",
    "040dissent": "dissent",
    "050addendum": "majority",
    "060remittitur": "majority",
    "070rehearing": "majority",
    "080onthemerits": "majority",
    "090onmotiontoamend": "majority",
}


@dataclass
class CourtListenerOpinion:
    title: str
    parts: list[OpinionPart]
    blocks: list[Block]
    plain_text: str
    cluster: dict[str, Any]


def assemble_case_parts(client, item: dict) -> CourtListenerOpinion:
    """Fetch a CourtListener cluster and assemble structured opinion parts."""
    cluster_id = item.get("cluster_id") or item.get("id")
    if not cluster_id:
        raise ValueError("This result does not include a CourtListener cluster id.")

    cluster = client.get_cluster(
        int(cluster_id),
        fields="case_name,citations,judges,attorneys,syllabus,headnotes,"
               "sub_opinions,date_filed,docket",
    )
    _fill_missing_court_id(client, item, cluster)

    title = _strip_tags(
        cluster.get("case_name") or item.get("caseName") or item.get("case_name") or ""
    ) or case_name(item, "Opinion")
    header_blocks = _header_blocks(cluster, item, title)
    parts: list[OpinionPart] = []
    if header_blocks:
        parts.append(OpinionPart(label="Header", kind="header", blocks=header_blocks))

    opinions = _fetch_sub_opinions(client, cluster)
    combined = _pick_combined_opinion(opinions)
    if combined is not None:
        combined_parts = _combined_parts(combined)
        if combined_parts:
            blocks = [b for p in combined_parts for b in p.blocks]
            plain = _safe_blocks_to_text(blocks)
            return CourtListenerOpinion(title, combined_parts, blocks, plain, cluster)

    blocks: list[Block] = list(header_blocks)
    for idx, op in enumerate(opinions):
        type_code = op.get("type") or ""
        label = _OPINION_TYPE_LABELS.get(type_code, type_code or "Opinion")
        kind = _CL_TYPE_KIND.get(type_code, "majority")
        author = (op.get("author_str") or "").strip()
        if op.get("per_curiam") and not author:
            author = "Per Curiam"
        if author:
            label = f"{label} ({author})"

        op_blocks, op_footnotes = _opinion_blocks(op, idx)
        if op_blocks:
            parts.append(
                OpinionPart(label=label, kind=kind, blocks=op_blocks, footnotes=op_footnotes)
            )
            blocks.extend(op_blocks)

    plain = _safe_blocks_to_text(blocks)
    return CourtListenerOpinion(title, parts, blocks, plain, cluster)


def plain_case_text(client, item: dict) -> str:
    """Plain-text fallback for downloads or diagnostic views."""
    assembled = assemble_case_parts(client, item)
    if assembled.plain_text.strip():
        return assembled.plain_text
    return "\n\n".join(block.text() for block in assembled.blocks if block.text().strip())


def _fill_missing_court_id(client, item: dict, cluster: dict) -> None:
    if str(item.get("court_id") or item.get("court") or "").strip():
        return
    docket_url = cluster.get("docket")
    if not docket_url:
        return
    try:
        docket = client._get_url(docket_url, {"fields": "court_id"})
    except Exception:
        return
    if docket.get("court_id"):
        item["court_id"] = docket["court_id"]


def _header_blocks(cluster: dict, item: dict, title: str) -> list[Block]:
    blocks: list[Block] = []
    if title:
        blocks.append(Block(kind="center", spans=[Span(text=title, bold=True)]))

    citations = cluster_citations_to_strings(cluster.get("citations") or [])
    if citations:
        blocks.append(Block(kind="center", spans=[Span(text=", ".join(citations))]))

    for field_name, label in (("judges", "Judges"), ("attorneys", "Attorneys")):
        value = _strip_html(cluster.get(field_name) or "")
        if value:
            blocks.append(Block(kind="para", spans=[
                Span(text=f"{label}: ", bold=True),
                Span(text=value),
            ]))

    for field_name, label in (("syllabus", "Syllabus"), ("headnotes", "Headnotes")):
        value = (cluster.get(field_name) or "").strip()
        if not value:
            continue
        parsed, _footnotes = parse_cl_html(value)
        if parsed:
            blocks.append(Block(kind="heading", spans=[Span(text=label, bold=True)]))
            blocks.extend(parsed)
    return blocks


def _fetch_sub_opinions(client, cluster: dict) -> list[dict]:
    opinions: list[dict] = []
    for url in cluster.get("sub_opinions") or []:
        try:
            opinions.append(client._get_url(
                url,
                {"fields": "ordering_key,type,author_str,per_curiam,"
                           "html_with_citations,html,plain_text"},
            ))
        except Exception as exc:
            print(f"[cl-text] failed to fetch sub-opinion {url}: {exc}")
    opinions.sort(key=lambda op: (
        op.get("ordering_key") is None,
        op.get("ordering_key") or 0,
    ))
    return opinions


def _pick_combined_opinion(opinions: list[dict]) -> dict | None:
    def starred(op: dict) -> bool:
        return "star-pagination" in (op.get("html_with_citations") or op.get("html") or "")

    for op in opinions:
        if "combined" in (op.get("type") or "") and starred(op):
            return op
    hits = [op for op in opinions if starred(op)]
    return hits[0] if len(hits) == 1 else None


def _combined_parts(opinion: dict) -> list[OpinionPart]:
    html_text = opinion.get("html_with_citations") or opinion.get("html") or ""
    try:
        blocks = parse_opinion_blocks(html_text)
        parts = segment_blocks(blocks)
        link_footnotes_by_marker(parts)
        return parts
    except Exception as exc:
        print(f"[cl-text] combined opinion parse failed: {exc}")
        return []


def _opinion_blocks(opinion: dict, index: int) -> tuple[list[Block], list[Block]]:
    html_text = opinion.get("html_with_citations") or opinion.get("html") or ""
    if html_text:
        return parse_cl_html(html_text, fn_prefix=f"op{index}_")

    plain = (opinion.get("plain_text") or "").strip()
    if not plain:
        return [], []
    plain = educate_quotes(plain)
    blocks = [
        Block(kind="para", spans=[Span(text=para.strip())])
        for para in re.split(r"\n{2,}", plain)
        if para.strip()
    ]
    return blocks, []


def _safe_blocks_to_text(blocks: list[Block]) -> str:
    try:
        return blocks_to_text(blocks)
    except Exception:
        return "\n\n".join(block.text() for block in blocks if block.text().strip())


def _strip_html(value: str) -> str:
    text = re.sub(
        r"<(br|/p|/div|/h[1-6]|/li|/tr|/blockquote)\b[^>]*>",
        "\n",
        value,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", str(value or "")).strip()
