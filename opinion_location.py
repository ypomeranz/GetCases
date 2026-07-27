"""Pure text/PDF location matching for opinion-reader source switches.

The GUI supplies the structured text opinion and the glyph data it already
extracts from the PDF.  This module deliberately knows nothing about tkinter:
its immutable records can be built on a worker thread and translated to Tk
marks on the main thread.

The public entry points are:

``build_text_source(parts, native_cite="")``
    Flatten styled ``OpinionPart`` objects while omitting visible star-page
    glyphs.  Addresses still count those omitted glyphs, so
    ``block-start mark + TextAddress.block_offset`` identifies the source
    character exactly in a rendered block.

``build_plain_text_source(text, native_cite="")``
    The equivalent for an unstructured CourtListener text fallback.

``align_opinion_locations(source, pdf_pages, ...)``
    Match the source against pypdfium's ``[[(char, box), ...], ...]`` pages.
    It considers the PDF stream order, geometry-reconstructed lines, and a
    left-column-then-right-column variant.  Reporter-page markers are hard
    anchors when the source and PDF use the same reporter; otherwise page
    boundaries are inferred from matched opinion language.
"""

from __future__ import annotations

import bisect
import difflib
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable, Optional

import citations
from slip_opinion import group_lines, is_single_column


@dataclass(frozen=True)
class ReporterCitation:
    volume: int
    reporter: str
    reporter_key: str
    page: int

    @property
    def cite(self) -> str:
        return f"{self.volume} {self.reporter} {self.page}"


@dataclass(frozen=True, order=True)
class TextAddress:
    """A character in a structured source block.

    ``block_offset`` is measured in the original concatenation of every span
    in the block, *including* pagenum spans omitted from :class:`TextSource`.
    """

    part_index: int
    footnote: bool
    block_id: int
    block_offset: int


@dataclass(frozen=True)
class TextRun:
    """A contiguous slice of ``TextSource.text`` backed by one source span."""

    start: int
    end: int
    address: Optional[TextAddress]
    source_end: int
    native_page: Optional[int]


@dataclass(frozen=True)
class NativePageAnchor:
    page: int
    source_offset: int
    address: TextAddress
    explicit: bool = True


@dataclass(frozen=True)
class TextSource:
    text: str
    runs: tuple[TextRun, ...]
    native_pages: tuple[NativePageAnchor, ...]
    native_cite: str = ""

    def address_at(self, source_offset: int) -> Optional[TextAddress]:
        """Return the structured address nearest *source_offset*."""
        if not self.runs:
            return None
        source_offset = max(0, min(int(source_offset), len(self.text)))
        candidates = [
            run for run in self.runs
            if run.address is not None and run.start <= source_offset < run.end
        ]
        if candidates:
            run = candidates[0]
        else:
            real = [run for run in self.runs if run.address is not None]
            if not real:
                return None
            run = min(
                real,
                key=lambda r: min(
                    abs(source_offset - r.start), abs(source_offset - r.end)
                ),
            )
        delta = max(0, min(source_offset - run.start, run.end - run.start))
        return TextAddress(
            run.address.part_index,
            run.address.footnote,
            run.address.block_id,
            min(run.address.block_offset + delta, run.source_end),
        )

    def offset_for(self, address: TextAddress) -> Optional[int]:
        """Translate a block-relative address back into flattened text."""
        matches = [
            run for run in self.runs
            if run.address is not None
            and run.address.part_index == address.part_index
            and run.address.footnote == address.footnote
            and run.address.block_id == address.block_id
            and run.address.block_offset <= address.block_offset <= run.source_end
        ]
        if matches:
            run = matches[0]
            return run.start + min(
                address.block_offset - run.address.block_offset,
                run.end - run.start,
            )
        same_block = [
            run for run in self.runs
            if run.address is not None
            and run.address.part_index == address.part_index
            and run.address.footnote == address.footnote
            and run.address.block_id == address.block_id
        ]
        if not same_block:
            return None
        run = min(
            same_block,
            key=lambda r: min(
                abs(address.block_offset - r.address.block_offset),
                abs(address.block_offset - r.source_end),
            ),
        )
        return run.start if address.block_offset <= run.address.block_offset else run.end


@dataclass(frozen=True)
class LocationAnchor:
    source_offset: int
    address: Optional[TextAddress]
    pdf_page: int
    y_pt: Optional[float]
    pdf_char: int
    reporter_page: Optional[int]
    confidence: float


@dataclass(frozen=True)
class PageBoundary:
    pdf_page: int
    reporter_page: Optional[int]
    source_offset: int
    address: Optional[TextAddress]
    confidence: float
    exact: bool = False


@dataclass(frozen=True)
class OpinionLocationMap:
    source: TextSource
    anchors: tuple[LocationAnchor, ...]
    boundaries: tuple[PageBoundary, ...]
    confidence: float
    native_cite: str
    pdf_cite: str
    us_cite: str
    copy_ready: bool
    copy_cite: str

    def _source_offset(self, value: "int | TextAddress") -> Optional[int]:
        if isinstance(value, TextAddress):
            return self.source.offset_for(value)
        try:
            return max(0, min(int(value), len(self.source.text)))
        except (TypeError, ValueError):
            return None

    def pdf_location(
        self, source: "int | TextAddress",
    ) -> Optional[LocationAnchor]:
        """Nearest matched PDF location for a text offset/address."""
        offset = self._source_offset(source)
        if offset is None:
            return None
        if self.anchors:
            return min(
                self.anchors,
                key=lambda a: (
                    abs(a.source_offset - offset),
                    a.source_offset > offset,
                    -a.confidence,
                ),
            )
        if not self.boundaries:
            return None
        boundary = min(
            self.boundaries,
            key=lambda b: (
                abs(b.source_offset - offset),
                b.source_offset > offset,
                -b.confidence,
            ),
        )
        return LocationAnchor(
            boundary.source_offset,
            boundary.address,
            boundary.pdf_page,
            None,
            -1,
            boundary.reporter_page,
            boundary.confidence,
        )

    def text_location(
        self, pdf_page: int, y_pt: Optional[float] = None,
    ) -> Optional[LocationAnchor]:
        """Nearest text location for a physical PDF page/height."""
        candidates = [a for a in self.anchors if a.pdf_page == pdf_page]
        if candidates:
            if y_pt is None:
                return min(candidates, key=lambda a: a.source_offset)
            with_y = [a for a in candidates if a.y_pt is not None]
            if with_y:
                return min(with_y, key=lambda a: abs(float(a.y_pt) - y_pt))
            return min(candidates, key=lambda a: a.source_offset)
        boundary = next(
            (b for b in self.boundaries if b.pdf_page == pdf_page), None
        )
        if boundary is None:
            return None
        return LocationAnchor(
            boundary.source_offset,
            boundary.address,
            boundary.pdf_page,
            None,
            -1,
            boundary.reporter_page,
            boundary.confidence,
        )

    def reporter_page(self, source: "int | TextAddress") -> Optional[int]:
        """Reporter page in effect at a text location."""
        offset = self._source_offset(source)
        if offset is None or not self.boundaries:
            return None
        before = [b for b in self.boundaries if b.source_offset <= offset]
        boundary = max(before, key=lambda b: b.source_offset) if before else min(
            self.boundaries, key=lambda b: abs(b.source_offset - offset)
        )
        return boundary.reporter_page

    def reporter_pages_for_range(
        self, start: "int | TextAddress", end: "int | TextAddress",
    ) -> tuple[int, ...]:
        """Reporter pages touched by a text range, in displayed source order."""
        lo, hi = self._source_offset(start), self._source_offset(end)
        if lo is None or hi is None:
            return ()
        if hi < lo:
            lo, hi = hi, lo
        rows = sorted(self.boundaries, key=lambda b: b.source_offset)
        pages: list[int] = []
        first = self.reporter_page(lo)
        if first is not None:
            pages.append(first)
        for row in rows:
            if lo < row.source_offset < hi and row.reporter_page is not None:
                if not pages or pages[-1] != row.reporter_page:
                    pages.append(row.reporter_page)
        last = self.reporter_page(max(lo, hi - 1))
        if last is not None and (not pages or pages[-1] != last):
            pages.append(last)
        return tuple(pages)


def parse_reporter_cite(value: str) -> Optional[ReporterCitation]:
    match = citations.find_case_citation(value or "", permissive=True)
    if match is None:
        return None
    try:
        volume, page = int(match.group(1)), int(match.group(3))
    except (TypeError, ValueError):
        return None
    reporter = citations.canonical_reporter(match.group(2))
    key = citations.reporter_key(reporter)
    if not key:
        return None
    return ReporterCitation(volume, reporter, key, page)


def _same_reporter(
    left: Optional[ReporterCitation], right: Optional[ReporterCitation],
) -> bool:
    return bool(
        left and right
        and left.volume == right.volume
        and left.reporter_key == right.reporter_key
    )


def _append_separator(
    pieces: list[str], runs: list[TextRun], native_page: Optional[int],
    start: int, value: str = "\n\n",
) -> int:
    pieces.append(value)
    runs.append(TextRun(start, start + len(value), None, 0, native_page))
    return start + len(value)


def build_text_source(parts, native_cite: str = "") -> TextSource:
    """Build a location-aware text stream from styled opinion parts."""
    parsed_native = parse_reporter_cite(native_cite)
    current_page = parsed_native.page if parsed_native else None
    pieces: list[str] = []
    runs: list[TextRun] = []
    pages: list[NativePageAnchor] = []
    source_length = 0

    def total() -> int:
        return source_length

    def add_separator(value: str = "\n\n") -> None:
        nonlocal source_length
        source_length = _append_separator(
            pieces, runs, current_page, source_length, value
        )

    def add_block(block, part_index: int, footnote: bool) -> None:
        nonlocal current_page, source_length
        local_offset = 0
        block_id = id(block)
        for span in getattr(block, "spans", None) or ():
            value = str(getattr(span, "text", "") or "")
            address = TextAddress(
                part_index, footnote, block_id, local_offset
            )
            if getattr(span, "pagenum", False):
                match = re.search(r"\d+", value)
                if match:
                    current_page = int(match.group(0))
                    pages.append(
                        NativePageAnchor(current_page, total(), address, True)
                    )
                # Preserve a word boundary without preserving the marker.
                if pieces and pieces[-1] and not pieces[-1][-1].isspace():
                    add_separator(" ")
                local_offset += len(value)
                continue
            if value:
                start = total()
                pieces.append(value)
                source_length += len(value)
                runs.append(
                    TextRun(
                        start,
                        start + len(value),
                        address,
                        local_offset + len(value),
                        current_page,
                    )
                )
            local_offset += len(value)
        add_separator()

    for part_index, part in enumerate(parts or ()):
        for block in getattr(part, "blocks", None) or ():
            add_block(block, part_index, False)
        footnotes = getattr(part, "footnotes", None) or ()
        if footnotes:
            add_separator()
            for block in footnotes:
                add_block(block, part_index, True)
        add_separator()

    text = "".join(pieces)
    real_runs = [run for run in runs if run.address is not None]
    if parsed_native and real_runs and not any(
        anchor.page == parsed_native.page for anchor in pages
    ):
        pages.insert(
            0,
            NativePageAnchor(
                parsed_native.page,
                real_runs[0].start,
                real_runs[0].address,
                False,
            ),
        )
    return TextSource(text, tuple(runs), tuple(pages), native_cite)


_PLAIN_PAGE_RE = re.compile(r"(?<!\*)\*(\d{1,5})\b")


def build_plain_text_source(text: str, native_cite: str = "") -> TextSource:
    """Build a source for raw CourtListener text.

    Bare ``*N`` markers are treated like styled pagenums and their lengths
    remain part of ``TextAddress.block_offset``.
    """
    text = text or ""
    parsed_native = parse_reporter_cite(native_cite)
    current_page = parsed_native.page if parsed_native else None
    pieces: list[str] = []
    runs: list[TextRun] = []
    pages: list[NativePageAnchor] = []
    pos = 0
    source_length = 0

    def total() -> int:
        return source_length

    def add_separator(value: str = "\n\n") -> None:
        nonlocal source_length
        source_length = _append_separator(
            pieces, runs, current_page, source_length, value
        )

    for match in _PLAIN_PAGE_RE.finditer(text):
        if match.start() > pos:
            value = text[pos:match.start()]
            start = total()
            pieces.append(value)
            source_length += len(value)
            runs.append(
                TextRun(
                    start, start + len(value),
                    TextAddress(0, False, 0, pos),
                    match.start(), current_page,
                )
            )
        current_page = int(match.group(1))
        pages.append(
            NativePageAnchor(
                current_page,
                total(),
                TextAddress(0, False, 0, match.start()),
                True,
            )
        )
        if pieces and pieces[-1] and not pieces[-1][-1].isspace():
            add_separator(" ")
        pos = match.end()
    if pos < len(text):
        value = text[pos:]
        start = total()
        pieces.append(value)
        source_length += len(value)
        runs.append(
            TextRun(
                start, start + len(value),
                TextAddress(0, False, 0, pos),
                len(text), current_page,
            )
        )
    if parsed_native and runs and not any(
        anchor.page == parsed_native.page for anchor in pages
    ):
        address = next(
            (run.address for run in runs if run.address is not None),
            TextAddress(0, False, 0, 0),
        )
        pages.insert(
            0,
            NativePageAnchor(parsed_native.page, 0, address, False),
        )
    return TextSource("".join(pieces), tuple(runs), tuple(pages), native_cite)


@dataclass(frozen=True)
class _Word:
    value: str
    start: int
    end: int
    y: Optional[float] = None
    char_index: int = -1


_LIGATURES = {
    "ﬀ": "ff", "ﬁ": "fi", "ﬂ": "fl", "ﬃ": "ffi", "ﬄ": "ffl", "ſ": "s",
}


def _words(
    text: str,
    coords: Optional[list[tuple[Optional[float], int]]] = None,
) -> tuple[_Word, ...]:
    """OCR-tolerant word normalization with original-coordinate retention."""
    folded: list[str] = []
    origins: list[int] = []
    i = 0
    while i < len(text):
        ch = text[i]
        # A printed line-wrap hyphen is not part of the word.
        if ch in "-‐‑" and i + 1 < len(text):
            j = i + 1
            saw_break = False
            while j < len(text) and text[j].isspace():
                saw_break = saw_break or text[j] in "\r\n"
                j += 1
            if saw_break and j < len(text) and text[j].isalpha():
                i = j
                continue
        replacement = _LIGATURES.get(ch, ch)
        replacement = unicodedata.normalize("NFKD", replacement).casefold()
        for out in replacement:
            if unicodedata.combining(out):
                continue
            if out.isascii() and out.isalnum():
                folded.append(out)
                origins.append(i)
            elif out in "'’.":
                # OCR commonly loses these; joining is more stable than making
                # an extra one-letter token ("Court's" -> "courts").
                continue
            else:
                folded.append(" ")
                origins.append(i)
        i += 1
    normalized = "".join(folded)
    out: list[_Word] = []
    for match in re.finditer(r"[a-z0-9]+", normalized):
        first = origins[match.start()]
        last = origins[match.end() - 1]
        y = None
        char_index = first
        if coords:
            values = [
                coords[k][0]
                for k in range(first, min(last + 1, len(coords)))
                if coords[k][0] is not None
            ]
            y = max(values) if values else None
            char_index = coords[first][1] if first < len(coords) else -1
        out.append(_Word(match.group(0), first, last + 1, y, char_index))
    return tuple(out)


def _line_variant(lines) -> tuple[_Word, ...]:
    text_parts: list[str] = []
    coords: list[tuple[Optional[float], int]] = []
    for line in lines:
        if text_parts:
            text_parts.append("\n")
            coords.append((None, -1))
        text_parts.append(line.text)
        coords.extend([(line.y, -1)] * len(line.text))
    return _words("".join(text_parts), coords)


def _page_variants(chars: list) -> tuple[tuple[_Word, ...], ...]:
    variants: list[tuple[_Word, ...]] = []
    raw_text: list[str] = []
    raw_coords: list[tuple[Optional[float], int]] = []
    for index, (chunk, box) in enumerate(chars or ()):
        chunk = str(chunk or "")
        raw_text.append(chunk)
        y = float(box[3]) if box is not None else None
        raw_coords.extend([(y, index)] * len(chunk))
    if raw_text:
        variants.append(_words("".join(raw_text), raw_coords))

    lines = group_lines(chars or [])
    if lines:
        variants.append(_line_variant(lines))

    boxes = [box for _ch, box in (chars or ()) if box is not None]
    if boxes and not is_single_column(chars or []):
        x0 = min(box[0] for box in boxes)
        x1 = max(box[2] for box in boxes)
        midpoint = (x0 + x1) / 2.0
        left = [
            (ch, box) for ch, box in chars
            if box is not None and (box[0] + box[2]) / 2.0 < midpoint
        ]
        right = [
            (ch, box) for ch, box in chars
            if box is not None and (box[0] + box[2]) / 2.0 >= midpoint
        ]
        if left and right:
            variants.append(
                _line_variant(group_lines(left) + group_lines(right))
            )

    unique: list[tuple[_Word, ...]] = []
    seen: set[tuple[str, ...]] = set()
    for variant in variants:
        key = tuple(word.value for word in variant)
        if key and key not in seen:
            seen.add(key)
            unique.append(variant)
    return tuple(unique)


def _source_token_index(words: tuple[_Word, ...], source_offset: int) -> int:
    starts = [word.start for word in words]
    return max(0, bisect.bisect_right(starts, source_offset) - 1)


def _alignment_candidates(
    source_words: tuple[_Word, ...],
    page_words: tuple[_Word, ...],
    hints: Iterable[int] = (),
) -> list[int]:
    source_values = tuple(word.value for word in source_words)
    page_values = tuple(word.value for word in page_words)
    votes: Counter[int] = Counter()

    width = 4 if min(len(source_values), len(page_values)) >= 8 else 2
    grams: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for i in range(max(0, len(source_values) - width + 1)):
        grams[source_values[i:i + width]].append(i)
    for i in range(max(0, len(page_values) - width + 1)):
        positions = grams.get(page_values[i:i + width], ())
        if len(positions) <= 8:
            for pos in positions:
                votes[pos - i] += 6

    occurrences: dict[str, list[int]] = defaultdict(list)
    for i, value in enumerate(source_values):
        if len(value) >= 4:
            occurrences[value].append(i)
    for i, value in enumerate(page_values):
        positions = occurrences.get(value, ())
        if positions and len(positions) <= 8:
            for pos in positions:
                # Small bins tolerate one OCR insertion/deletion.
                delta = pos - i
                votes[int(round(delta / 4.0)) * 4] += 1

    for source_offset in hints:
        votes[_source_token_index(source_words, source_offset)] += 10
    if not votes:
        step = max(1, len(page_values) // 2)
        for start in range(0, len(source_values), step):
            votes[start] = 1
    return [delta for delta, _score in votes.most_common(12)]


def _align_variant(
    source_words: tuple[_Word, ...],
    page_words: tuple[_Word, ...],
    hints: Iterable[int] = (),
) -> tuple[float, tuple[tuple[int, int, int], ...]]:
    if len(source_words) < 2 or len(page_words) < 2:
        return 0.0, ()
    source_values = tuple(word.value for word in source_words)
    page_values = tuple(word.value for word in page_words)
    best_score = 0.0
    best_blocks: tuple[tuple[int, int, int], ...] = ()
    margin = max(12, min(40, len(page_values) // 4))
    for delta in _alignment_candidates(source_words, page_words, hints):
        lo = max(0, delta - margin)
        hi = min(len(source_values), delta + len(page_values) + margin)
        if hi - lo < 2:
            continue
        matcher = difflib.SequenceMatcher(
            None, page_values, source_values[lo:hi], autojunk=False
        )
        blocks = tuple(
            (block.a, lo + block.b, block.size)
            for block in matcher.get_matching_blocks()
            if block.size
        )
        matched = sum(size for _p, _s, size in blocks)
        longest = max((size for _p, _s, size in blocks), default=0)
        score = (
            0.85 * matched / max(1, len(page_values))
            + 0.15 * min(1.0, longest / 8.0)
        )
        if score > best_score:
            best_score, best_blocks = score, blocks
    return best_score, best_blocks


def _dominant_boundary_anchor(
    candidates: list[LocationAnchor],
) -> Optional[LocationAnchor]:
    """Choose the main-text cluster that begins a physical PDF page.

    Reporter PDFs leave footnotes at the bottoms of their original pages,
    while text opinions commonly collect those notes after each writing.  A
    raw minimum source offset can therefore jump the page boundary into a
    detached footnote (or a repeated running head).  Prefer non-footnote
    matches and the densest nearby source cluster, then take its earliest
    source word.
    """
    if not candidates:
        return None
    body = [
        anchor for anchor in candidates
        if anchor.address is None or not anchor.address.footnote
    ]
    pool = body or candidates
    ordered = sorted(pool, key=lambda anchor: anchor.source_offset)
    clusters: list[list[LocationAnchor]] = []
    for anchor in ordered:
        if (
            not clusters
            or anchor.source_offset - clusters[-1][-1].source_offset > 2500
        ):
            clusters.append([anchor])
        else:
            clusters[-1].append(anchor)
    cluster = max(
        clusters,
        key=lambda group: (
            len(group),
            sum(anchor.confidence for anchor in group),
            -group[0].source_offset,
        ),
    )
    return min(cluster, key=lambda anchor: anchor.source_offset)


def _monotonic_boundaries(
    rows: list[PageBoundary], source: TextSource,
) -> tuple[list[PageBoundary], set[int]]:
    """Repair fuzzy page-boundary outliers with a weighted monotonic chain."""
    if len(rows) < 3 or any(row.exact for row in rows):
        return rows, set()
    ordered = sorted(rows, key=lambda row: row.pdf_page)
    # Maximum-weight increasing subsequence.  A long run of mutually
    # consistent pages outweighs an attractive isolated match to repeated
    # language elsewhere in the opinion.
    scores: list[float] = []
    previous: list[int] = []
    for i, row in enumerate(ordered):
        weight = max(0.1, float(row.confidence))
        best_score = weight
        best_previous = -1
        for j in range(i):
            if ordered[j].source_offset > row.source_offset:
                continue
            candidate = scores[j] + weight
            if candidate > best_score:
                best_score = candidate
                best_previous = j
        scores.append(best_score)
        previous.append(best_previous)
    cursor = max(range(len(ordered)), key=lambda i: scores[i])
    kept: set[int] = set()
    while cursor >= 0:
        kept.add(cursor)
        cursor = previous[cursor]
    if len(kept) == len(ordered):
        return ordered, set()

    repaired_pages: set[int] = set()
    repaired: list[PageBoundary] = []
    kept_order = sorted(kept)
    for i, row in enumerate(ordered):
        if i in kept:
            repaired.append(row)
            continue
        before = [j for j in kept_order if j < i]
        after = [j for j in kept_order if j > i]
        if not before or not after:
            # Do not extrapolate beyond the reliable matched run.
            repaired.append(row)
            continue
        left, right = ordered[before[-1]], ordered[after[0]]
        span = right.pdf_page - left.pdf_page
        if span <= 0:
            repaired.append(row)
            continue
        fraction = (row.pdf_page - left.pdf_page) / span
        source_offset = int(round(
            left.source_offset
            + fraction * (right.source_offset - left.source_offset)
        ))
        repaired.append(PageBoundary(
            pdf_page=row.pdf_page,
            reporter_page=row.reporter_page,
            source_offset=source_offset,
            address=source.address_at(source_offset),
            confidence=min(left.confidence, right.confidence) * 0.7,
            exact=False,
        ))
        repaired_pages.add(row.pdf_page)
    return repaired, repaired_pages


def align_opinion_locations(
    source: TextSource,
    pdf_pages: list,
    *,
    pdf_cite: str = "",
    native_cite: str = "",
    us_cite: str = "",
) -> OpinionLocationMap:
    """Align an opinion source with extracted PDF glyph pages."""
    native_cite = native_cite or source.native_cite
    native = parse_reporter_cite(native_cite)
    us = parse_reporter_cite(us_cite)
    pdf = parse_reporter_cite(pdf_cite) or us
    effective_pdf_cite = pdf.cite if pdf else pdf_cite
    source_words = _words(
        source.text,
        [(None, index) for index in range(len(source.text))],
    )

    exact_by_pdf: dict[int, NativePageAnchor] = {}
    if _same_reporter(native, pdf):
        for anchor in source.native_pages:
            page_index = anchor.page - pdf.page
            if 0 <= page_index < len(pdf_pages):
                exact_by_pdf.setdefault(page_index, anchor)

    dense: list[LocationAnchor] = []
    page_scores: dict[int, float] = {}
    matched_source_tokens: set[int] = set()
    for page_index, chars in enumerate(pdf_pages or ()):
        hints = []
        exact = exact_by_pdf.get(page_index)
        if exact is not None:
            hints.append(exact.source_offset)
        best_score = 0.0
        best_words: tuple[_Word, ...] = ()
        best_blocks: tuple[tuple[int, int, int], ...] = ()
        for variant in _page_variants(chars):
            score, blocks = _align_variant(source_words, variant, hints)
            if score > best_score:
                best_score, best_words, best_blocks = score, variant, blocks
        matched = sum(size for _p, _s, size in best_blocks)
        minimum = 3 if len(best_words) < 20 else 5
        if matched < minimum or best_score < 0.16:
            continue
        page_scores[page_index] = best_score
        reporter_page = pdf.page + page_index if pdf else None
        for pdf_start, source_start, size in best_blocks:
            matched_source_tokens.update(range(source_start, source_start + size))
            samples = {0, size - 1}
            samples.update(range(0, size, 10))
            for delta in sorted(samples):
                if delta < 0 or delta >= size:
                    continue
                sw = source_words[source_start + delta]
                pw = best_words[pdf_start + delta]
                dense.append(
                    LocationAnchor(
                        sw.start,
                        source.address_at(sw.start),
                        page_index,
                        pw.y,
                        pw.char_index,
                        reporter_page,
                        best_score,
                    )
                )

    # One hard boundary per exact marker; otherwise the first confidently
    # matched source word on the physical PDF page.
    boundaries: list[PageBoundary] = []
    for page_index in range(len(pdf_pages or ())):
        reporter_page = pdf.page + page_index if pdf else None
        exact = exact_by_pdf.get(page_index)
        if exact is not None:
            boundaries.append(
                PageBoundary(
                    page_index,
                    reporter_page,
                    exact.source_offset,
                    exact.address,
                    1.0,
                    True,
                )
            )
            continue
        candidates = [a for a in dense if a.pdf_page == page_index]
        first = _dominant_boundary_anchor(candidates)
        if first is not None:
            boundaries.append(
                PageBoundary(
                    page_index,
                    reporter_page,
                    first.source_offset,
                    first.address,
                    page_scores.get(page_index, first.confidence),
                    False,
                )
            )

    boundaries, repaired_pages = _monotonic_boundaries(boundaries, source)
    if repaired_pages:
        # The page's match was demonstrably out of document order.  Its dense
        # y-level anchors would send PDF→text back to that same false passage;
        # retain the interpolated coarse boundary instead.
        dense = [
            anchor for anchor in dense
            if anchor.pdf_page not in repaired_pages
        ]

    # Hard boundaries also act as coarse navigation anchors when exact text
    # matching on a page is poor.
    dense_keys = {
        (anchor.source_offset, anchor.pdf_page, anchor.y_pt) for anchor in dense
    }
    for boundary in boundaries:
        key = (boundary.source_offset, boundary.pdf_page, None)
        if key not in dense_keys:
            dense.append(
                LocationAnchor(
                    boundary.source_offset,
                    boundary.address,
                    boundary.pdf_page,
                    None,
                    -1,
                    boundary.reporter_page,
                    boundary.confidence,
                )
            )

    dense.sort(key=lambda a: (a.source_offset, a.pdf_page, -(a.y_pt or 0.0)))
    boundaries.sort(key=lambda b: (b.source_offset, b.pdf_page))
    exact_ratio = len(exact_by_pdf) / max(1, len(source.native_pages))
    text_coverage = len(matched_source_tokens) / max(1, len(source_words))
    mean_score = (
        sum(page_scores.values()) / len(page_scores) if page_scores else 0.0
    )
    confidence = min(
        1.0,
        max(exact_ratio, 0.55 * mean_score + 0.45 * text_coverage),
    )

    pdf_is_us = _same_reporter(pdf, us)
    source_is_us = _same_reporter(native, us)
    enough_inferred = bool(boundaries) and (
        source_is_us
        or (text_coverage >= 0.18 and mean_score >= 0.22)
    )
    copy_ready = bool(us and pdf_is_us and enough_inferred)
    return OpinionLocationMap(
        source=source,
        anchors=tuple(dense),
        boundaries=tuple(boundaries),
        confidence=confidence,
        native_cite=native.cite if native else native_cite,
        pdf_cite=effective_pdf_cite,
        us_cite=us.cite if us else us_cite,
        copy_ready=copy_ready,
        copy_cite=us.cite if copy_ready and us else "",
    )


__all__ = [
    "ReporterCitation",
    "TextAddress",
    "TextRun",
    "NativePageAnchor",
    "TextSource",
    "LocationAnchor",
    "PageBoundary",
    "OpinionLocationMap",
    "parse_reporter_cite",
    "build_text_source",
    "build_plain_text_source",
    "align_opinion_locations",
]
