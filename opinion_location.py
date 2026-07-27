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
from slip_opinion import group_lines


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
    navigation_ready: bool = False
    pdf_page_count: int = 0
    matched_pdf_pages: tuple[int, ...] = ()
    source_edges_covered: bool = False
    physical_edges_covered: bool = False
    direct_page_coverage: float = 0.0
    max_unmatched_pages: int = 0

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
        boundary = max(
            before,
            key=lambda b: (b.source_offset, b.pdf_page),
        ) if before else min(
            self.boundaries,
            key=lambda b: (
                abs(b.source_offset - offset),
                b.pdf_page,
            ),
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
        anchor.page == parsed_native.page
        and not anchor.address.footnote
        for anchor in pages
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


def _plain_page_markers(text: str):
    """Yield star-page markers, excluding ordinary WL/LEXIS star pin cites."""
    for match in _PLAIN_PAGE_RE.finditer(text or ""):
        prefix = text[max(0, match.start() - 80):match.start()]
        if re.search(r"\bat\s*$", prefix, re.IGNORECASE):
            continue
        if re.search(
            r"\b(?:WL|LEXIS)\b[^\n]{0,50},\s*$", prefix, re.IGNORECASE
        ):
            continue
        yield match


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

    for match in _plain_page_markers(text):
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
    if boxes:
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
        # With heavily damaged OCR there may be no exact word or n-gram vote.
        # Probe the whole opinion evenly; insertion-ordered ``most_common(12)``
        # otherwise examines only its beginning and can never reach late pages.
        last = max(0, len(source_values) - min(
            len(source_values), len(page_values)
        ))
        starts = {
            int(round(last * slot / 11.0))
            for slot in range(12)
        } if last else {0}
        for start in sorted(starts):
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


def _dominant_anchor_cluster(
    candidates: list[LocationAnchor],
    source_floor: Optional[int] = None,
) -> list[LocationAnchor]:
    """Return the main-text cluster on a physical PDF page.

    Reporter PDFs leave footnotes at the bottoms of their original pages,
    while text opinions commonly collect those notes after each writing.  A
    raw match can therefore jump into a detached footnote or a repeated running
    head.  Prefer non-footnote matches and the densest nearby source cluster.
    The same filtering is applied to the dense y-level navigation anchors, so
    clicking back from the top of a later PDF page cannot follow its running
    head to the beginning of the opinion.
    """
    if not candidates:
        return []
    body = [
        anchor for anchor in candidates
        if anchor.address is None or not anchor.address.footnote
    ]
    pool = body or candidates
    if source_floor is not None:
        forward = [
            anchor for anchor in pool
            if anchor.source_offset > source_floor + 2
        ]
        if forward:
            pool = forward
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
    return max(
        clusters,
        key=lambda group: (
            len(group),
            sum(anchor.confidence for anchor in group),
            -group[0].source_offset,
        ),
    )


def _dominant_boundary_anchor(
    candidates: list[LocationAnchor],
) -> Optional[LocationAnchor]:
    # Collected notes occur out of physical-page order in the text source.
    # Keep their dense anchors for precise navigation, but never let one claim
    # to be the beginning of the reporter page on which it was printed.
    body = [
        anchor for anchor in candidates
        if anchor.address is None or not anchor.address.footnote
    ]
    cluster = _dominant_anchor_cluster(body)
    return min(cluster, key=lambda anchor: anchor.source_offset) if cluster else None


def _monotonic_boundaries(
    rows: list[PageBoundary], source: TextSource,
) -> tuple[list[PageBoundary], set[int]]:
    """Repair fuzzy page-boundary outliers with a weighted monotonic chain."""
    if len(rows) < 2:
        return rows, set()
    ordered = sorted(rows, key=lambda row: row.pdf_page)
    # Maximum-weight increasing subsequence.  A long run of mutually
    # consistent pages outweighs an attractive isolated match to repeated
    # language elsewhere in the opinion.
    scores: list[float] = []
    previous: list[int] = []
    for i, row in enumerate(ordered):
        # Exact reporter stars are hard constraints.  When they are mutually
        # monotonic, this weight makes every one part of the selected chain
        # while still allowing fuzzy rows between them to be rejected.
        weight = 1_000_000.0 if row.exact else max(
            0.1, float(row.confidence)
        )
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
        if before and after:
            left, right = ordered[before[-1]], ordered[after[0]]
            span = right.pdf_page - left.pdf_page
            fraction = (
                (row.pdf_page - left.pdf_page) / span if span > 0 else 0.0
            )
            source_offset = int(round(
                left.source_offset
                + fraction * (right.source_offset - left.source_offset)
            ))
            confidence = min(left.confidence, right.confidence) * 0.7
        else:
            # A rejected edge outlier cannot be interpolated.  Clamping it to
            # the nearest kept row creates two reporter pages at one source
            # offset and can shift even the trustworthy row's pin cite.
            repaired_pages.add(row.pdf_page)
            continue
        repaired.append(PageBoundary(
            pdf_page=row.pdf_page,
            reporter_page=row.reporter_page,
            source_offset=source_offset,
            address=source.address_at(source_offset),
            confidence=confidence,
            exact=False,
        ))
        repaired_pages.add(row.pdf_page)
    return repaired, repaired_pages


def _complete_page_boundaries(
    rows: list[PageBoundary],
    source: TextSource,
    page_count: int,
    pdf: Optional[ReporterCitation],
) -> tuple[list[PageBoundary], set[int]]:
    """Interpolate pages whose OCR produced no trustworthy text match."""
    if not rows or page_count <= 0:
        return rows, set()
    by_page = {row.pdf_page: row for row in rows}
    known = sorted(by_page)
    inserted: set[int] = set()
    for page_index in range(page_count):
        if page_index in by_page:
            continue
        before = [page for page in known if page < page_index]
        after = [page for page in known if page > page_index]
        # Only a bounded gap has enough information to interpolate.  Clamping
        # unmatched leading/trailing pages to one known source offset creates
        # duplicate boundaries and can assign the wrong reporter page even to
        # the text that actually matched.
        if not before or not after:
            continue
        left, right = by_page[before[-1]], by_page[after[0]]
        span = right.pdf_page - left.pdf_page
        fraction = (
            (page_index - left.pdf_page) / span if span > 0 else 0.0
        )
        source_offset = int(round(
            left.source_offset
            + fraction * (right.source_offset - left.source_offset)
        ))
        confidence = min(left.confidence, right.confidence) * 0.55
        by_page[page_index] = PageBoundary(
            pdf_page=page_index,
            reporter_page=(pdf.page + page_index) if pdf else None,
            source_offset=source_offset,
            address=source.address_at(source_offset),
            confidence=confidence,
            exact=False,
        )
        inserted.add(page_index)
    return [by_page[page] for page in sorted(by_page)], inserted


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
            if anchor.address.footnote:
                # Text renderers collect notes after a writing, whereas the
                # reporter prints them on their original pages.  A star inside
                # that collected note is not a main-text page boundary.
                continue
            page_index = anchor.page - pdf.page
            if 0 <= page_index < len(pdf_pages):
                existing = exact_by_pdf.get(page_index)
                if (
                    existing is None
                    or (not existing.explicit and anchor.explicit)
                ):
                    exact_by_pdf[page_index] = anchor

    dense: list[LocationAnchor] = []
    page_scores: dict[int, float] = {}
    matched_source_tokens: set[int] = set()
    matched_tokens_by_page: dict[int, set[int]] = {}
    source_frontier: Optional[int] = None
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
        reporter_page = pdf.page + page_index if pdf else None
        page_dense: list[LocationAnchor] = []
        page_source_tokens: set[int] = set()
        for pdf_start, source_start, size in best_blocks:
            page_source_tokens.update(range(source_start, source_start + size))
            samples = {0, size - 1}
            samples.update(range(0, size, 10))
            for delta in sorted(samples):
                if delta < 0 or delta >= size:
                    continue
                sw = source_words[source_start + delta]
                pw = best_words[pdf_start + delta]
                page_dense.append(
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
        footnote_dense = [
            anchor for anchor in page_dense
            if anchor.address is not None and anchor.address.footnote
        ]
        main_dense = _dominant_anchor_cluster(
            [
                anchor for anchor in page_dense
                if anchor.address is None or not anchor.address.footnote
            ],
            source_frontier,
        )
        if main_dense:
            page_scores[page_index] = best_score
            lo = min(anchor.source_offset for anchor in main_dense)
            hi = max(anchor.source_offset for anchor in main_dense)
            page_tokens = {
                token for token in page_source_tokens
                if lo <= source_words[token].start <= hi
            }
            matched_tokens_by_page[page_index] = page_tokens
            matched_source_tokens.update(page_tokens)
            dense.extend(main_dense)
            source_frontier = max(
                source_frontier if source_frontier is not None else -1,
                max(anchor.source_offset for anchor in main_dense),
            )
        # A footnote is printed on its original page but commonly collected
        # after the writing in text.  Retain those out-of-order y-level anchors
        # so switching while reading a note still lands on that note.
        dense.extend(
            anchor for anchor in footnote_dense
            if anchor not in main_dense
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
    boundaries, inserted_pages = _complete_page_boundaries(
        boundaries, source, len(pdf_pages or ()), pdf
    )
    repaired_pages.update(inserted_pages)
    if repaired_pages:
        # The page's match was demonstrably out of document order.  Its dense
        # y-level anchors would send PDF→text back to that same false passage;
        # retain the interpolated coarse boundary instead.
        dense = [
            anchor for anchor in dense
            if (
                anchor.pdf_page not in repaired_pages
                or (
                    anchor.address is not None
                    and anchor.address.footnote
                )
            )
        ]
        for page_index in repaired_pages:
            page_scores.pop(page_index, None)
        matched_source_tokens = set().union(*(
            tokens
            for page_index, tokens in matched_tokens_by_page.items()
            if page_index not in repaired_pages
        )) if matched_tokens_by_page else set()

    exact_boundaries = {
        boundary.pdf_page: boundary
        for boundary in boundaries
        if boundary.exact
    }
    if exact_boundaries:
        # An exact star marker is the source start of that physical page.
        # Drop body matches before it (normally a repeated running head), but
        # retain collected-footnote anchors because their source order is
        # intentionally unrelated to their printed page.
        dense = [
            anchor for anchor in dense
            if (
                anchor.pdf_page not in exact_boundaries
                or anchor.address is not None
                and anchor.address.footnote
                or anchor.source_offset
                >= exact_boundaries[anchor.pdf_page].source_offset
            )
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

    page_count = len(pdf_pages or ())
    matched_pdf_pages = tuple(sorted(page_scores))
    physical_coverage = len(matched_pdf_pages) / max(1, page_count)
    body_runs = [
        run for run in source.runs
        if (
            run.address is not None
            and not run.address.footnote
            and run.end > run.start
        )
    ]
    source_edges_covered = False
    if matched_source_tokens and body_runs:
        body_start = min(run.start for run in body_runs)
        body_end = max(run.end for run in body_runs)
        matched_start = min(
            source_words[token].start for token in matched_source_tokens
        )
        matched_end = max(
            source_words[token].end for token in matched_source_tokens
        )
        margin = max(20, int((body_end - body_start) * 0.20))
        source_edges_covered = (
            matched_start <= body_start + margin
            and matched_end >= body_end - margin
        )

    exact_pages = sorted(exact_by_pdf)
    if page_count <= 1:
        exact_ready = bool(exact_pages)
        fuzzy_ready = bool(
            matched_pdf_pages
            and text_coverage >= 0.18
            and mean_score >= 0.22
        )
    else:
        last_page = page_count - 1
        exact_ready = bool(
            len(exact_pages) >= 2
            and exact_pages[0] == 0
            and exact_pages[-1] == last_page
        )
        fuzzy_ready = bool(
            len(matched_pdf_pages) >= 2
            and matched_pdf_pages[0] == 0
            and matched_pdf_pages[-1] == last_page
            and physical_coverage >= 0.20
            and source_edges_covered
            and text_coverage >= 0.18
            and mean_score >= 0.22
        )
    physical_edges_covered = bool(
        page_count <= 1
        or (
            (0 in exact_pages or 0 in matched_pdf_pages)
            and (
                page_count - 1 in exact_pages
                or page_count - 1 in matched_pdf_pages
            )
        )
    )
    navigation_ready = bool(boundaries) and (exact_ready or fuzzy_ready)

    direct_pages = set(exact_pages) | set(matched_pdf_pages)
    direct_page_coverage = len(direct_pages) / max(1, page_count)
    max_unmatched_pages = 0
    current_gap = 0
    for page_index in range(page_count):
        if page_index in direct_pages:
            current_gap = 0
        else:
            current_gap += 1
            max_unmatched_pages = max(max_unmatched_pages, current_gap)
    # Approximate view switching can tolerate a broad interpolated interval.
    # A legal pin cite cannot: page-density differences make long linear gaps
    # capable of assigning the wrong reporter page.  Require most pages to
    # have direct text/exact anchors, with only short bounded gaps, before
    # enabling the inferred reporter for copy.  Very high overall coverage
    # permits one additional missing page in a local OCR-damaged run.
    copy_alignment_ready = bool(
        navigation_ready
        and (
            page_count <= 1
            or (
                (
                    direct_page_coverage >= 0.70
                    and max_unmatched_pages <= 2
                )
                or (
                    direct_page_coverage >= 0.80
                    and max_unmatched_pages <= 3
                )
            )
        )
    )
    pdf_is_us = _same_reporter(pdf, us)
    enough_inferred = copy_alignment_ready
    copy_ready = bool(
        us and pdf_is_us and enough_inferred
    )
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
        navigation_ready=navigation_ready,
        pdf_page_count=page_count,
        matched_pdf_pages=matched_pdf_pages,
        source_edges_covered=source_edges_covered,
        physical_edges_covered=physical_edges_covered,
        direct_page_coverage=direct_page_coverage,
        max_unmatched_pages=max_unmatched_pages,
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
