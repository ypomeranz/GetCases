"""The one process-wide lock serializing every PDFium call.

PDFium is not thread-safe — not even across *different* documents — so any
two threads calling into the C library at the same time can crash the whole
process with an access violation.  The GUI renders pages on the main thread
and scans text layers on worker threads; ``us_reports_pdf`` carves opinions
out of volume PDFs and ``brief_reader`` extracts brief text, both from worker
threads.  Every one of those call sites must hold this lock around each
PDFium call (per page, not per job, so long scans interleave with rendering
instead of freezing it).

Re-entrant, so a helper that takes the lock can be called by a caller that
already holds it.
"""

from __future__ import annotations

import threading

PDFIUM_LOCK = threading.RLock()
