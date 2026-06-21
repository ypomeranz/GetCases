"""Plain-text extraction for uploaded briefs (``.docx``, ``.rtf``, ``.doc``).

Used by GetCases' "Open Brief…" feature: the extracted text is scanned for
citations (see :mod:`citations`) and shown with every case/statute/rule cite
highlighted and clickable.

Design notes
------------
* ``.docx`` is an Open-Packaging zip; the body text lives in
  ``word/document.xml``.  We pull the ``<w:t>`` runs (with ``<w:tab>``/``<w:br>``
  / paragraph breaks) using only the standard library, so no third-party
  dependency is required.
* ``.rtf`` is parsed with the well-known striprtf control-word state machine —
  again stdlib only.
* ``.doc`` (the legacy OLE binary) has no clean stdlib reader.  We try common
  command-line converters (``antiword``/``catdoc``/LibreOffice) when present and
  fall back to a crude printable-run extraction, which is lossy on formatting
  but still surfaces the (ASCII) citations the feature cares about.
* ``.txt`` is read directly.

Run ``python3 brief_reader.py`` for an offline self-test (exit 0 = pass).
"""

from __future__ import annotations

import os
import re
import subprocess
import zipfile
from xml.etree import ElementTree as ET

__all__ = ["extract_text", "SUPPORTED_EXTS"]

SUPPORTED_EXTS = (".docx", ".rtf", ".doc", ".txt")


def extract_text(path: str) -> str:
    """Return the plain text of a brief, dispatching on file extension.

    Raises ``ValueError`` for an unsupported type and the underlying error
    (e.g. a bad zip) when a known type can't be read.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == ".docx":
        return _docx_text(path)
    if ext == ".rtf":
        with open(path, "rb") as fh:
            raw = fh.read()
        return rtf_to_text(raw.decode("latin-1", "replace"))
    if ext == ".doc":
        return _doc_text(path)
    if ext in (".txt", ".text"):
        with open(path, "rb") as fh:
            return fh.read().decode("utf-8", "replace")
    raise ValueError(
        f"Unsupported brief format {ext!r}. "
        "Use a .docx, .rtf, .doc or .pdf file."
    )


# ---------------------------------------------------------------------------
# .docx
# ---------------------------------------------------------------------------

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def _docx_text(path: str) -> str:
    """Concatenate paragraph text from a .docx's word/document.xml."""
    with zipfile.ZipFile(path) as zf:
        try:
            xml = zf.read("word/document.xml")
        except KeyError as exc:  # not a Word document
            raise ValueError("this .docx has no word/document.xml") from exc
    root = ET.fromstring(xml)
    paras: list[str] = []
    for p in root.iter(_W + "p"):
        runs: list[str] = []
        for node in p.iter():
            tag = node.tag
            if tag == _W + "t":
                runs.append(node.text or "")
            elif tag == _W + "tab":
                runs.append("\t")
            elif tag in (_W + "br", _W + "cr"):
                runs.append("\n")
        paras.append("".join(runs))
    return "\n".join(paras)


# ---------------------------------------------------------------------------
# .rtf  (striprtf control-word state machine)
# ---------------------------------------------------------------------------

# Control words whose group content is metadata, not body text — their groups
# are skipped entirely.
_RTF_DESTINATIONS = frozenset((
    "aftncn", "aftnsep", "aftnsepc", "annotation", "atnauthor", "atndate",
    "atnicn", "atnid", "atnparent", "atnref", "atntime", "atrfend",
    "atrfstart", "author", "background", "bkmkcolf", "bkmkcoll", "bkmkend",
    "bkmkstart", "blipuid", "buptim", "category", "colorschememapping",
    "colortbl", "comment", "company", "creatim", "datafield", "datastore",
    "defchp", "defpap", "do", "doccomm", "docvar", "dptxbxtext", "ebcend",
    "ebcstart", "factoidname", "falt", "fchars", "ffdeftext", "ffentrymcr",
    "ffexitmcr", "ffformat", "ffhelptext", "ffl", "ffname", "ffstattext",
    "field", "file", "filetbl", "fldinst", "fldrslt", "fldtype", "fname",
    "fontemb", "fontfile", "fonttbl", "footer", "footerf", "footerl",
    "footerr", "footnote", "formfield", "ftncn", "ftnsep", "ftnsepc",
    "g", "generator", "gridtbl", "header", "headerf", "headerl", "headerr",
    "hl", "hlfr", "hlinkbase", "hlloc", "hlsrc", "hsv", "htmltag", "info",
    "keycode", "keywords", "latentstyles", "lchars", "levelnumbers",
    "leveltext", "lfolevel", "linkval", "list", "listlevel", "listname",
    "listoverride", "listoverridetable", "listpicture", "liststylename",
    "listtable", "listtext", "lsdlockedexcept", "macc", "maccPr", "mailmerge",
    "maln", "malnScr", "manager", "margPr", "mbrk", "mbrkBin", "mbrkBinSub",
    "mcGp", "mcGpRule", "mcs", "mcsr", "mctr", "mdiff", "mdispDef", "mdom",
    "mdPr", "me", "mendChr", "mf", "mfName", "mfPr", "mformPr", "mfr", "mhideBot",
    "mhideLeft", "mhideRight", "mhideTop", "mhtmltag", "mlim", "mlimloc",
    "mlimlow", "mlimlowPr", "mlimupp", "mlimuppPr", "mm", "mmaddfieldname",
    "mmath", "mmathPict", "mmathPr", "mmaxdist", "mmc", "mmcJc", "mmconnectstr",
    "mmconnectstrdata", "mmcPr", "mmcs", "mmdatasource", "mmheadersource",
    "mmmailsubject", "mmodso", "mmodsofilter", "mmodsofldmpdata",
    "mmodsomappedname", "mmodsoname", "mmodsorecipdata", "mmodsosort",
    "mmodsosrc", "mmodsotable", "mmodsoudl", "mmodsoudldata", "mmodsouniquetag",
    "mmPr", "mmquery", "mmr", "mnary", "mnaryPr", "mnoBreak", "mnum", "mobjDist",
    "moMath", "moMathPara", "moMathParaPr", "mopEmu", "mphant", "mphantPr",
    "mplcHide", "mpos", "mr", "mrad", "mradPr", "mrPr", "msepChr", "mshow",
    "mshp", "msPre", "msPrePr", "msSub", "msSubPr", "msSubSup", "msSubSupPr",
    "msSup", "msSupPr", "mstrikeBLTR", "mstrikeH", "mstrikeTLBR", "mstrikeV",
    "msub", "msubHide", "msup", "msupHide", "mtransp", "mtype", "mvertJc",
    "mvfmf", "mvfml", "mvtof", "mvtol", "mzeroAsc", "mzeroDesc", "mzeroWid",
    "nesttableprops", "nextfile", "nonesttables", "objalias", "objclass",
    "objdata", "object", "objname", "objsect", "objtime", "oldcprops",
    "oldpprops", "oldsprops", "oldtprops", "oleclsid", "operator", "panose",
    "password", "passwordhash", "pgp", "pgptbl", "picprop", "pict", "pn",
    "pnseclvl", "pntext", "pntxta", "pntxtb", "printim", "private", "propname",
    "protend", "protstart", "protusertbl", "pxe", "result", "revtbl",
    "revtim", "rsidtbl", "rxe", "shp", "shpgrp", "shpinst", "shppict",
    "shprslt", "shptxt", "sn", "sp", "staticval", "stylesheet", "subject",
    "sv", "svb", "tc", "template", "themedata", "title", "txe", "ud", "upr",
    "userprops", "wgrffmtfilter", "windowcaption", "writereservation",
    "writereservhash", "xe", "xform", "xmlattrname", "xmlattrvalue", "xmlclose",
    "xmlname", "xmlnstbl", "xmlopen",
))

# Control words that map to a literal character / break.
_RTF_SPECIALCHARS = {
    "par": "\n", "sect": "\n\n", "page": "\n\n", "line": "\n", "tab": "\t",
    "emdash": "—", "endash": "–", "emspace": " ",
    "enspace": " ", "qmspace": " ", "bullet": "•",
    "lquote": "‘", "rquote": "’", "ldblquote": "“",
    "rdblquote": "”",
}

_RTF_PATTERN = re.compile(
    r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-fA-F]{2})|\\([^a-z])|"
    r"([{}])|[\r\n]+|(.)",
    re.IGNORECASE,
)


def rtf_to_text(text: str) -> str:
    """Convert an RTF string to plain text (striprtf algorithm)."""
    stack: list[tuple[int, bool]] = []
    ignorable = False       # whether current group is a skipped destination
    ucskip = 1              # chars to skip after a \\uN unicode escape
    curskip = 0             # chars still to skip
    out: list[str] = []
    for match in _RTF_PATTERN.finditer(text):
        word, arg, hexv, char, brace, tchar = match.groups()
        if brace:
            curskip = 0
            if brace == "{":
                stack.append((ucskip, ignorable))
            elif brace == "}":
                if stack:
                    ucskip, ignorable = stack.pop()
        elif char:  # \\x escaped control symbol
            curskip = 0
            if char == "~":
                if not ignorable:
                    out.append(" ")
            elif char in "{}\\":
                if not ignorable:
                    out.append(char)
            elif char == "*":
                ignorable = True
        elif word:
            curskip = 0
            if word in _RTF_DESTINATIONS:
                ignorable = True
            elif word in _RTF_SPECIALCHARS:
                if not ignorable:
                    out.append(_RTF_SPECIALCHARS[word])
            elif word == "uc":
                ucskip = int(arg) if arg else 1
            elif word == "u":
                c = int(arg) if arg else 0
                if c < 0:
                    c += 0x10000
                if not ignorable:
                    try:
                        out.append(chr(c))
                    except (ValueError, OverflowError):
                        pass
                curskip = ucskip
        elif hexv is not None:
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                out.append(bytes([int(hexv, 16)]).decode("cp1252", "replace"))
        elif tchar:
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                out.append(tchar)
    return "".join(out)


# ---------------------------------------------------------------------------
# .doc  (legacy OLE binary — best effort)
# ---------------------------------------------------------------------------

def _doc_text(path: str) -> str:
    """Extract text from a legacy Word .doc, trying external converters first
    and falling back to a crude printable-run scan."""
    for tool in (["antiword", path], ["catdoc", "-w", path]):
        text = _run_converter(tool)
        if text and text.strip():
            return text
    text = _libreoffice_convert(path)
    if text and text.strip():
        return text
    # Last resort: pull readable runs straight out of the binary.  Formatting
    # is lost, but the citations (ASCII) survive, which is what matters here.
    with open(path, "rb") as fh:
        data = fh.read()
    runs = _printable_runs(data)
    if not runs.strip():
        raise ValueError(
            "Could not read this .doc.  Install 'antiword' or LibreOffice, or "
            "re-save the brief as .docx, .rtf or .pdf."
        )
    return runs


def _run_converter(cmd: list[str]) -> str:
    try:
        proc = subprocess.run(
            cmd, capture_output=True, timeout=45, check=False,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.decode("utf-8", "replace")


def _libreoffice_convert(path: str) -> str:
    import shutil
    import tempfile

    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        return ""
    with tempfile.TemporaryDirectory() as outdir:
        try:
            proc = subprocess.run(
                [soffice, "--headless", "--convert-to", "txt:Text",
                 "--outdir", outdir, path],
                capture_output=True, timeout=90, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return ""
        if proc.returncode != 0:
            return ""
        stem = os.path.splitext(os.path.basename(path))[0]
        txt_path = os.path.join(outdir, stem + ".txt")
        if not os.path.exists(txt_path):
            return ""
        with open(txt_path, "rb") as fh:
            return fh.read().decode("utf-8", "replace")


def _printable_runs(data: bytes, min_len: int = 4) -> str:
    """Recover readable text from a binary .doc: take both the cp1252 byte
    stream and the UTF-16LE stream, keep runs of printable characters, and
    return whichever yields more text."""
    candidates = []
    for decoded in (data.decode("cp1252", "ignore"),
                    data.decode("utf-16-le", "ignore")):
        runs = re.findall(r"[ -~ -ɏ‐-‟]{%d,}" % min_len,
                          decoded)
        candidates.append("\n".join(r.strip() for r in runs if r.strip()))
    return max(candidates, key=len) if candidates else ""


# ---------------------------------------------------------------------------
# Offline self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover - offline smoke test
    import io
    import sys
    import tempfile

    failures = 0

    # --- .rtf ---
    rtf = (
        r"{\rtf1\ansi\deff0 {\fonttbl{\f0 Times;}}"
        r"The Court cited {\i Roe v. Wade}, 410 U.S. 113 (1973).\par "
        r"See 42 U.S.C. \'a7 1983.\par}"
    )
    rtf_out = rtf_to_text(rtf)
    if "Roe v. Wade" not in rtf_out or "410 U.S. 113" not in rtf_out:
        print("RTF FAIL:", repr(rtf_out)); failures += 1
    if "§ 1983" not in rtf_out and "1983" not in rtf_out:
        print("RTF section-sign FAIL:", repr(rtf_out)); failures += 1
    # The fonttbl destination must not leak into the body.
    if "Times" in rtf_out:
        print("RTF leaked fonttbl:", repr(rtf_out)); failures += 1

    # --- .docx (built in-memory) ---
    doc_xml = (
        '<?xml version="1.0"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/'
        'wordprocessingml/2006/main"><w:body>'
        '<w:p><w:r><w:t>The defendant relied on </w:t></w:r>'
        '<w:r><w:t xml:space="preserve">29 C.F.R. </w:t></w:r>'
        '<w:r><w:t>&#167; 1614.105.</w:t></w:r></w:p>'
        '<w:p><w:r><w:t>See Fed. R. Civ. P. 56.</w:t></w:r></w:p>'
        '</w:body></w:document>'
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("word/document.xml", doc_xml)
    with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tf:
        tf.write(buf.getvalue())
        docx_path = tf.name
    try:
        docx_out = extract_text(docx_path)
    finally:
        os.unlink(docx_path)
    if "29 C.F.R. § 1614.105" not in docx_out:
        print("DOCX FAIL:", repr(docx_out)); failures += 1
    if "Fed. R. Civ. P. 56" not in docx_out:
        print("DOCX paragraph break FAIL:", repr(docx_out)); failures += 1

    # --- .doc printable-run fallback ---
    blob = (b"\x00\x01\x02" + "Cited 410 U.S. 113 today.".encode("cp1252")
            + b"\x00\x00\xff")
    runs = _printable_runs(blob)
    if "410 U.S. 113" not in runs:
        print("DOC fallback FAIL:", repr(runs)); failures += 1

    if failures:
        print(f"\n{failures} check(s) failed")
        sys.exit(1)
    print("OK: brief_reader self-test passed")
