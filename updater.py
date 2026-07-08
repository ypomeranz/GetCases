"""Self-update for GetCases.

Checks the ``main`` branch on GitHub and, on request, downloads the latest
files over the install and relaunches.  Design:

* **Version identity is a git commit SHA.**  The *installed* SHA is whatever the
  last update recorded (persisted by the caller in ``config.json``), falling
  back to ``git rev-parse HEAD`` for a fresh clone.  A tarball update does not
  move git HEAD, so once this updater has run the recorded SHA -- not git -- is
  authoritative; the caller must therefore prefer the recorded value.
* **Updating** downloads GitHub's ``main`` tarball, verifies it contains the
  expected files, then copies every file over the install directory.  Files
  that exist locally but not in the archive are left alone (we never delete),
  so a user's backup / index / config are safe.  ``data/opinions.jsonl`` is
  backed up by the caller beforehand and merged back after relaunch, so locally
  saved opinions are never lost.
* **No third-party dependencies** -- only ``urllib``/``tarfile`` -- so the
  updater works even in a bare install where ``requests`` etc. are absent.

The network/decision helpers are side-effect free and unit-tested in
``__main__``; the file-mutating helpers (:func:`download_and_stage`,
:func:`apply_over`, :func:`relaunch`) are exercised by the GUI.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from typing import Optional

REPO_SLUG = "ypomeranz/GetCases"
BRANCH = "main"
_API = "https://api.github.com"
_UA = "GetCases-updater"
_TIMEOUT = 60


def app_dir() -> Path:
    """The install directory (where the program's ``.py`` files live)."""
    return Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# GitHub queries
# ---------------------------------------------------------------------------

def _api_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={
        "User-Agent": _UA,
        "Accept": "application/vnd.github+json",
    })
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def git_head(directory: Optional[Path] = None) -> Optional[str]:
    """The current git commit SHA of *directory*, or ``None`` when git or a
    ``.git`` checkout is unavailable."""
    directory = directory or app_dir()
    if not (directory / ".git").exists():
        return None
    try:
        out = subprocess.run(
            ["git", "-C", str(directory), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def latest_commit() -> dict:
    """``{'sha', 'date', 'message'}`` for the tip of the main branch."""
    data = _api_get_json(f"{_API}/repos/{REPO_SLUG}/commits/{BRANCH}")
    commit = data.get("commit", {}) or {}
    message = (commit.get("message") or "").splitlines()
    return {
        "sha": data.get("sha") or "",
        "date": (commit.get("committer") or {}).get("date") or "",
        "message": message[0] if message else "",
    }


def compare(base_sha: str) -> Optional[dict]:
    """Compare *base_sha* to the main tip.  Returns ``{'status', 'ahead_by',
    'behind_by'}`` where *status* describes main relative to *base_sha*
    (``'ahead'`` means main has commits the base lacks), or ``None`` when the
    comparison can't be made (e.g. the base SHA is unknown to GitHub)."""
    if not base_sha:
        return None
    try:
        data = _api_get_json(
            f"{_API}/repos/{REPO_SLUG}/compare/{base_sha}...{BRANCH}")
        return {
            "status": data.get("status") or "",
            "ahead_by": int(data.get("ahead_by") or 0),
            "behind_by": int(data.get("behind_by") or 0),
        }
    except Exception:
        return None


def decide_update(local_sha: Optional[str], latest_sha: Optional[str],
                  cmp: Optional[dict]) -> dict:
    """Pure decision on whether an update should be offered.  Returns
    ``{'update': bool, 'reason': str}``."""
    if not latest_sha:
        return {"update": False, "reason": "Could not read the latest version."}
    if local_sha and local_sha == latest_sha:
        return {"update": False, "reason": "You're running the latest version."}
    if cmp is not None:
        if cmp["status"] in ("ahead", "diverged") and cmp["ahead_by"] > 0:
            n = cmp["ahead_by"]
            return {"update": True,
                    "reason": f"{n} new update{'s' if n != 1 else ''} available."}
        if cmp["status"] in ("identical", "behind"):
            return {"update": False, "reason": "You're running the latest version."}
    if not local_sha:
        return {"update": True,
                "reason": "A newer version may be available (current version "
                          "unknown)."}
    return {"update": True, "reason": "A newer version is available."}


def check(recorded_sha: Optional[str]) -> dict:
    """Full update check.  *recorded_sha* is the SHA the caller has persisted
    (``None`` if never updated through here).  Returns a dict for the UI:
    ``{'ok', 'update', 'local_sha', 'latest_sha', 'latest_date',
    'latest_message', 'reason', 'error'}``."""
    out = {"ok": False, "update": False, "local_sha": recorded_sha or git_head(),
           "latest_sha": None, "latest_date": "", "latest_message": "",
           "reason": "", "error": ""}
    try:
        latest = latest_commit()
    except Exception as exc:
        out["error"] = f"Could not reach GitHub: {exc}"
        out["reason"] = out["error"]
        return out
    out["ok"] = True
    out["latest_sha"] = latest["sha"]
    out["latest_date"] = latest["date"]
    out["latest_message"] = latest["message"]
    cmp = compare(out["local_sha"]) if out["local_sha"] else None
    decision = decide_update(out["local_sha"], latest["sha"], cmp)
    out["update"] = decision["update"]
    out["reason"] = decision["reason"]
    return out


# ---------------------------------------------------------------------------
# Download + apply
# ---------------------------------------------------------------------------

def _safe_extractall(tf: tarfile.TarFile, dest: Path) -> None:
    """Extract *tf* into *dest*, refusing any member that would escape it
    (guards against path-traversal in a malicious/corrupt archive)."""
    dest = dest.resolve()
    base = str(dest) + os.sep
    for member in tf.getmembers():
        target = (dest / member.name).resolve()
        if target != dest and not str(target).startswith(base):
            raise RuntimeError(f"unsafe path in archive: {member.name!r}")
    tf.extractall(dest)


def download_and_stage(dest_dir: Optional[Path] = None) -> Path:
    """Download the main-branch tarball and extract it.  Returns the extracted
    repo root (the folder containing ``courtlistener_gui.py``).  Raises on any
    network/extract failure or if the archive is missing expected files.  The
    caller owns *dest_dir* and should delete it when done."""
    dest_dir = dest_dir or Path(tempfile.mkdtemp(prefix="getcases_update_"))
    tar_path = dest_dir / "main.tar.gz"
    url = f"https://codeload.github.com/{REPO_SLUG}/tar.gz/refs/heads/{BRANCH}"
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp, \
            open(tar_path, "wb") as fh:
        shutil.copyfileobj(resp, fh)
    extract_dir = dest_dir / "extract"
    extract_dir.mkdir(exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tf:
        _safe_extractall(tf, extract_dir)
    # GitHub wraps the tree in a single "GetCases-<branch>/" directory.
    subdirs = [p for p in extract_dir.iterdir() if p.is_dir()]
    root = subdirs[0] if len(subdirs) == 1 else extract_dir
    if not (root / "courtlistener_gui.py").is_file():
        raise RuntimeError("downloaded archive is missing expected files")
    return root


def apply_over(src_root: Path, dst_dir: Path,
               skip_names: tuple[str, ...] = (".git",)) -> int:
    """Copy every file from *src_root* over *dst_dir*, overwriting in place.
    Directories/files whose path contains a name in *skip_names* are ignored.
    Files present locally but not in the archive are left untouched (we never
    delete).  Returns the number of files written."""
    written = 0
    skip = set(skip_names)
    for src in sorted(src_root.rglob("*")):
        rel = src.relative_to(src_root)
        if any(part in skip for part in rel.parts):
            continue
        dst = dst_dir / rel
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            written += 1
    return written


def relaunch() -> None:
    """Start a fresh instance with the same interpreter and arguments.  The
    caller should tear down its window immediately afterwards so only the new
    process remains."""
    subprocess.Popen([sys.executable] + sys.argv)


# ---------------------------------------------------------------------------
# Offline self-test:  python updater.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    failed = 0

    def check_eq(got, want, label):
        global failed
        ok = got == want
        failed += not ok
        print(("ok   " if ok else "FAIL ") + label
              + ("" if ok else f"  (got {got!r}, want {want!r})"))

    # decide_update ---------------------------------------------------------
    check_eq(decide_update("abc", "abc", None)["update"], False, "same sha")
    check_eq(decide_update("abc", "def",
                           {"status": "ahead", "ahead_by": 3, "behind_by": 0})["update"],
             True, "behind by 3")
    check_eq(decide_update("abc", "def",
                           {"status": "behind", "ahead_by": 0, "behind_by": 2})["update"],
             False, "local ahead (dev)")
    check_eq(decide_update("abc", "def",
                           {"status": "identical", "ahead_by": 0, "behind_by": 0})["update"],
             False, "identical trees")
    check_eq(decide_update("abc", "def",
                           {"status": "diverged", "ahead_by": 1, "behind_by": 1})["update"],
             True, "diverged")
    check_eq(decide_update("abc", "def", None)["update"], True,
             "differs, no compare")
    check_eq(decide_update(None, "def", None)["update"], True, "unknown local")
    check_eq(decide_update("abc", None, None)["update"], False, "no latest")

    # _safe_extractall rejects path traversal -------------------------------
    import io
    with tempfile.TemporaryDirectory() as d:
        dpath = Path(d)
        bad = dpath / "bad.tar.gz"
        with tarfile.open(bad, "w:gz") as tf:
            data = b"x"
            info = tarfile.TarInfo(name="../escape.txt")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        try:
            with tarfile.open(bad, "r:gz") as tf:
                _safe_extractall(tf, dpath / "out")
            check_eq(True, False, "traversal blocked")
        except RuntimeError:
            check_eq(True, True, "traversal blocked")

    # apply_over copies new + overwrites, never deletes ---------------------
    with tempfile.TemporaryDirectory() as d:
        dpath = Path(d)
        src = dpath / "src"
        dst = dpath / "dst"
        (src / "sub").mkdir(parents=True)
        (src / "courtlistener_gui.py").write_text("new")
        (src / "sub" / "a.py").write_text("A")
        (src / ".git").mkdir()
        (src / ".git" / "x").write_text("skip me")
        dst.mkdir()
        (dst / "courtlistener_gui.py").write_text("old")
        (dst / "keep.txt").write_text("local only")
        n = apply_over(src, dst)
        check_eq((dst / "courtlistener_gui.py").read_text(), "new", "overwrote")
        check_eq((dst / "sub" / "a.py").read_text(), "A", "added nested")
        check_eq((dst / "keep.txt").read_text(), "local only", "kept local file")
        check_eq((dst / ".git").exists(), False, "skipped .git")
        check_eq(n, 2, "wrote 2 files")

    raise SystemExit(1 if failed else 0)
