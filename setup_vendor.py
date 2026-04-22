"""Vendor downloader — fetches HLAE + ffmpeg into client/vendor/.

These binaries (~200 MB combined) aren't committed to git because:
  - HLAE is GPL'd; redistributing in our repo would impose GPL on the
    surrounding Python code by linkage. Pulling at install/build time
    keeps users grabbing it from advancedfx.org directly.
  - ffmpeg static builds change frequently; pinning the URL beats baking
    the bytes into our git history.

Two callers:
  1. End-user `main.py` first run on Windows — checks vendor/ and only
     downloads if AfxHookSource2.dll is missing. Idempotent.
  2. CI build (release.yml) before PyInstaller — same check, downloads
     into the workspace so PyInstaller can bundle them via FragReel.spec.

Pinned versions are lock-stepped with what PC-Claude validated end-to-end
on 2026-04-22 (commit cf36542). Update only after re-testing the full
capture → ProRes → Remotion pipeline against the new build.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import os
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("fragreel.setup_vendor")


# ---------------------------------------------------------------------------
# Pinned downloads
# ---------------------------------------------------------------------------

# HLAE 2.189.9 — Source 2 (CS2) compatible build with x64/AfxHookSource2.dll.
# advancedfx.org publishes releases on GitHub; using the tag URL keeps us
# pinned to the exact build PC-Claude validated.
HLAE_VERSION = "2.189.9"
# Asset filename uses underscores (hlae_2_189_9.zip), tag uses dots (v2.189.9)
HLAE_URL = (
    "https://github.com/advancedfx/advancedfx/releases/download/"
    f"v{HLAE_VERSION}/hlae_{HLAE_VERSION.replace('.', '_')}.zip"
)
# SHA256 left empty until first run computes it; mismatch logs a warning
# but doesn't fatal — we trust GitHub's TLS for the download integrity.
HLAE_SHA256: str | None = None

# ffmpeg release-essentials from gyan.dev — static Windows build, ~96 MB
# extracted. Includes prores_ks encoder which we need for ProRes 4444.
FFMPEG_VERSION = "release-essentials"
FFMPEG_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
FFMPEG_SHA256: str | None = None


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VendorLayout:
    """Where vendored binaries land relative to the client root."""

    vendor_root: Path  # <client>/vendor

    @property
    def hlae_dir(self) -> Path:
        return self.vendor_root / "hlae"

    @property
    def hook_dll(self) -> Path:
        # x64/AfxHookSource2.dll is the Source 2 (CS2) hook. Root-level
        # AfxHookSource.dll is 32-bit CS:GO legacy and doesn't load CS2.
        return self.hlae_dir / "x64" / "AfxHookSource2.dll"

    @property
    def ffmpeg_exe(self) -> Path:
        return self.hlae_dir / "ffmpeg" / "bin" / "ffmpeg.exe"

    def is_complete(self) -> bool:
        return self.hook_dll.exists() and self.ffmpeg_exe.exists()


def default_layout() -> VendorLayout:
    """Default vendor location.

    - Frozen bundle (PyInstaller): vendor/ ships inside the .exe and is
      extracted to sys._MEIPASS at runtime — read-only. We use that.
    - Source checkout: <client>/vendor (writable; downloaded on first run).
    """
    if getattr(sys, "frozen", False):
        # _MEIPASS is the runtime extraction dir for one-file builds.
        meipass = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
        return VendorLayout(vendor_root=meipass / "vendor")
    return VendorLayout(vendor_root=Path(__file__).parent / "vendor")


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------


def _download(url: str, expected_sha256: str | None = None) -> bytes:
    """Stream a URL into memory, verify SHA256 if provided."""
    log.info("downloading %s", url)
    with urllib.request.urlopen(url, timeout=180) as resp:
        total = int(resp.headers.get("Content-Length", "0") or "0")
        chunks: list[bytes] = []
        read = 0
        while True:
            chunk = resp.read(1 << 20)  # 1 MiB
            if not chunk:
                break
            chunks.append(chunk)
            read += len(chunk)
            if total:
                pct = 100 * read / total
                log.info("  %6.1f%%  %d / %d bytes", pct, read, total)
        data = b"".join(chunks)

    if expected_sha256:
        digest = hashlib.sha256(data).hexdigest()
        if digest.lower() != expected_sha256.lower():
            raise RuntimeError(
                f"SHA256 mismatch for {url}: got {digest}, expected {expected_sha256}"
            )
        log.info("sha256 verified")
    else:
        log.info("sha256 not pinned (got %s)", hashlib.sha256(data).hexdigest())
    return data


def _extract_zip(data: bytes, dest_dir: Path, *, strip_top_level: bool = False) -> None:
    """Extract a zip into dest_dir. With `strip_top_level=True` (used for the
    ffmpeg release zip whose contents live under `ffmpeg-7.x-essentials/`),
    the single top folder is unwrapped so we get `bin/ffmpeg.exe` directly."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        members = zf.namelist()
        if not members:
            raise RuntimeError("zip is empty")
        prefix = ""
        if strip_top_level:
            top = members[0].split("/", 1)[0] + "/"
            if all(m.startswith(top) or m == top.rstrip("/") for m in members):
                prefix = top
        for m in members:
            if prefix and not m.startswith(prefix):
                continue
            rel = m[len(prefix):] if prefix else m
            if not rel:
                continue
            target = dest_dir / rel
            if m.endswith("/"):
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(m) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def install_hlae(layout: VendorLayout, *, force: bool = False) -> None:
    """Download HLAE and extract into <vendor>/hlae/. Skips if hook already
    present and `force=False`."""
    if not force and layout.hook_dll.exists():
        log.info("HLAE already present at %s — skipping", layout.hook_dll)
        return
    if layout.hlae_dir.exists() and force:
        log.info("removing existing %s for clean reinstall", layout.hlae_dir)
        shutil.rmtree(layout.hlae_dir, ignore_errors=True)
    data = _download(HLAE_URL, expected_sha256=HLAE_SHA256)
    _extract_zip(data, layout.hlae_dir)
    if not layout.hook_dll.exists():
        raise RuntimeError(
            f"HLAE extracted but {layout.hook_dll.name} not found at expected "
            f"path {layout.hook_dll}. Did the upstream layout change?"
        )
    log.info("HLAE installed at %s", layout.hlae_dir)


def install_ffmpeg(layout: VendorLayout, *, force: bool = False) -> None:
    """Download ffmpeg essentials build and extract into <vendor>/hlae/ffmpeg/."""
    if not force and layout.ffmpeg_exe.exists():
        log.info("ffmpeg already present at %s — skipping", layout.ffmpeg_exe)
        return
    ffmpeg_root = layout.hlae_dir / "ffmpeg"
    if ffmpeg_root.exists() and force:
        log.info("removing existing %s for clean reinstall", ffmpeg_root)
        shutil.rmtree(ffmpeg_root, ignore_errors=True)
    data = _download(FFMPEG_URL, expected_sha256=FFMPEG_SHA256)
    # gyan.dev zips wrap everything under `ffmpeg-<ver>-essentials_build/`,
    # so strip_top_level gives us bin/, doc/, etc directly under hlae/ffmpeg/.
    _extract_zip(data, ffmpeg_root, strip_top_level=True)
    if not layout.ffmpeg_exe.exists():
        raise RuntimeError(
            f"ffmpeg extracted but {layout.ffmpeg_exe.name} not found at "
            f"expected path {layout.ffmpeg_exe}"
        )
    log.info("ffmpeg installed at %s", layout.ffmpeg_exe)


def ensure_vendor(*, layout: VendorLayout | None = None, force: bool = False) -> VendorLayout:
    """Idempotent install of HLAE + ffmpeg into vendor/. Safe to call on
    every app start; returns the resolved layout."""
    layout = layout or default_layout()
    layout.vendor_root.mkdir(parents=True, exist_ok=True)
    if not force and layout.is_complete():
        log.info("vendor already complete at %s", layout.vendor_root)
        return layout
    install_hlae(layout, force=force)
    install_ffmpeg(layout, force=force)
    return layout


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Download HLAE + ffmpeg into vendor/")
    ap.add_argument(
        "--vendor-dir",
        type=Path,
        default=None,
        help="Root for vendored binaries (default: <client>/vendor)",
    )
    ap.add_argument("--force", action="store_true", help="Re-download even if present")
    ap.add_argument(
        "--check",
        action="store_true",
        help="Only check whether vendor/ is complete; exit 0 if yes, 1 if not",
    )
    args = ap.parse_args()

    layout = (
        VendorLayout(vendor_root=args.vendor_dir)
        if args.vendor_dir
        else default_layout()
    )

    if args.check:
        if layout.is_complete():
            log.info("vendor complete at %s", layout.vendor_root)
            return 0
        log.warning("vendor incomplete at %s", layout.vendor_root)
        return 1

    ensure_vendor(layout=layout, force=args.force)
    log.info("done. hook=%s ffmpeg=%s", layout.hook_dll, layout.ffmpeg_exe)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
