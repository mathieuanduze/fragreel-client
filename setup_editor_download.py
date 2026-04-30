"""
setup_editor_download.py — Sprint J Etapa J.2 (29/04 morning).

Baixa vendor-editor.zip de GitHub Release asset do fragreel-client em vez
de copy from sibling repo (que era setup_editor.py — kept para CI usage).

Por que NOVO em vez de modificar setup_editor.py:
  - setup_editor.py é usado pelo CI workflow pra preparar o asset
    (`copy fragreel sibling → vendor/editor → zip → GitHub Release upload`)
  - setup_editor_download.py é usado em RUNTIME pelo client final pra
    baixar o asset preparado pelo CI
  - Separação evita confusão: cada script tem 1 responsabilidade

Source URL: github.com/mathieuanduze/fragreel-client/releases/download/<TAG>/vendor-editor.zip
Onde <TAG> é a versão da release (default v0.6.0+, pode ser overridden via env).

Bug #13 V2 sanity check (CRÍTICO):
  Pós-extract, valida @remotion/cli/dist/index.js existe. Se zip não
  preservou node_modules/*/dist/ (Bug #13 ignore-pattern recursive issue),
  Remotion render cai no fallback ffmpeg concat — produz vídeo widescreen
  sem música/transições/orientation. Round 4c teve 36 fases pra resolver
  isso, NÃO regredir.

Plan reference: [[Sprint J — Thin Client Implementation Plan]] em Obsidian.
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
import shutil
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger("fragreel.setup_editor_download")


# ── Pinned release ────────────────────────────────────────────────────────────


# Default version aponta pra release que CI publicou. Pode ser sobrescrito
# via env var FRAGREEL_EDITOR_VERSION pra dev/staging que apontam pra
# release diferente. CI build define isso via -X flag durante PyInstaller
# (Etapa J.4 do Sprint J plan).
DEFAULT_EDITOR_VERSION = os.environ.get("FRAGREEL_EDITOR_VERSION", "v0.6.0")


def _editor_zip_url(version: str) -> str:
    """URL do vendor-editor.zip asset numa release tag específica."""
    return (
        f"https://github.com/mathieuanduze/fragreel-client/releases/download/"
        f"{version}/vendor-editor.zip"
    )


# ── Public API ────────────────────────────────────────────────────────────────


ProgressCallback = Callable[[str, int, int], None]
"""(component_name, bytes_downloaded, total_bytes) → None."""


def ensure_editor(
    target_dir: Path,
    *,
    version: Optional[str] = None,
    force: bool = False,
    on_progress: Optional[ProgressCallback] = None,
) -> bool:
    """Baixa vendor-editor.zip + extrai pra target_dir.

    Sanity check Bug #13 V2: pós-extract, valida `@remotion/cli/dist/index.js`
    existe. Se faltar, raise — Remotion não vai funcionar.

    Args:
        target_dir: pasta destino. Tipicamente %APPDATA%/FragReel/editor/.
        version: tag da release (ex: "v0.6.0"). Default = DEFAULT_EDITOR_VERSION.
        force: re-baixa mesmo se exists.
        on_progress: callback (name, bytes, total) pra UI progress.

    Returns:
        True se sucesso, False se falhou (logs detalham).

    Raises:
        RuntimeError: pós-extract, marker @remotion/cli/dist faltando
                      (Bug #13 V2 risk — indica zip mal-formado).
    """
    version = version or DEFAULT_EDITOR_VERSION
    target_dir = Path(target_dir)
    package_json = target_dir / "package.json"
    cli_dist_marker = (
        target_dir / "node_modules" / "@remotion" / "cli" / "dist" / "index.js"
    )

    # Skip se completo + force=False
    if not force and package_json.exists() and cli_dist_marker.exists():
        log.info("Editor já presente em %s — skip download", target_dir)
        return True

    if force and target_dir.exists():
        log.info("force=True, removendo %s", target_dir)
        shutil.rmtree(target_dir, ignore_errors=True)

    target_dir.mkdir(parents=True, exist_ok=True)

    url = _editor_zip_url(version)
    log.info("Baixando editor de %s", url)

    try:
        zip_bytes = _download(url, on_progress=on_progress)
    except Exception as e:
        log.error("Editor download falhou: %s", e)
        return False

    log.info("Editor zip baixado: %.1f MB", len(zip_bytes) / 1024 / 1024)

    # Extract
    try:
        log.info("Extraindo editor zip pra %s", target_dir)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(target_dir)
    except Exception as e:
        log.error("Editor extract falhou: %s", e)
        return False

    # Bug #13 V2 sanity check — CRÍTICO
    if not package_json.exists():
        log.error("Editor extract: package.json missing em %s", package_json)
        raise RuntimeError(
            f"Editor zip mal-formado: package.json missing em {package_json}"
        )
    if not cli_dist_marker.exists():
        # Esse é o bug que aconteceu em v0.4.2 — node_modules/*/dist filtrado
        # pelo zip ignore patterns. Remotion vai cair em fallback ffmpeg concat.
        # NÃO continuar — é regressão crítica.
        log.error(
            "Bug #13 V2 risk: @remotion/cli/dist/index.js missing em %s. "
            "Editor zip está mal-formado — node_modules/*/dist provavelmente "
            "filtrado por ignore patterns. Remotion vai falhar.",
            cli_dist_marker,
        )
        raise RuntimeError(
            f"Bug #13 V2 risk: {cli_dist_marker} missing pós-extract. "
            f"Verificar workflow CI .github/workflows/release.yml — zip do editor "
            f"deve preservar node_modules/*/dist (NÃO usar ignore-recursive)."
        )

    log.info(
        "Editor instalado em %s (Bug #13 V2 sanity check ✓ — @remotion/cli/dist OK)",
        target_dir,
    )
    return True


# ── Internal helpers ──────────────────────────────────────────────────────────


def _download(
    url: str,
    *,
    on_progress: Optional[ProgressCallback] = None,
    timeout: int = 180,
) -> bytes:
    """Stream de URL pra memória com progress callback opcional.

    Editor zip é ~80 MB — cabe em RAM tranquilamente em PCs modernos.
    Pra Sprint J versão futura, considerar stream-to-disk se vendor-editor
    crescer > 200 MB.
    """
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "FragReel-Client/setup_editor_download"},
    )
    chunks: list[bytes] = []
    read = 0

    with urllib.request.urlopen(req, timeout=timeout) as resp:
        total = int(resp.headers.get("Content-Length", "0") or "0")
        chunk_size = 256 * 1024  # 256 KB

        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
            read += len(chunk)
            if on_progress and total > 0:
                on_progress("Editor Remotion", read, total)

    return b"".join(chunks)


# ── CLI for testing ───────────────────────────────────────────────────────────


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Sprint J Etapa J.2: editor downloader"
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=None,
        help="Onde extrair (default: %APPDATA%/FragReel/editor)",
    )
    parser.add_argument(
        "--version",
        default=None,
        help=f"Tag da release (default: {DEFAULT_EDITOR_VERSION})",
    )
    parser.add_argument("--force", action="store_true", help="Re-baixa mesmo se exists")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Determinar target dir
    if args.target_dir:
        target = args.target_dir
    else:
        # Reusa lógica de vendor_downloader pra consistência
        try:
            from vendor_downloader import editor_dir
            target = editor_dir()
        except ImportError:
            # Fallback: %APPDATA%/FragReel/editor
            if sys.platform == "win32":
                target = Path(os.environ.get("APPDATA", Path.home())) / "FragReel" / "editor"
            else:
                target = Path.home() / ".fragreel" / "editor"

    def progress_print(name: str, downloaded: int, total: int) -> None:
        pct = downloaded / total * 100 if total else 0
        print(
            f"\r{name}: {downloaded/1024/1024:.1f}/{total/1024/1024:.1f} MB ({pct:.0f}%)",
            end="", flush=True,
        )
        if downloaded >= total:
            print()

    try:
        ok = ensure_editor(
            target,
            version=args.version,
            force=args.force,
            on_progress=progress_print,
        )
        sys.exit(0 if ok else 1)
    except RuntimeError as e:
        log.error("Editor download FAILED com sanity check: %s", e)
        sys.exit(2)
