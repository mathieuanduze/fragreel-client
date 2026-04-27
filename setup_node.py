"""Node.js portable downloader — fetches Node 20 LTS Windows x64 zip
into `vendor/node/` for bundling in the FragReel.exe via PyInstaller.

Round 4c Fase 2 — escalabilidade pra user final. Antes da Fase 2, o
cliente dependia do user ter Node instalado pra `npx remotion render`.
Bundling Node portable elimina essa dependência: o `.exe` final inclui
runtime completo (~30 MB extra) e usa `_MEIPASS/node/npx.cmd` em vez
de `shutil.which("npx")`.

Two callers (mirror do setup_vendor.py):
  1. End-user `main.py` first run on Windows — checks `vendor/node/` e
     baixa só se ausente. Idempotente.
  2. CI build (release.yml) antes do PyInstaller — same check, baixa
     no workspace pra `FragReel.spec` bundlar via _bundle_tree.

Pinned: Node 20.20.2 LTS (latest Node 20 LTS at 2026-04-27). Mantém
em sync com o que o editor Remotion suporta (engines field do
package.json provavelmente exige >=18, 20 LTS é safe choice).
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import os
import shutil
import sys
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("fragreel.setup_node")


# ---------------------------------------------------------------------------
# Pinned download
# ---------------------------------------------------------------------------

# Node 20 LTS portable Windows x64. Latest 20.x at pin date 2026-04-27.
# Bumpar requer re-test render Remotion completo (compatibility com
# @remotion/renderer 4.0.180 + dependências).
NODE_VERSION = "20.20.2"
NODE_URL = (
    f"https://nodejs.org/dist/v{NODE_VERSION}/node-v{NODE_VERSION}-win-x64.zip"
)
# SHA256 left empty until first run computes it; mismatch logs warning
# but doesn't fatal — we trust nodejs.org TLS pra integrity.
NODE_SHA256: str | None = None


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeLayout:
    """Where Node portable lands relative to the client root."""

    vendor_root: Path  # <client>/vendor

    @property
    def node_dir(self) -> Path:
        return self.vendor_root / "node"

    @property
    def node_exe(self) -> Path:
        # Node Windows portable extrai pra `node-vX.Y.Z-win-x64/node.exe`.
        # Após unzip + flatten (move conteúdo do subdir pro vendor/node/),
        # node.exe vive direto em vendor/node/.
        return self.node_dir / "node.exe"

    @property
    def npx_cmd(self) -> Path:
        # npx.cmd shim — Windows usa .cmd shim wrappers ao invés de symlinks.
        return self.node_dir / "npx.cmd"

    def is_complete(self) -> bool:
        return self.node_exe.exists() and self.npx_cmd.exists()


def default_layout() -> NodeLayout:
    """Default Node location.

    - Frozen bundle: `_MEIPASS/vendor/node/` extraído pelo PyInstaller.
    - Source checkout: `<client>/vendor/node/` (writable).
    """
    if getattr(sys, "frozen", False):
        meipass = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
        return NodeLayout(vendor_root=meipass / "vendor")
    return NodeLayout(vendor_root=Path(__file__).parent / "vendor")


# ---------------------------------------------------------------------------
# Download helpers (mirror setup_vendor.py)
# ---------------------------------------------------------------------------


def _download(url: str, expected_sha256: str | None = None) -> bytes:
    """Stream URL → bytes, verify SHA256 se fornecido."""
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
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected_sha256:
            log.warning(
                "SHA256 mismatch for %s: expected %s got %s",
                url, expected_sha256, actual,
            )
        else:
            log.info("SHA256 verified")
    else:
        log.info("SHA256 (computed): %s", hashlib.sha256(data).hexdigest())

    return data


def _extract_node_zip(zip_bytes: bytes, target_dir: Path) -> None:
    """Extrai node-vX.Y.Z-win-x64.zip e flatten contents pra target_dir/.

    O zip do Node tem estrutura:
        node-v20.20.2-win-x64/
            node.exe
            npm
            npm.cmd
            npx
            npx.cmd
            node_modules/
            ...

    Queremos vendor/node/node.exe direto (sem subdir). Flatten remove
    o nível de versão pra path resolution simples no hlae_runner.
    """
    log.info("extracting Node zip → %s", target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Primeiro descobre o root subdir name (ex: "node-v20.20.2-win-x64/")
        root_prefix = ""
        for name in zf.namelist():
            if "/" in name:
                root_prefix = name.split("/", 1)[0] + "/"
                break

        if not root_prefix:
            raise RuntimeError("Node zip não tem subdir root esperado")

        # Extrai stripping o root prefix
        for member in zf.infolist():
            if member.is_dir():
                continue
            if not member.filename.startswith(root_prefix):
                # Edge case: arquivo solto no root do zip — copia as-is
                rel_path = member.filename
            else:
                rel_path = member.filename[len(root_prefix):]
            if not rel_path:
                continue
            dest = target_dir / rel_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)

    log.info("Node extracted: %d items", sum(1 for _ in target_dir.rglob("*")))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ensure_node(layout: NodeLayout | None = None, force: bool = False) -> NodeLayout:
    """Garante Node portable disponível em `layout.node_dir`.

    Idempotente — se `layout.is_complete()` já é True e `force=False`,
    retorna sem download.
    """
    if layout is None:
        layout = default_layout()

    if layout.is_complete() and not force:
        log.info("Node já presente em %s", layout.node_dir)
        return layout

    if force and layout.node_dir.exists():
        log.info("force=True — limpando %s", layout.node_dir)
        shutil.rmtree(layout.node_dir, ignore_errors=True)

    log.info("baixando Node v%s portable Windows x64", NODE_VERSION)
    zip_bytes = _download(NODE_URL, expected_sha256=NODE_SHA256)
    _extract_node_zip(zip_bytes, layout.node_dir)

    if not layout.is_complete():
        raise RuntimeError(
            f"Node extraction concluiu mas {layout.node_exe} ou {layout.npx_cmd} "
            "ausentes. Algo estranho com o zip."
        )

    log.info(
        "Node portable pronto em %s (node.exe + npx.cmd validados)",
        layout.node_dir,
    )
    return layout


def check_only(layout: NodeLayout | None = None) -> bool:
    """Verifica se Node tá completo. Retorna True/False sem download."""
    if layout is None:
        layout = default_layout()
    ok = layout.is_complete()
    log.info(
        "Node check: %s (node.exe=%s, npx.cmd=%s)",
        "OK" if ok else "MISSING",
        layout.node_exe.exists(),
        layout.npx_cmd.exists(),
    )
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Download Node 20 LTS portable Windows pra bundling"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Só verifica se Node tá completo, sem baixar (exit 0/1)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Força re-download mesmo se Node já presente",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.check:
        return 0 if check_only() else 1

    try:
        ensure_node(force=args.force)
        return 0
    except Exception as exc:
        log.error("setup_node failed: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(_main())
