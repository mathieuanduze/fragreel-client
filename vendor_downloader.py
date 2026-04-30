"""
vendor_downloader.py — Sprint J (29/04 morning).

Entry point único pra runtime first-run que baixa silenciosamente HLAE +
Node + ffmpeg + Editor pra `%APPDATA%/FragReel/` em vez de bundlar tudo
no `.exe` (que era v0.5.0 e antes — 362 MB monolítico).

Por que existe (Sprint J motivation):
  v0.5.0 bundle gerava 362 MB com PyInstaller ONEFILE. Heurística de
  AV (Kaspersky PDM:Trojan.Win32.Generic, Defender) flagava por:
    - Tamanho > 100MB packed
    - Self-extracting executable (PyInstaller pattern)
    - Process injection (HLAE inject DLL no CS2)
    - Spawns child processes (CS2, ffmpeg, npx, node)
    - Bundled Python interpreter
  Combo desses triggers fazia AV bloquear download (Mathieu's amigo
  parou nos 3 alertas, não conseguiu rodar).

  Solução Sprint J: thin client (~5-10 MB) baixa componentes individuais
  de fontes oficiais que AV já trust. AV vê "HLAE.exe oficial inject"
  em vez de "FragReel.exe desconhecido inject" → drop heurísticos.

Reusa scripts existentes (não re-implementa do zero):
  - setup_vendor.py (HLAE + ffmpeg) — já tem URLs+layout testados em CI
  - setup_node.py (Node 20 LTS) — idem
  - setup_editor_download.py (NEW Etapa J.2) — baixa editor de GitHub
    Release asset

Cache: %APPDATA%/FragReel/vendor/<component>/. Idempotente — skip se
exists. Re-download forçado via `force=True`.

Plan reference: [[Sprint J — Thin Client Implementation Plan]] em Obsidian.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger("fragreel.vendor_downloader")


# ── Storage paths ─────────────────────────────────────────────────────────────


def appdata_fragreel_dir() -> Path:
    """Pasta persistente FragReel. Não usa _MEIPASS — sobrevive entre runs
    do .exe ONEFILE (que extrai e deleta a cada run).

    Windows: %APPDATA%/FragReel/
    Outros (dev mode em macOS/Linux): ~/.fragreel/
    """
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home())) / "FragReel"
    else:
        base = Path.home() / ".fragreel"
    base.mkdir(parents=True, exist_ok=True)
    return base


def vendor_root() -> Path:
    """vendor/ subdir dentro do appdata FragReel.

    Estrutura final pós-Sprint J:
      %APPDATA%/FragReel/vendor/
      ├── hlae/
      │   ├── HLAE.exe
      │   ├── x64/AfxHookSource2.dll
      │   └── ffmpeg/bin/ffmpeg.exe
      └── node/
          ├── node.exe        ← flatten (setup_node.py faz flatten do subdir)
          └── npx.cmd
    """
    vendor = appdata_fragreel_dir() / "vendor"
    vendor.mkdir(parents=True, exist_ok=True)
    return vendor


# ── Path resolvers (replaced _MEIPASS lookups in hlae_runner.py + outros) ────


def hlae_dir() -> Path:
    """Path do HLAE extract root. Use `hook_dll_path()` pra .dll específica."""
    return vendor_root() / "hlae"


def hook_dll_path() -> Path:
    """AfxHookSource2.dll pra Source 2 (CS2) injection."""
    return hlae_dir() / "x64" / "AfxHookSource2.dll"


def node_dir() -> Path:
    """Path do Node extract root. node.exe vai estar direto aqui (flatten
    feito por setup_node.py)."""
    return vendor_root() / "node"


def npx_path() -> Path:
    """Path direto do npx.cmd usado por hlae_runner.py pra Remotion."""
    return node_dir() / "npx.cmd"


def node_exe_path() -> Path:
    """Path direto do node.exe."""
    return node_dir() / "node.exe"


def ffmpeg_path() -> Path:
    """Path direto do ffmpeg.exe (instalado dentro de hlae/ffmpeg/ pelo
    setup_vendor.py com strip_top_level)."""
    return hlae_dir() / "ffmpeg" / "bin" / "ffmpeg.exe"


def editor_dir() -> Path:
    """Path do editor Remotion (com node_modules). Sai de %APPDATA%/FragReel/
    (não vendor/), porque _resolve_editor_dir em local_api.py historicamente
    procura em `_MEIPASS/editor` (Round 4c Fase 1) — manter struct similar."""
    return appdata_fragreel_dir() / "editor"


# ── Public API: ensure_vendor_runtime ─────────────────────────────────────────


ProgressCallback = Callable[[str, int, int], None]
"""(component_name, bytes_downloaded, total_bytes) → None. Pra UI progress."""


def is_vendor_complete() -> bool:
    """Quick check sem download — todos os marker paths existem?

    Usado por main.py pra detectar first-run vs warm-run sem bloquear.
    """
    markers = [
        hook_dll_path(),     # HLAE
        ffmpeg_path(),       # ffmpeg
        npx_path(),          # Node 20
        editor_dir() / "package.json",  # Editor
        editor_dir() / "node_modules" / "@remotion" / "cli" / "dist" / "index.js",
        # Bug #13 sanity check — se faltar dist/ do remotion/cli, Remotion
        # cai no fallback ffmpeg concat (sem música/transitions/orientation).
        # Validamos explicitamente pra detectar bundle quebrado.
    ]
    for marker in markers:
        if not marker.exists():
            log.debug("is_vendor_complete: marker %s missing", marker)
            return False
    return True


def ensure_vendor_runtime(*, force: bool = False, on_progress: Optional[ProgressCallback] = None) -> bool:
    """Garante que TODOS os componentes vendor estão presentes em
    %APPDATA%/FragReel/.

    Roda no first-run do cliente (chamado por main.py antes de iniciar
    pipeline). Idempotente — skip downloads já completos.

    Args:
        force: se True, re-baixa todos componentes mesmo se presentes
        on_progress: callback (component, bytes_downloaded, total_bytes)
                     pra UI splash mostrar progress

    Returns:
        True se TUDO está OK ao final. False se algum download falhou.

    Raises:
        nada — failures viram log warnings + return False
    """
    log.info("Sprint J: ensure_vendor_runtime starting (force=%s)", force)
    log.info("Sprint J: appdata=%s vendor=%s", appdata_fragreel_dir(), vendor_root())

    if not force and is_vendor_complete():
        log.info("Sprint J: vendor já completo (warm run, skip downloads)")
        return True

    # Sprint J.5 (30/04): downloads paralelos via ThreadPoolExecutor.
    # Antes era serial — HLAE+ffmpeg → Node → Editor — total ~20min com
    # gyan.dev a 50KB/s. Agora 3 jobs em paralelo, limitado pela banda
    # total mas dropa de 20min → ~5-7min em internet decente. Combinado
    # com troca gyan.dev → BtbN GitHub CDN, esperado 2-3min.
    #
    # Threads são seguras aqui porque cada job escreve em path próprio:
    #   - HLAE + ffmpeg → vendor/hlae/
    #   - Node          → vendor/node/
    #   - Editor        → editor/
    # Sem race em sistema de arquivos. urllib é thread-safe pra reads.
    import concurrent.futures

    jobs: dict[str, callable] = {}

    # Job 1: HLAE + ffmpeg (esses 2 ainda em série dentro do mesmo job
    # porque usam a mesma layout/dir e setup_vendor.ensure_vendor já
    # encadeia ambos. Total ~150MB, o gargalo histórico do first-run).
    def job_hlae_ffmpeg() -> None:
        from setup_vendor import VendorLayout
        from setup_vendor import ensure_vendor as ensure_hlae_ffmpeg
        hlae_layout = VendorLayout(vendor_root=vendor_root())
        log.info("Sprint J: [parallel] HLAE + ffmpeg → %s", hlae_layout.vendor_root)
        ensure_hlae_ffmpeg(layout=hlae_layout, force=force)
        log.info("Sprint J: [parallel] HLAE + ffmpeg OK")
    jobs["hlae_ffmpeg"] = job_hlae_ffmpeg

    # Job 2: Node 20 LTS (~30MB).
    def job_node() -> None:
        from setup_node import NodeLayout, ensure_node
        node_layout = NodeLayout(vendor_root=vendor_root())
        log.info("Sprint J: [parallel] Node 20 → %s", node_layout.node_dir)
        ensure_node(layout=node_layout, force=force)
        log.info("Sprint J: [parallel] Node 20 OK")
    jobs["node"] = job_node

    # Job 3: Editor zip (~43MB). Returns False em HTTP errors,
    # propagamos via exception pra cair no except do executor.
    def job_editor() -> None:
        from setup_editor_download import ensure_editor
        log.info("Sprint J: [parallel] Editor → %s", editor_dir())
        ok = ensure_editor(target_dir=editor_dir(), force=force, on_progress=on_progress)
        if not ok:
            raise RuntimeError("ensure_editor retornou False (HTTP error?)")
        log.info("Sprint J: [parallel] Editor OK")
    jobs["editor"] = job_editor

    failures: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3, thread_name_prefix="vendor-dl") as ex:
        future_to_name = {ex.submit(fn): name for name, fn in jobs.items()}
        for future in concurrent.futures.as_completed(future_to_name):
            name = future_to_name[future]
            try:
                future.result()
            except Exception as e:
                log.error("Sprint J: [parallel] job %s FAILED: %s", name, e)
                failures.append(f"{name}: {e}")

    if failures:
        log.error("Sprint J: %d job(s) falharam: %s", len(failures), failures)
        return False

    # Sanity check final pós-downloads
    if not is_vendor_complete():
        log.error(
            "Sprint J: ensure_vendor_runtime completou mas is_vendor_complete=False. "
            "Algum marker path missing — verificar logs acima."
        )
        return False

    log.info("Sprint J: ✓ ensure_vendor_runtime completo, todos os componentes OK")
    return True


# ── CLI for testing ───────────────────────────────────────────────────────────


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sprint J: vendor downloader")
    parser.add_argument("--force", action="store_true", help="Re-baixa tudo")
    parser.add_argument("--check", action="store_true", help="Só verifica se completo")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.check:
        complete = is_vendor_complete()
        print(f"Vendor complete: {complete}")
        print(f"  appdata: {appdata_fragreel_dir()}")
        print(f"  vendor:  {vendor_root()}")
        print(f"  hook_dll: {hook_dll_path().exists()} ({hook_dll_path()})")
        print(f"  ffmpeg:   {ffmpeg_path().exists()} ({ffmpeg_path()})")
        print(f"  npx:      {npx_path().exists()} ({npx_path()})")
        print(f"  editor:   {(editor_dir() / 'package.json').exists()} ({editor_dir()})")
        print(f"  remotion/cli/dist: {(editor_dir() / 'node_modules' / '@remotion' / 'cli' / 'dist' / 'index.js').exists()}")
        sys.exit(0 if complete else 1)

    def progress_print(name: str, downloaded: int, total: int) -> None:
        pct = downloaded / total * 100 if total else 0
        print(f"\r{name}: {downloaded/1024/1024:.1f}/{total/1024/1024:.1f} MB ({pct:.0f}%)",
              end="", flush=True)
        if downloaded >= total:
            print()

    ok = ensure_vendor_runtime(force=args.force, on_progress=progress_print)
    sys.exit(0 if ok else 1)
