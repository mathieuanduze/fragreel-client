"""
zip_vendor_editor.py — Sprint J Etapa J.3 helper.

Cria vendor-editor.zip a partir de vendor/editor/ pra upload como GitHub
Release asset. Cliente final baixa em runtime via setup_editor_download.py.

Lógica path-aware (Bug #13 V2 fix lição):
  - Top-level skip: .git, .next, .vercel, .turbo, dist, build, out
    (esses são build outputs do editor — NÃO incluir)
  - Subdirs (incluindo node_modules/*/dist): preservar TUDO exceto .git
    (CRÍTICO Bug #13 V2 — Remotion precisa de node_modules/*/dist em runtime)

Sanity check: confirma node_modules/@remotion/cli/dist/index.js dentro do
zip. Se não, sys.exit(1) — cancelar release porque Remotion vai cair em
fallback ffmpeg concat (sem música/transitions/orientation).

Uso (CI):
  python scripts/zip_vendor_editor.py vendor/editor vendor-editor.zip

Por que script dedicado em vez de inline `python -c`:
  - Inline em PowerShell tem escape hell com aspas + backslashes
  - Script dedicado é debugável (rodar local) + testável
  - Logging detalhado mais fácil
"""
from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path


# Top-level skip (build outputs do editor):
# .git/.next/.vercel/.turbo são VCS/build caches sempre irrelevantes
# dist/build/out são output do editor build (não run-time deps)
TOP_LEVEL_SKIP = {".git", ".next", ".vercel", ".turbo", "dist", "build", "out"}

# Sempre-skip recursive (apenas .git pra evitar submodule .git nested)
ALWAYS_SKIP_RECURSIVE = {".git"}

# Marker pra Bug #13 V2 sanity check
REMOTION_CLI_DIST_MARKER = "node_modules/@remotion/cli/dist/index.js"


def main() -> int:
    if len(sys.argv) != 3:
        print(f"Uso: {sys.argv[0]} <source_dir> <output_zip>", file=sys.stderr)
        return 2

    source = Path(sys.argv[1])
    target = Path(sys.argv[2])

    if not source.is_dir():
        print(f"ERROR: source dir não existe: {source}", file=sys.stderr)
        return 2

    print(f"Zipping {source} → {target}")
    print(f"  TOP_LEVEL_SKIP: {TOP_LEVEL_SKIP}")
    print(f"  ALWAYS_SKIP_RECURSIVE: {ALWAYS_SKIP_RECURSIVE}")

    n_files = 0
    n_dirs_walked = 0
    skipped_at_top_level = []
    sample_remotion_paths = []
    remotion_cli_dist_found = False

    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED, compresslevel=5) as zf:
        for root, dirs, files in os.walk(source, topdown=True):
            n_dirs_walked += 1
            # Normalize rel path (Windows backslash → forward slash)
            rel = os.path.relpath(root, source).replace("\\", "/")
            is_top_level = rel == "."

            # In-place filter dirs pra que os.walk NÃO entre neles
            if is_top_level:
                # Top-level: aplica TOP_LEVEL_SKIP
                original_dirs = list(dirs)
                dirs[:] = [d for d in dirs if d not in TOP_LEVEL_SKIP]
                skipped_at_top_level = [d for d in original_dirs if d in TOP_LEVEL_SKIP]
            else:
                # Subdir: SÓ ALWAYS_SKIP_RECURSIVE (preserva node_modules/*/dist)
                dirs[:] = [d for d in dirs if d not in ALWAYS_SKIP_RECURSIVE]

            for f in files:
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, source).replace("\\", "/")

                try:
                    zf.write(abs_path, rel_path)
                except (OSError, ValueError) as e:
                    print(f"  WARN: skip {rel_path} ({e})", file=sys.stderr)
                    continue

                n_files += 1

                # Track remotion paths pra debug
                if "@remotion" in rel_path and len(sample_remotion_paths) < 10:
                    sample_remotion_paths.append(rel_path)
                if rel_path == REMOTION_CLI_DIST_MARKER:
                    remotion_cli_dist_found = True

    size_mb = target.stat().st_size / 1024 / 1024
    print(f"\n=== Stats ===")
    print(f"  Dirs walked: {n_dirs_walked}")
    print(f"  Files written: {n_files}")
    print(f"  Output size: {size_mb:.1f} MB")
    print(f"  Skipped top-level: {skipped_at_top_level}")
    print(f"\n=== Sample @remotion paths (first 10) ===")
    for p in sample_remotion_paths:
        print(f"  {p}")

    print(f"\n=== Bug #13 V2 sanity check ===")
    print(f"  Marker: {REMOTION_CLI_DIST_MARKER}")
    print(f"  Found:  {remotion_cli_dist_found}")

    if not remotion_cli_dist_found:
        # Investigation: listar TUDO em node_modules/@remotion/cli/ pra entender
        cli_root = source / "node_modules" / "@remotion" / "cli"
        print(f"\n=== DEBUG: source {cli_root} contents ===")
        if cli_root.is_dir():
            for p in sorted(cli_root.rglob("*"))[:30]:
                rel = p.relative_to(source)
                print(f"  {'D' if p.is_dir() else 'F'} {rel}")
        else:
            print(f"  {cli_root} NÃO EXISTE em source — node_modules incompleto antes do zip!")

        print(f"\nERROR: {REMOTION_CLI_DIST_MARKER} NOT in zip — Remotion vai falhar em runtime", file=sys.stderr)
        return 1

    print(f"\n✓ Bug #13 V2 sanity check OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
