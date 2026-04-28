"""Editor (Remotion) bundler — copia o sibling repo `fragreel/editor/`
pra `vendor/editor/` pra bundling no FragReel.exe via PyInstaller.

Round 4c Fase 2 — escalabilidade pra user final. Antes da Fase 2,
o cliente dependia de sibling repo `<workspace>/fragreel/editor/`
(dev mode only). User final que baixar `.exe` não tem esse sibling.

Esse script copia o editor inteiro (incluindo `node_modules/`) pra
`vendor/editor/`, onde PyInstaller's `_bundle_tree` no FragReel.spec
pega + bundle no `_MEIPASS/vendor/editor/`. hlae_runner.py
`_resolve_editor_dir()` já tem case `_MEIPASS/editor` (Round 4c
Fase 1) — só precisa o conteúdo lá.

Two callers:
  1. CI build (release.yml): clone fragreel → npm ci editor → run
     this script pra copy → PyInstaller bundle.
  2. Local Windows dev: usa diretamente sibling repo (skip esse
     script, dev mode). Skip silently se sibling não tá presente.

Tamanho: editor + node_modules = ~200-300 MB. Necessário pro Remotion
funcionar standalone (Chromium headless via Puppeteer + dependências).
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("fragreel.setup_editor")


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EditorLayout:
    """Where editor (Remotion) lands relative to client root."""

    vendor_root: Path  # <client>/vendor

    @property
    def editor_dir(self) -> Path:
        return self.vendor_root / "editor"

    @property
    def package_json(self) -> Path:
        return self.editor_dir / "package.json"

    @property
    def remotion_config(self) -> Path:
        return self.editor_dir / "remotion.config.ts"

    @property
    def node_modules(self) -> Path:
        return self.editor_dir / "node_modules"

    def is_complete(self) -> bool:
        # node_modules é o sinal mais forte — package.json sozinho indica
        # repo cloned mas npm install ainda pendente.
        # Bug #13 (28/04): adicionado check de @remotion/cli/dist/ pra
        # detectar bundle mutilado (PyInstaller filtrou dist/ recursivo)
        # ANTES de tagar release — CI agora falha se faltar.
        remotion_cli_dist = self.node_modules / "@remotion" / "cli" / "dist"
        return (
            self.package_json.exists()
            and self.remotion_config.exists()
            and self.node_modules.is_dir()
            and remotion_cli_dist.is_dir()
        )

    def diagnose(self) -> dict[str, bool]:
        """Detailed check pra logging — qual peça especificamente falta."""
        return {
            "package_json": self.package_json.exists(),
            "remotion_config": self.remotion_config.exists(),
            "node_modules": self.node_modules.is_dir(),
            "remotion_cli_dist": (self.node_modules / "@remotion" / "cli" / "dist").is_dir(),
            "remotion_renderer_dist": (self.node_modules / "@remotion" / "renderer" / "dist").is_dir(),
            "remotion_bundler_dist": (self.node_modules / "@remotion" / "bundler" / "dist").is_dir(),
        }


def default_layout() -> EditorLayout:
    """Default editor location.

    - Frozen bundle: `_MEIPASS/vendor/editor/` extraído pelo PyInstaller.
      MAS frozen mode usa `_resolve_editor_dir()` em local_api.py que
      olha pra `_MEIPASS/editor` (sem `vendor/`). Pra consistency,
      esse script copia direto pra `<client>/editor/` no source mode
      e PyInstaller bundle como `editor/` (sem prefix vendor).
    - Source checkout: `<client>/vendor/editor/` (writable pra ci copy).
    """
    if getattr(sys, "frozen", False):
        meipass = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
        return EditorLayout(vendor_root=meipass / "vendor")
    return EditorLayout(vendor_root=Path(__file__).parent / "vendor")


# ---------------------------------------------------------------------------
# Source resolution (sibling repo)
# ---------------------------------------------------------------------------


def find_sibling_editor() -> Path | None:
    """Encontra editor source no sibling repo fragreel.

    Mesma lógica de `local_api._resolve_editor_dir` (sibling discovery).
    Returns None se não achar — caller decide (skip ou error).
    """
    client_parent = Path(__file__).parent.parent
    candidates = [
        client_parent / "fragreel" / "editor",
        client_parent / "main" / "editor",  # PC layout alt
        client_parent / "fragreel-server" / "editor",
        client_parent.parent / "fragreel" / "editor",
        Path(__file__).parent / "fragreel" / "editor",  # CI checkout side-by-side
    ]
    for c in candidates:
        if (c / "package.json").exists() and (c / "remotion.config.ts").exists():
            log.info("found editor source: %s", c)
            return c
    log.warning("no sibling editor found nas candidates: %s", [str(c) for c in candidates])
    return None


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------


def copy_editor_to_vendor(
    source_editor: Path,
    layout: EditorLayout | None = None,
    force: bool = False,
) -> EditorLayout:
    """Copia editor source → vendor location.

    Inclui node_modules — required pro Remotion funcionar standalone.
    Skip dirs que não precisam (.git, .next, dist, etc) pra reduzir
    bundle size.
    """
    if layout is None:
        layout = default_layout()

    if layout.is_complete() and not force:
        log.info("vendor editor já presente em %s — skip copy", layout.editor_dir)
        return layout

    if not source_editor.is_dir():
        raise RuntimeError(f"source editor não existe: {source_editor}")

    if (source_editor / "node_modules").is_dir() is False:
        raise RuntimeError(
            f"source editor missing node_modules — run `npm ci` em {source_editor} primeiro"
        )

    # Limpa target se exists e force
    if force and layout.editor_dir.exists():
        log.info("force=True — limpando %s", layout.editor_dir)
        shutil.rmtree(layout.editor_dir, ignore_errors=True)

    layout.editor_dir.parent.mkdir(parents=True, exist_ok=True)

    log.info("copying %s → %s (incluindo node_modules)", source_editor, layout.editor_dir)

    # Skip dirs que não precisam ir no bundle final.
    #
    # Bug #13 (28/04, descoberto em v0.4.2 PC test): dist/build/out são
    # build outputs do EDITOR (top-level), mas TAMBÉM são dirs essenciais
    # dentro de node_modules/<pkg>/dist/ (compiled JS de cada pacote).
    # SKIP_DIRS antigo {.git,.next,"dist","build",.vercel,.turbo,"out"}
    # mutilava node_modules/@remotion/cli/dist/ → Cannot find module './dist/index'
    # → fallback ffmpeg concat (sem música/orientation/transitions).
    #
    # Fix: separar em 2 sets — SKIP_ALWAYS (irrelevantes em qualquer
    # lugar) + SKIP_TOPLEVEL_ONLY (só pula no root do editor, NUNCA
    # dentro de node_modules onde dist/ é obrigatório).
    SKIP_ALWAYS = {".git", ".next", ".vercel", ".turbo"}
    SKIP_TOPLEVEL_ONLY = {"dist", "build", "out"}

    source_str = str(source_editor)

    def _ignore(path: str, names: list[str]) -> list[str]:
        # Path absoluto vs source root: se contém /node_modules/ em
        # qualquer ponto, é dependência transitiva — preserva tudo.
        rel_path = path[len(source_str):].replace("\\", "/").lstrip("/")
        in_node_modules = "node_modules/" in (rel_path + "/")

        skipped = []
        for n in names:
            if n in SKIP_ALWAYS:
                skipped.append(n)
            elif n in SKIP_TOPLEVEL_ONLY and not in_node_modules:
                # Top-level editor build outputs — safe to skip.
                # Dentro de node_modules/<pkg>/dist/ é COMPILED CODE
                # do pacote — NÃO pode skipar (Bug #13).
                skipped.append(n)
        return skipped

    shutil.copytree(
        source_editor,
        layout.editor_dir,
        ignore=_ignore,
        dirs_exist_ok=False,
    )

    if not layout.is_complete():
        raise RuntimeError(
            f"copy concluiu mas layout incompleto: package_json={layout.package_json.exists()}, "
            f"remotion_config={layout.remotion_config.exists()}, node_modules={layout.node_modules.is_dir()}"
        )

    log.info("editor bundled em %s", layout.editor_dir)
    return layout


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Copia editor Remotion (sibling repo) → vendor/editor pra bundling"
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=None,
        help="Path explícito pro editor source. Default: auto-discovery sibling repo.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Só verifica se vendor editor tá completo (exit 0/1)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Força re-copy mesmo se vendor já presente",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    layout = default_layout()

    if args.check:
        ok = layout.is_complete()
        diag = layout.diagnose()
        log.info("editor vendor check: %s", "OK" if ok else "MISSING")
        for piece, present in diag.items():
            log.info("  %s: %s", piece, "✓" if present else "✗ MISSING")
        if not ok:
            log.error(
                "Bug #13 sanity check: alguma peça crítica faltando. Ver diagnose acima. "
                "Se remotion_cli_dist=✗ → SKIP_DIRS está mutilando node_modules — revisar setup_editor.py copy_editor_to_vendor()."
            )
        return 0 if ok else 1

    source = args.source or find_sibling_editor()
    if source is None:
        log.error(
            "no editor source achado. Especifique --source PATH ou clone "
            "fragreel sibling repo. CI deve checkout antes desse script."
        )
        return 2

    try:
        copy_editor_to_vendor(source, layout, force=args.force)
        return 0
    except Exception as exc:
        log.error("setup_editor failed: %s", exc)
        return 3


if __name__ == "__main__":
    sys.exit(_main())
