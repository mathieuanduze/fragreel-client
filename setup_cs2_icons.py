"""
setup_cs2_icons.py — Sprint Killfeed Icons (06/05).

Extrai ícones canônicos do CS2 panorama pro vendor folder do FragReel,
usados pelo editor Remotion pra renderizar killfeed idêntico ao CS2 vanilla.

Mathieu spec (06/05): "vamos fazer isso agora? minha questão é ele conversar
com o que o usuário já conhece do CS." → ícones canônicos > text-only weapon
names > pixel art próprio.

Decisão de licensing: extrair do install do user (vs bundle FragReel) porque
user é dono dos assets via ownership do CS2. Distribuição requer cuidado de
licensing — bundle direct do Valve content é gray area. Usar local install é
clean: redistributing to your own machine.

Source paths típicos no CS2 (varia por versão):
  Equipment (weapons):
    <CS2>/game/csgo/panorama/images/icons/equipment/ak47.svg
    <CS2>/game/csgo/panorama/images/icons/equipment/awp.svg
    ...
  Death notice modifiers (HS, wallbang, etc):
    <CS2>/game/csgo/panorama/images/icons/death_notice/headshot.svg
    <CS2>/game/csgo/panorama/images/icons/death_notice/penetrate.svg
    ...

Output:
  %APPDATA%/FragReel/cs2-icons/
    equipment/<weapon>.svg
    death_notice/<modifier>.svg

Idempotente: skip se files já presentes (mtime check).

Graceful degradation: se CS2 não encontrado OU panorama path missing, retorna
False sem raise. Editor cai pro fallback text-only weapon name (current
behavior).
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Optional

log = logging.getLogger("fragreel.setup_cs2_icons")


# ── CS2 install detection ─────────────────────────────────────────────────────


def _find_cs2_panorama_dir(cs2_install: Optional[Path] = None) -> Optional[Path]:
    """Localiza <CS2>/game/csgo/panorama/images/icons/ no install do user.

    Args:
        cs2_install: opcional, override do install path. Default = auto-detect
                     via steam_detect.find_cs2_install_dir() (idêntico ao
                     existing path resolver do hlae_runner).

    Returns:
        Path do diretório icons/ se encontrado, None caso contrário.
    """
    if cs2_install is None:
        try:
            from steam_detect import _cs2_roots
            roots = _cs2_roots()
            cs2_install = roots[0] if roots else None
        except Exception as e:
            log.warning("CS2 auto-detect falhou: %s", e)
            return None

    if cs2_install is None:
        log.info("CS2 install não encontrado — skip icons extraction")
        return None

    cs2_install = Path(cs2_install)
    panorama_icons = cs2_install / "game" / "csgo" / "panorama" / "images" / "icons"
    if not panorama_icons.exists():
        log.warning(
            "Panorama icons dir missing em %s — CS2 install pode estar "
            "incompleto ou versão diferente", panorama_icons,
        )
        return None

    log.info("CS2 panorama icons: %s", panorama_icons)
    return panorama_icons


# ── Public API ────────────────────────────────────────────────────────────────


def ensure_cs2_icons(
    target_dir: Path,
    *,
    cs2_install: Optional[Path] = None,
    force: bool = False,
) -> bool:
    """Copia equipment/ e death_notice/ icons do CS2 install pro target_dir.

    Idempotente: skip se target_dir já tem >=10 SVGs em equipment/ (assumindo
    que se tem icons, são todos que precisamos — CS2 não fragmenta downloads
    de assets). force=True força recopy.

    Args:
        target_dir: %APPDATA%/FragReel/cs2-icons/ tipicamente
        cs2_install: opcional override
        force: re-copy mesmo se target tem files

    Returns:
        True se sucesso (target populado), False caso contrário (graceful —
        editor cai pra fallback text-only).
    """
    target_dir = Path(target_dir)
    equipment_target = target_dir / "equipment"
    death_notice_target = target_dir / "death_notice"

    # Idempotência: se já tem files suficientes, skip
    if not force and equipment_target.exists():
        existing_svgs = list(equipment_target.glob("*.svg"))
        if len(existing_svgs) >= 10:
            log.info(
                "CS2 icons já presentes em %s (%d SVGs) — skip extraction",
                target_dir, len(existing_svgs),
            )
            return True

    panorama_icons = _find_cs2_panorama_dir(cs2_install)
    if panorama_icons is None:
        return False

    target_dir.mkdir(parents=True, exist_ok=True)

    # ── Equipment (weapon icons) ─────────────────────────────────────────────
    src_equipment = panorama_icons / "equipment"
    if src_equipment.exists() and src_equipment.is_dir():
        equipment_target.mkdir(parents=True, exist_ok=True)
        copied = 0
        for src_file in src_equipment.glob("*.svg"):
            dst = equipment_target / src_file.name
            try:
                # Skip se already match (mtime check pra não polluir)
                if dst.exists() and dst.stat().st_mtime >= src_file.stat().st_mtime:
                    continue
                shutil.copy2(src_file, dst)
                copied += 1
            except Exception as e:
                log.warning("Falha ao copiar %s: %s", src_file.name, e)
        log.info("Equipment icons: %d copiados de %s", copied, src_equipment)
    else:
        log.warning("equipment/ dir missing em %s", src_equipment)

    # ── Death notice (HS, wallbang, smoke, blind modifiers) ─────────────────
    src_death = panorama_icons / "death_notice"
    if src_death.exists() and src_death.is_dir():
        death_notice_target.mkdir(parents=True, exist_ok=True)
        copied = 0
        for src_file in src_death.glob("*.svg"):
            dst = death_notice_target / src_file.name
            try:
                if dst.exists() and dst.stat().st_mtime >= src_file.stat().st_mtime:
                    continue
                shutil.copy2(src_file, dst)
                copied += 1
            except Exception as e:
                log.warning("Falha ao copiar %s: %s", src_file.name, e)
        log.info("Death notice icons: %d copiados de %s", copied, src_death)
    else:
        log.info("death_notice/ dir missing em %s — modifiers serão fallback",
                 src_death)

    # Sanity check final
    final_count = len(list(equipment_target.glob("*.svg"))) if equipment_target.exists() else 0
    if final_count == 0:
        log.warning("Nenhum equipment icon copiado — editor caindo pra fallback")
        return False

    log.info(
        "CS2 icons extracted: %d equipment + %d death_notice em %s",
        final_count,
        len(list(death_notice_target.glob("*.svg"))) if death_notice_target.exists() else 0,
        target_dir,
    )
    return True


# ── CLI for testing ───────────────────────────────────────────────────────────


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Sprint Killfeed Icons: extract CS2 SVGs")
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=None,
        help="Onde copiar (default: %APPDATA%/FragReel/cs2-icons)",
    )
    parser.add_argument(
        "--cs2-install",
        type=Path,
        default=None,
        help="Path do CS2 install (default: auto-detect via steam_detect)",
    )
    parser.add_argument("--force", action="store_true", help="Re-copy mesmo se exists")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.target_dir:
        target = args.target_dir
    else:
        try:
            from vendor_downloader import cs2_icons_dir
            target = cs2_icons_dir()
        except ImportError:
            if sys.platform == "win32":
                import os
                target = Path(os.environ.get("APPDATA", Path.home())) / "FragReel" / "cs2-icons"
            else:
                target = Path.home() / ".fragreel" / "cs2-icons"

    ok = ensure_cs2_icons(target, cs2_install=args.cs2_install, force=args.force)
    sys.exit(0 if ok else 1)
