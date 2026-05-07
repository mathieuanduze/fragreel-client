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


# ── Filename normalization ────────────────────────────────────────────────────


# Prefixos comuns que CS2 panorama pode usar ANTES do weapon name canônico.
# Diferentes versões do CS2 variam — pra garantir que editor/weaponIcons.ts
# encontre o file independente da convenção, normalizamos TODOS pra
# `<weapon>.svg` (sem prefix). Se o file já está sem prefix, no-op.
_FILENAME_PREFIX_STRIP = (
    "weapon_",
    "loadout_",
    "inventory_",
    "item_",
)


def _normalize_icon_name(filename: str) -> str:
    """Normaliza filename de panorama icon → weapon name canônico.

    Examples:
      'weapon_ak47.svg'    → 'ak47.svg'
      'ak47.svg'           → 'ak47.svg'
      'loadout_awp.svg'    → 'awp.svg'
      'inventory_glock.svg' → 'glock.svg'

    Lowercase, strip prefixes. Se nenhum prefix bate, retorna lowercase.
    """
    name = filename.lower()
    for prefix in _FILENAME_PREFIX_STRIP:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


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

    # 06/05 — version marker pra invalidar cache quando lógica de extração
    # muda. v2 = normalized filenames (strip weapon_/loadout_/etc prefixes).
    # Users com cache v1 (sem normalização) precisam re-extrair pra editor
    # weaponIcons.ts encontrar.
    EXTRACTION_VERSION = "v2-normalized"
    version_marker = target_dir / ".fragreel-icons-version"

    cached_version = None
    if version_marker.exists():
        try:
            cached_version = version_marker.read_text(encoding="utf-8").strip()
        except Exception:
            cached_version = None

    # Idempotência: skip APENAS se cached_version bate + tem files
    if (not force
        and equipment_target.exists()
        and cached_version == EXTRACTION_VERSION):
        existing_svgs = list(equipment_target.glob("*.svg"))
        if len(existing_svgs) >= 10:
            log.info(
                "CS2 icons já presentes em %s (%d SVGs, version %s) — skip",
                target_dir, len(existing_svgs), cached_version,
            )
            return True

    if cached_version != EXTRACTION_VERSION and equipment_target.exists():
        log.info(
            "CS2 icons cache version mismatch (cached=%s, current=%s) — re-extract",
            cached_version, EXTRACTION_VERSION,
        )

    panorama_icons = _find_cs2_panorama_dir(cs2_install)
    if panorama_icons is None:
        return False

    target_dir.mkdir(parents=True, exist_ok=True)

    # ── Equipment (weapon icons) ─────────────────────────────────────────────
    src_equipment = panorama_icons / "equipment"
    if src_equipment.exists() and src_equipment.is_dir():
        equipment_target.mkdir(parents=True, exist_ok=True)
        copied = 0
        # 06/05 (Mathieu): normaliza filenames pra editor weaponIcons.ts
        # encontrar independente da convenção (weapon_ak47.svg vs ak47.svg).
        # Plus copia TAMBÉM com o nome original como backup — caso alguma
        # weapon precise do prefix exato (raro mas defensivo).
        for src_file in src_equipment.glob("*.svg"):
            normalized = _normalize_icon_name(src_file.name)
            dst_normalized = equipment_target / normalized
            dst_original = equipment_target / src_file.name.lower()
            for dst in {dst_normalized, dst_original}:
                try:
                    if dst.exists() and dst.stat().st_mtime >= src_file.stat().st_mtime:
                        continue
                    shutil.copy2(src_file, dst)
                    copied += 1
                except Exception as e:
                    log.warning("Falha ao copiar %s → %s: %s", src_file.name, dst.name, e)
        log.info("Equipment icons: %d files (originais + normalizados) copiados de %s",
                 copied, src_equipment)
        # 06/05 — verbose logging pra debug filename mismatch (Mathieu reportou
        # 'svg quebrados como se não existisse o arquivo'). Lista TODOS os
        # filenames pós-extração pra editor weaponIcons.ts mapping ser ajustado
        # se necessário.
        all_svgs = sorted([p.name for p in equipment_target.glob("*.svg")])
        log.info(
            "Equipment icons: %d filenames disponíveis. Primeiros 30: %s",
            len(all_svgs), all_svgs[:30],
        )
        if len(all_svgs) > 30:
            log.info("... e mais %d (full list em DEBUG): %s",
                     len(all_svgs) - 30, all_svgs[30:60])
    else:
        log.warning("equipment/ dir missing em %s", src_equipment)

    # ── Death notice (HS, wallbang, smoke, blind modifiers) ─────────────────
    src_death = panorama_icons / "death_notice"
    if src_death.exists() and src_death.is_dir():
        death_notice_target.mkdir(parents=True, exist_ok=True)
        copied = 0
        # Mesma normalização do equipment — strip prefixes pra garantir que
        # 'headshot.svg' funciona em qualquer versão do CS2 panorama.
        for src_file in src_death.glob("*.svg"):
            normalized = _normalize_icon_name(src_file.name)
            dst_normalized = death_notice_target / normalized
            dst_original = death_notice_target / src_file.name.lower()
            for dst in {dst_normalized, dst_original}:
                try:
                    if dst.exists() and dst.stat().st_mtime >= src_file.stat().st_mtime:
                        continue
                    shutil.copy2(src_file, dst)
                    copied += 1
                except Exception as e:
                    log.warning("Falha ao copiar %s → %s: %s", src_file.name, dst.name, e)
        log.info("Death notice icons: %d files copiados de %s", copied, src_death)
    else:
        log.info("death_notice/ dir missing em %s — modifiers serão fallback",
                 src_death)

    # Sanity check final
    final_count = len(list(equipment_target.glob("*.svg"))) if equipment_target.exists() else 0
    if final_count == 0:
        log.warning("Nenhum equipment icon copiado — editor caindo pra fallback")
        return False

    # Persiste version marker pra próximo run skip se já normalizado
    try:
        version_marker.write_text(EXTRACTION_VERSION, encoding="utf-8")
    except Exception as e:
        log.warning("Não conseguiu gravar version marker: %s", e)

    log.info(
        "CS2 icons extracted (%s): %d equipment + %d death_notice em %s",
        EXTRACTION_VERSION,
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
