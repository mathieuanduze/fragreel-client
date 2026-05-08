# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for FragReel Windows client.
# Built automatically by GitHub Actions on every push to main.
# To build locally on Windows:
#   1) python setup_vendor.py     (downloads vendor/hlae + ffmpeg, ~200MB)
#   2) pyinstaller --noconfirm --clean FragReel.spec
#
# vendor/ holds HLAE x64/AfxHookSource2.dll + 60 deps + ffmpeg binary,
# and is sourced by setup_vendor.py before the build runs (CI does it
# in a previous step). It's NOT committed — see .gitignore.

import os
from pathlib import Path
from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

PROJECT_ROOT = Path(os.path.abspath(os.getcwd()))
VENDOR_DIR = PROJECT_ROOT / "vendor"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SPLASH_PATH = PROJECT_ROOT / "splash.png"


def _bundle_tree(source: Path, dest_in_bundle: str) -> list[tuple[str, str]]:
    """Walk `source` and emit ('absolute/file', 'rel/in/bundle') tuples.

    PyInstaller's `datas` parameter copies these verbatim at build time.
    Returns [] silently if source doesn't exist (so a CI job that hasn't
    run setup_vendor.py yet still builds the .exe — just without HLAE).
    """
    if not source.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for root, _dirs, files in os.walk(source):
        rel_root = os.path.relpath(root, source)
        for f in files:
            abs_path = os.path.join(root, f)
            target_dir = (
                dest_in_bundle
                if rel_root == "."
                else f"{dest_in_bundle}/{rel_root}".replace(os.sep, "/")
            )
            out.append((abs_path, target_dir))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Sprint J (29/04) — Thin client refactor:
# vendor_datas + node_datas + editor_datas REMOVIDOS do bundle.
# .exe vai de ~362MB (v0.5.0) → ~5-15MB (v0.6.0+).
#
# Em runtime first-run, vendor_downloader.py baixa esses componentes
# pra %APPDATA%/FragReel/vendor/ + %APPDATA%/FragReel/editor/ via:
#   - setup_vendor.py (HLAE + ffmpeg)
#   - setup_node.py (Node 20 LTS)
#   - setup_editor_download.py (Editor de GitHub Release asset)
#
# Por que: AV heurística (Kaspersky PDM:Trojan, Defender) flagava o
# binário monolítico pelo combo "tamanho > 100MB + self-extracting +
# process injection + bundled Python interpreter". Thin client + downloads
# de fontes oficiais (advancedfx, nodejs.org, gyan.dev, GitHub Release)
# elimina maioria dos triggers heurísticos.
#
# Plan completo: [[Sprint J — Thin Client Implementation Plan]] em Obsidian.
# ─────────────────────────────────────────────────────────────────────────────

# scripts/ holds the .cfg generator imported by hlae_runner.py — needs to
# travel with the .exe so the bundled Python interpreter can import it.
scripts_datas = _bundle_tree(SCRIPTS_DIR, "scripts")

# Sprint DEMO-3 Sprint 3 (08/05/2026) — bundle steam_gc_sidecar/.
#
# Inclui:
#   steam_gc_sidecar/steam_gc.js (entry node)
#   steam_gc_sidecar/package.json (metadata)
#   steam_gc_sidecar/node_modules/ (deps node-steam-user, node-globaloffensive)
#   steam_gc_sidecar/node.exe (Windows binary, baixado pelo CI release.yml)
#
# CI workflow (.github/workflows/release.yml) faz `npm install` em
# steam_gc_sidecar/ + baixa node.exe Windows binary BEFORE PyInstaller run.
# Em dev (Mac), node global é usado via _resolve_node_executable().
SIDECAR_DIR = Path("steam_gc_sidecar").resolve()
sidecar_datas = []
if SIDECAR_DIR.exists():
    # Bundle todos files (incluindo node_modules + node.exe se present)
    for item in SIDECAR_DIR.rglob("*"):
        if item.is_file():
            # Skip dev artifacts
            if any(p in item.parts for p in (".git", "__pycache__", ".cache")):
                continue
            rel = item.relative_to(SIDECAR_DIR)
            target_dir = (Path("steam_gc_sidecar") / rel.parent).as_posix()
            sidecar_datas.append((str(item), target_dir))
    print(f"[FragReel.spec] steam_gc_sidecar: {len(sidecar_datas)} files bundled")
else:
    print("[FragReel.spec] steam_gc_sidecar/ não encontrado — DEMO-3 disabled neste build")


# Bug #18 (28/04, descoberto em v0.4.3 PC test): PyInstaller Splash() exige
# Tcl/Tk DLLs (tcl86t.dll, tk86t.dll) bundled, mas eles NÃO são auto-coletados
# unless tkinter está em hiddenimports + binaries explicitly.
# Sintoma v0.4.3: "failed to load tcl DLL - tcl86t.dll não foi possível
# encontrar o módulo especificado" + "SPLASH:Failed to load Tcl/Tk shared
# libraries" → app não inicia.
# Fix: collect_dynamic_libs('tcl') + collect_dynamic_libs('tk') puxa os
# .dll nativos. Plus tkinter + _tkinter em hiddenimports pra garantir
# Python wrapper layer também.
tcl_tk_binaries = collect_dynamic_libs('tcl') + collect_dynamic_libs('tk')

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=tcl_tk_binaries,
    # Sprint J: SÓ scripts_datas. vendor_datas/node_datas/editor_datas
    # removidos — vendor_downloader baixa em runtime first-run.
    # Sprint DEMO-3 Sprint 3: + sidecar_datas (steam_gc_sidecar bundle).
    datas=scripts_datas + sidecar_datas,
    hiddenimports=[
        'plyer.platforms.win.notification',
        'pystray._win32',
        'PIL._tkinter_finder',
        'watchdog.observers.winapi',
        'flask',
        'flask_cors',
        'werkzeug.serving',
        'demoparser2',
        # Bug #18 (28/04): tkinter + _tkinter explicitly pra Splash()
        # achar Tcl/Tk DLLs. PIL._tkinter_finder acima ajuda Pillow
        # mas NÃO é suficiente pro splash bootloader.
        'tkinter',
        '_tkinter',
        'tkinter.ttk',
        # polars + pyarrow são deps transitivas do demoparser2. parse_event()
        # tenta polars primeiro, pyarrow como fallback — sem ambos o Rust
        # faz .unwrap() num Err e estoura PanicException. Listamos as .lib
        # nativas explicitamente porque PyInstaller frequentemente falha em
        # descobrir os .pyd só pelo nome do pacote.
        'polars',
        'polars.polars',
        'pyarrow',
        'pyarrow.lib',
        'pyarrow.compute',
        # HLAE pipeline modules. PyInstaller usually picks these up
        # automatically because main.py → local_api.py imports them
        # transitively, but listing them defends against import order changes.
        'cs2_launcher',
        'hlae_runner',
        'render_coordinator',
        # Sprint J first-run downloaders. Reusam setup_vendor + setup_node
        # existentes (modo runtime) + adicionam setup_editor_download (NEW).
        'vendor_downloader',
        'setup_vendor',
        'setup_node',
        'setup_editor',           # mantido: usado pelo CI build
        'setup_editor_download',  # NEW Sprint J runtime
        # Sprint I.5 modules — full migration cliente parseia local + Vercel.
        'api_client',
        'local_matches_store',
        'local_parser',
        'local_parser.demo_parser',
        'parser',
        'parser.scorer',
        'parser.demo_parser',
        'scripts.capture_script',
        # v0.2.7: client_config holds output_dir persistence.
        'client_config',
        # Sprint DEMO-3 Sprint 3 (08/05/2026) — Steam GC client-side bot.
        'steam_gc',
        'steam_token_store',
        # Token store deps:
        'cryptography',
        'cryptography.fernet',
        # Windows-only: pywin32 pra DPAPI. Em CI Windows-built, esse
        # import succeeds. Em dev Mac/Linux, falha graciosamente
        # (steam_token_store usa Fernet fallback).
        'win32crypt',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

# Bug #18 (28/04): SPLASH DESABILITADO TEMPORARIAMENTE em v0.4.4.
# v0.4.3 introduziu Splash() mas PyInstaller bootloader falhou em loadar
# tcl/tk DLLs ("failed to load tcl DLL - tcl86t.dll não foi possível
# encontrar"), travando o app antes do Python iniciar. Erro é do bootloader
# nativo, não do Python — try/except em main.py NÃO captura.
#
# Fix preparado mas NÃO ativado nesta release:
#   - collect_dynamic_libs('tcl')+('tk') adicionado em binaries (acima)
#   - tkinter + _tkinter + tkinter.ttk em hiddenimports
# Antes de re-ativar Splash(), preciso validar em ambiente PyInstaller
# real (Windows VM ou GH Actions matrix com smoke test do .exe extraído).
#
# splash.png + generate_splash.py + main.py pyi_splash.close() ficam no
# repo aguardando próxima rodada com smoke test dedicado.
splash = None
splash_binaries = []
# DESABILITADO até validar tcl/tk bundle:
# if SPLASH_PATH.is_file():
#     splash = Splash(
#         str(SPLASH_PATH),
#         binaries=a.binaries,
#         datas=a.datas,
#         text_pos=None,
#         text_size=12,
#         minify_script=True,
#         always_on_top=True,
#     )
#     splash_binaries = splash.binaries

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    *([splash, splash_binaries] if splash else []),
    [],
    name='FragReel',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # UPX disabled: it triples startup time AND some AV engines flag
    # UPX-packed PE files as suspicious. ~200MB vendor/ dwarfs any size
    # win we'd get on the python runtime anyway.
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,          # sem janela preta — UX user-friendly. Logs vão pra %APPDATA%/FragReel/fragreel.log e tray icon confirma "rodando".
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
