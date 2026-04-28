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


# Vendor (HLAE + ffmpeg + Node + editor) — ~450MB total. Skipped silently
# se setup scripts ainda não rodaram, dev pode build local sem render.
vendor_datas = _bundle_tree(VENDOR_DIR, "vendor")

# Round 4c Fase 2 — Node + editor bundle pra escalabilidade user final.
# vendor/node/ vem de setup_node.py (~30MB Node 20 LTS portable Win x64).
# vendor/editor/ vem de setup_editor.py (clone fragreel sibling + npm ci
# editor + copy ~200-300MB com node_modules).
# hlae_runner._resolve_editor_dir() já tem case _MEIPASS/editor — pra
# aproveitar isso, copiamos editor no `editor/` direto (sem prefix vendor),
# mantendo Node em `vendor/node/`.
NODE_DIR = VENDOR_DIR / "node"
EDITOR_DIR_VENDOR = VENDOR_DIR / "editor"
node_datas = _bundle_tree(NODE_DIR, "vendor/node") if NODE_DIR.is_dir() else []
# Editor vai pra `editor/` (sem prefix vendor/) pra match _resolve_editor_dir.
editor_datas = _bundle_tree(EDITOR_DIR_VENDOR, "editor") if EDITOR_DIR_VENDOR.is_dir() else []

# scripts/ holds the .cfg generator imported by hlae_runner.py — needs to
# travel with the .exe so the bundled Python interpreter can import it.
scripts_datas = _bundle_tree(SCRIPTS_DIR, "scripts")


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
    datas=vendor_datas + node_datas + editor_datas + scripts_datas,
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
        # New in Round 4c: HLAE pipeline modules. PyInstaller usually picks
        # these up automatically because main.py → local_api.py imports them
        # transitively, but listing them defends against import order changes.
        'cs2_launcher',
        'hlae_runner',
        'render_coordinator',
        'setup_vendor',
        'setup_node',
        'setup_editor',
        'scripts.capture_script',
        # v0.2.7: client_config holds output_dir persistence. Imported by
        # local_api at top level, so a missing bundle would crash the entire
        # API thread on startup (not just /config endpoints).
        'client_config',
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
