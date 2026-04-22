# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for FragReel Windows client
# Built automatically by GitHub Actions on every push to main.
# To build locally on Windows: pyinstaller FragReel.spec

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=[],
    datas=[],
    hiddenimports=[
        'plyer.platforms.win.notification',
        'pystray._win32',
        'PIL._tkinter_finder',
        'watchdog.observers.winapi',
        'flask',
        'flask_cors',
        'werkzeug.serving',
        'demoparser2',
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
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='FragReel',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
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
