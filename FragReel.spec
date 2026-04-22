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
    console=True,           # v0.1.x: console visível pra debug. Voltará a False em produção.
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
