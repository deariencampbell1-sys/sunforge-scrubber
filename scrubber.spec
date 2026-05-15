# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all, collect_data_files

pandas_datas, pandas_binaries, pandas_hiddenimports = collect_all('pandas')
webview_datas = collect_data_files('webview')

a = Analysis(
    ['launcher.py'],
    pathex=['.'],
    binaries=pandas_binaries,
    datas=[
        ('scrubber.html', '.'),
    ] + pandas_datas + webview_datas,
    hiddenimports=[
        'serve',
        'scrubber',
        'flask',
        'flask_cors',
        'webview',
    ] + pandas_hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='SunForgeScrubber',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='scrubber.ico',
)
