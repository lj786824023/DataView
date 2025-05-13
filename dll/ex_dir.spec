# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['App.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    exclude_binaries=True,
    name='App',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries+[('oraociei11.dll','C:\\Users\\lojn\\AppData\\Local\\Programs\\Python\\Python38\\Lib\\site-packages\\oraociei11.dll','BINARY'),('oci.dll','C:\\Users\\lojn\\AppData\\Local\\Programs\\Python\\Python38\\Lib\\site-packages\\oci.dll','BINARY')],
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='App',
)
