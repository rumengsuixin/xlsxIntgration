# -*- mode: python ; coding: utf-8 -*-

import shutil
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules


block_cipher = None
project_root = Path.cwd()


a = Analysis(
    ["整合.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="银行流水整合",
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
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="银行流水整合",
)

dist_dir = project_root / "dist" / "银行流水整合"

template_src = project_root / "template"
template_dst = dist_dir / "template"
if template_src.exists():
    if template_dst.exists():
        shutil.rmtree(template_dst)
    shutil.copytree(template_src, template_dst)

data_dst = dist_dir / "data"
if data_dst.exists():
    shutil.rmtree(data_dst)
(data_dst / "input" / "raw").mkdir(parents=True, exist_ok=True)
(data_dst / "output").mkdir(parents=True, exist_ok=True)

for filename in ("开始整合.bat", "run_user.ps1", "使用说明.txt"):
    src = project_root / filename
    if src.exists():
        shutil.copy2(src, dist_dir / filename)
