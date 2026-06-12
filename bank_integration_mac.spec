# -*- mode: python ; coding: utf-8 -*-

import os
import shutil
import stat
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules


block_cipher = None
project_root = Path.cwd()

a1 = Analysis(
    ["整合1.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

a2 = Analysis(
    ["整合2.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

a3 = Analysis(
    ["整合3.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

a4 = Analysis(
    ["整合4.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

a5 = Analysis(
    ["整合5.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

a6 = Analysis(
    ["整合4_bc.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd") + ["websocket"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

a7 = Analysis(
    ["整合4_epin.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd") + ["websocket"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

a8 = Analysis(
    ["整合6.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
    noarchive=False,
)

pyz1 = PYZ(a1.pure, a1.zipped_data, cipher=block_cipher)
pyz2 = PYZ(a2.pure, a2.zipped_data, cipher=block_cipher)
pyz3 = PYZ(a3.pure, a3.zipped_data, cipher=block_cipher)
pyz4 = PYZ(a4.pure, a4.zipped_data, cipher=block_cipher)
pyz5 = PYZ(a5.pure, a5.zipped_data, cipher=block_cipher)
pyz6 = PYZ(a6.pure, a6.zipped_data, cipher=block_cipher)
pyz7 = PYZ(a7.pure, a7.zipped_data, cipher=block_cipher)
pyz8 = PYZ(a8.pure, a8.zipped_data, cipher=block_cipher)

exe1 = EXE(
    pyz1,
    a1.scripts,
    [],
    exclude_binaries=True,
    name="domestic_bank_integration",
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

exe2 = EXE(
    pyz2,
    a2.scripts,
    [],
    exclude_binaries=True,
    name="overseas_bank_integration",
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

exe3 = EXE(
    pyz3,
    a3.scripts,
    [],
    exclude_binaries=True,
    name="order_payment_match",
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

exe4 = EXE(
    pyz4,
    a4.scripts,
    [],
    exclude_binaries=True,
    name="recharge_order_export",
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

exe5 = EXE(
    pyz5,
    a5.scripts,
    [],
    exclude_binaries=True,
    name="payout_order_reconcile",
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

exe6 = EXE(
    pyz6,
    a6.scripts,
    [],
    exclude_binaries=True,
    name="bc_order_export",
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

exe7 = EXE(
    pyz7,
    a7.scripts,
    [],
    exclude_binaries=True,
    name="epin_data_extract",
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

exe8 = EXE(
    pyz8,
    a8.scripts,
    [],
    exclude_binaries=True,
    name="collection_payout_reconcile",
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
    exe1,
    a1.binaries,
    a1.zipfiles,
    a1.datas,
    exe2,
    a2.binaries,
    a2.zipfiles,
    a2.datas,
    exe3,
    a3.binaries,
    a3.zipfiles,
    a3.datas,
    exe4,
    a4.binaries,
    a4.zipfiles,
    a4.datas,
    exe5,
    a5.binaries,
    a5.zipfiles,
    a5.datas,
    exe6,
    a6.binaries,
    a6.zipfiles,
    a6.datas,
    exe7,
    a7.binaries,
    a7.zipfiles,
    a7.datas,
    exe8,
    a8.binaries,
    a8.zipfiles,
    a8.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="bank-integration",
)

dist_dir = project_root / "dist" / "bank-integration"

template_src = project_root / "template"
template_dst = dist_dir / "template"
if template_src.exists():
    if template_dst.exists():
        shutil.rmtree(template_dst)
    shutil.copytree(template_src, template_dst)

data_dst = dist_dir / "data"
if data_dst.exists():
    shutil.rmtree(data_dst)
(data_dst / "input" / "1").mkdir(parents=True, exist_ok=True)
(data_dst / "input" / "2").mkdir(parents=True, exist_ok=True)
(data_dst / "input" / "3").mkdir(parents=True, exist_ok=True)
(data_dst / "input" / "5").mkdir(parents=True, exist_ok=True)
(data_dst / "input" / "6").mkdir(parents=True, exist_ok=True)
(data_dst / "input" / "raw").mkdir(parents=True, exist_ok=True)
(data_dst / "input" / "raw" / "5").mkdir(parents=True, exist_ok=True)
(data_dst / "output").mkdir(parents=True, exist_ok=True)
(data_dst / "output" / "4").mkdir(parents=True, exist_ok=True)
(data_dst / "output" / "4_bc").mkdir(parents=True, exist_ok=True)
(data_dst / "output" / "4_epin").mkdir(parents=True, exist_ok=True)
(data_dst / "browser_profile" / "4").mkdir(parents=True, exist_ok=True)

for filename in (
    "start_domestic.command",
    "start_overseas.command",
    "start_orders.command",
    "start_export.command",
    "start_payout.command",
    "start_bc.command",
    "start_epin.command",
    "start_dual_reconcile.command",
    "README.md",
):
    src = project_root / filename
    if src.exists():
        dst = dist_dir / filename
        shutil.copy2(src, dst)
        if filename.endswith((".sh", ".command")):
            dst.chmod(dst.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
