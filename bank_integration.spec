# -*- mode: python ; coding: utf-8 -*-

import shutil
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules


block_cipher = None
project_root = Path.cwd()

# 打包体积优化:排除未使用的重依赖与预防性排除未安装的库
# lxml(代号7 删除后无 import;openpyxl 惰性回退 stdlib)、pypdfium2(仅图像渲染,本项目只提取文本/表格)
# cryptography 不排除(pdfminer 顶层强制加载,import pdfplumber 即需要)
EXCLUDES = [
    "lxml",
    "pypdfium2", "pypdfium2_raw",
    "numpy.tests", "pandas.tests",
    "matplotlib", "scipy",
    "tkinter", "_tkinter",
    "PyQt5", "PyQt6", "PySide2", "PySide6",
    "IPython", "jupyter", "notebook", "pytest",
]

a1 = Analysis(
    ["整合1.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=EXCLUDES,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
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
    excludes=EXCLUDES,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
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
    excludes=EXCLUDES,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
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
    excludes=EXCLUDES,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

a6 = Analysis(
    ["整合6.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=collect_submodules("xlrd"),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=EXCLUDES,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz1 = PYZ(a1.pure, a1.zipped_data, cipher=block_cipher)
pyz2 = PYZ(a2.pure, a2.zipped_data, cipher=block_cipher)
pyz3 = PYZ(a3.pure, a3.zipped_data, cipher=block_cipher)
pyz5 = PYZ(a5.pure, a5.zipped_data, cipher=block_cipher)
pyz6 = PYZ(a6.pure, a6.zipped_data, cipher=block_cipher)

exe1 = EXE(
    pyz1,
    a1.scripts,
    [],
    exclude_binaries=True,
    name="国内银行整合",
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
    name="海外银行整合",
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
    name="游戏订单匹配",
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
    name="代付订单对账",
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
    name="代收代付对账",
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
    exe5,
    a5.binaries,
    a5.zipfiles,
    a5.datas,
    exe6,
    a6.binaries,
    a6.zipfiles,
    a6.datas,
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

# 平台外置配置（接新平台免重打包）：拷贝到 exe 旁，用户可编辑/仿写。
# 与 template/、data/ 同为松散文件，运行时靠 get_project_root() 读取，不进二进制。
platforms_src = project_root / "platforms"
platforms_dst = dist_dir / "platforms"
if platforms_src.exists():
    if platforms_dst.exists():
        shutil.rmtree(platforms_dst)
    shutil.copytree(platforms_src, platforms_dst)
else:
    (platforms_dst / "6").mkdir(parents=True, exist_ok=True)
    (platforms_dst / "plugins").mkdir(parents=True, exist_ok=True)

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

for filename in (
    "开始整合1.bat",
    "开始整合2.bat",
    "开始整合3.bat",
    "开始整合5.bat",
    "开始整合6.bat",
    "run_1.ps1",
    "run_2.ps1",
    "run_3.ps1",
    "run_5.ps1",
    "run_6.ps1",
    "使用说明.txt",
):
    src = project_root / filename
    if src.exists():
        shutil.copy2(src, dist_dir / filename)
