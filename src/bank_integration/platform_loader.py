"""平台注册表加载器：内置默认 → 外部 JSON → 外部 .py 插件，合并成有序 spec 列表。

外置化的入口。程序运行时从 exe/仓库根旁的 platforms/ 目录读取：
    platforms/<code>/*.json   声明式平台（结构类似的平台，改配置即接入，免打包）
    platforms/plugins/*.py    疑难平台插件（自定义 handler，放文件即生效，免打包）
    platforms/vendor/         可选，纯 Python 第三方依赖（自动 prepend sys.path）

合并优先级：插件 > JSON > 内置；同 key 深合并（JSON 只需写增量）；
enabled=false 停用。任一 JSON/插件出错只打中文 warning 并跳过，不影响其它平台。
外置目录不存在 / 为空时，注册表 = 内置默认，行为与外置化之前逐字节一致。
"""

import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

from .config import get_project_root
from .platform_spec import PlatformSpec, register_handler

logger = logging.getLogger(__name__)


def get_platforms_dir() -> Path:
    """定位 platforms/ 目录：环境变量 BANK_PLATFORMS_DIR 优先，否则项目根/platforms。

    项目根由 config.get_project_root() 决定：打包后=exe 所在目录，源码运行=仓库根，
    与 template/、data/ 完全同构，不引入 sys._MEIPASS。
    """
    override = os.environ.get("BANK_PLATFORMS_DIR")
    if override:
        return Path(override)
    return get_project_root() / "platforms"


def _merge_dict(base: dict, key: str, value: dict) -> dict:
    """浅合并 base[key] 与 value（value 覆盖同名键）。"""
    merged = dict(base.get(key) or {})
    merged.update(value)
    return merged


def _deep_merge(base: dict, ext: dict) -> dict:
    """把外部声明 ext 深合并到 base：columns/status_map 等逐键合并，directions 逐方向合并。"""
    out = dict(base)
    for k, v in ext.items():
        if k in ("columns", "status_map", "status_prefix_map", "handler_params") \
                and isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge_dict(out, k, v)
        elif k == "directions" and isinstance(v, dict):
            dirs = {name: dict(d) for name, d in (out.get("directions") or {}).items()}
            for name, d in v.items():
                if name in dirs and isinstance(d, dict):
                    nd = dict(dirs[name])
                    for dk, dv in d.items():
                        if dk == "columns" and isinstance(dv, dict) and isinstance(nd.get("columns"), dict):
                            nd["columns"] = _merge_dict(nd, "columns", dv)
                        else:
                            nd[dk] = dv
                    dirs[name] = nd
                else:
                    dirs[name] = dict(d) if isinstance(d, dict) else d
            out["directions"] = dirs
        else:
            out[k] = v
    return out


def _load_json_specs(code_dir: Path, registry: Dict[str, dict]) -> None:
    """读取 platforms/<code>/*.json，深合并进 registry（失败跳过）。"""
    if not code_dir.is_dir():
        return
    for f in sorted(code_dir.glob("*.json")):
        try:
            ext = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("平台配置 %s 解析失败，已忽略（保留内置默认）：%s", f.name, e)
            continue
        key = ext.get("key")
        if not key:
            logger.warning("平台配置 %s 缺少 key 字段，已忽略", f.name)
            continue
        registry[key] = _deep_merge(registry.get(key, {}), ext)


class _RegistrationAPI:
    """传给插件 register(api) 的注册接口。"""

    def __init__(self, registry: Dict[str, dict], code: str):
        self._registry = registry
        self.code = code

    def register_platform(self, spec_dict: dict, handler=None) -> None:
        """注册/覆盖一个平台声明；可选注册自定义 handler。"""
        key = spec_dict.get("key")
        if not key:
            raise ValueError("register_platform 需要 spec_dict['key']")
        self._registry[key] = _deep_merge(self._registry.get(key, {}), spec_dict)
        if handler is not None:
            register_handler(spec_dict.get("handler", key), handler)


def _import_isolated(path: Path):
    """按文件路径隔离导入一个插件模块（唯一模块名，不污染 sys.path）。"""
    mod_name = f"_bank_platform_plugin_{path.stem}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法为 {path.name} 创建模块 spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _add_vendor_to_syspath(vendor_dir: Path) -> None:
    """把 platforms/vendor/ 加入 sys.path，供插件 import 纯 Python 依赖。"""
    if not vendor_dir.is_dir():
        return
    vp = str(vendor_dir.resolve())
    if vp not in sys.path:
        sys.path.insert(0, vp)


def _load_plugins(plugins_dir: Path, code: str, registry: Dict[str, dict]) -> None:
    """加载 platforms/plugins/*.py（失败隔离，单个插件异常不影响其它）。"""
    if not plugins_dir.is_dir():
        return
    _add_vendor_to_syspath(plugins_dir.parent / "vendor")
    for f in sorted(plugins_dir.glob("*.py")):
        if f.name.startswith("_"):
            continue
        try:
            mod = _import_isolated(f)
            # 插件可声明目标代号：CODE = "6" 或 CODES = ["5", "6"]，不匹配则跳过
            target = getattr(mod, "CODE", None)
            codes = getattr(mod, "CODES", None)
            if target is not None and str(target) != str(code):
                continue
            if codes is not None and str(code) not in [str(c) for c in codes]:
                continue
            if hasattr(mod, "register") and callable(mod.register):
                mod.register(_RegistrationAPI(registry, code))
            elif isinstance(getattr(mod, "PLATFORM", None), dict):
                key = mod.PLATFORM.get("key")
                if key:
                    registry[key] = _deep_merge(registry.get(key, {}), mod.PLATFORM)
                else:
                    logger.warning("插件 %s 的 PLATFORM 缺少 key，已忽略", f.name)
                    continue
            else:
                logger.warning("插件 %s 未提供 register() 或 PLATFORM，已忽略", f.name)
                continue
            logger.info("已加载平台插件：%s", f.name)
        except Exception as e:
            logger.warning("插件 %s 加载失败，已跳过（不影响其它平台）：%s", f.name, e)


def _builtin_specs_for(code: str) -> List[dict]:
    """取某代号的内置默认声明。"""
    if str(code) == "6":
        from .config6 import BUILTIN_SPECS_6
        return BUILTIN_SPECS_6
    if str(code) == "5":
        from .config5 import BUILTIN_SPECS_5
        return BUILTIN_SPECS_5
    return []


def load_platform_registry(
    code: str = "6",
    builtin_specs: Optional[List[dict]] = None,
    platforms_dir: Optional[Path] = None,
) -> List[PlatformSpec]:
    """构建某代号的平台注册表：内置 → JSON → 插件，按 priority 升序返回 PlatformSpec 列表。

    参数均可注入，便于测试；默认从内置常量 + get_platforms_dir() 读取。
    """
    if builtin_specs is None:
        builtin_specs = _builtin_specs_for(code)
    registry: Dict[str, dict] = {s["key"]: dict(s) for s in builtin_specs}

    base_dir = platforms_dir if platforms_dir is not None else get_platforms_dir()
    _load_json_specs(base_dir / str(code), registry)
    _load_plugins(base_dir / "plugins", str(code), registry)

    specs: List[PlatformSpec] = []
    for data in registry.values():
        if not data.get("enabled", True):
            continue
        try:
            specs.append(PlatformSpec.from_dict(data))
        except Exception as e:
            logger.warning("平台 %s 声明不完整，已跳过：%s", data.get("key"), e)
    specs.sort(key=lambda s: s.priority)
    return specs
