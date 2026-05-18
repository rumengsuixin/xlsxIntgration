"""Mode 4 browser export configuration."""

import os
from pathlib import Path
from typing import Mapping, Optional

from .config import DATA_DIR, OUTPUT_DIR, PROJECT_ROOT


MODE4_BATCH_WAIT_SECONDS_ENV = "MODE4_BATCH_WAIT_SECONDS"
MODE4_BATCH_SIZE_ENV = "MODE4_BATCH_SIZE"
MODE4_RETRY_LIMIT_ENV = "MODE4_RETRY_LIMIT"
MODE4_MISSING_CHECK_CHANCES_ENV = "MODE4_MISSING_CHECK_CHANCES"
MODE4_CHECK_INTERVAL_SECONDS_ENV = "MODE4_CHECK_INTERVAL_SECONDS"
DEFAULT_EXPORT_BATCH_WAIT_SECONDS_4 = 10
DEFAULT_EXPORT_BATCH_SIZE_4 = 5
DEFAULT_EXPORT_RETRY_LIMIT_4 = 3
DEFAULT_EXPORT_MISSING_CHECK_CHANCES_4 = 10
DEFAULT_EXPORT_CHECK_INTERVAL_SECONDS_4 = 2


def _parse_env_file(env_path: Path) -> dict:
    values = {}
    if not env_path.exists():
        return values

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def get_mode4_batch_wait_seconds(
    env: Optional[Mapping[str, str]] = None,
    env_path: Optional[Path] = None,
    default: int = DEFAULT_EXPORT_BATCH_WAIT_SECONDS_4,
) -> int:
    """Return mode 4 batch wait seconds from environment or .env."""
    return _get_positive_int_config(MODE4_BATCH_WAIT_SECONDS_ENV, default, env, env_path)


def get_mode4_batch_size(
    env: Optional[Mapping[str, str]] = None,
    env_path: Optional[Path] = None,
    default: int = DEFAULT_EXPORT_BATCH_SIZE_4,
) -> int:
    """Return mode 4 export URL count per batch from environment or .env."""
    return _get_positive_int_config(MODE4_BATCH_SIZE_ENV, default, env, env_path)


def get_mode4_retry_limit(
    env: Optional[Mapping[str, str]] = None,
    env_path: Optional[Path] = None,
    default: int = DEFAULT_EXPORT_RETRY_LIMIT_4,
) -> int:
    """Return mode 4 retry limit from environment or .env."""
    return _get_positive_int_config(MODE4_RETRY_LIMIT_ENV, default, env, env_path)


def get_mode4_missing_check_chances(
    env: Optional[Mapping[str, str]] = None,
    env_path: Optional[Path] = None,
    default: int = DEFAULT_EXPORT_MISSING_CHECK_CHANCES_4,
) -> int:
    """Return mode 4 missing file check chances from environment or .env."""
    return _get_positive_int_config(MODE4_MISSING_CHECK_CHANCES_ENV, default, env, env_path)


def get_mode4_check_interval_seconds(
    env: Optional[Mapping[str, str]] = None,
    env_path: Optional[Path] = None,
    default: int = DEFAULT_EXPORT_CHECK_INTERVAL_SECONDS_4,
) -> int:
    """Return mode 4 missing file check interval seconds from environment or .env."""
    return _get_positive_int_config(MODE4_CHECK_INTERVAL_SECONDS_ENV, default, env, env_path)


def _get_positive_int_config(
    name: str,
    default: int,
    env: Optional[Mapping[str, str]] = None,
    env_path: Optional[Path] = None,
) -> int:
    source_env = os.environ if env is None else env
    dotenv_values = _parse_env_file(env_path or PROJECT_ROOT / ".env")

    raw_value = source_env.get(name)
    if raw_value is None:
        raw_value = dotenv_values.get(name)
    if raw_value is None:
        return default

    try:
        parsed = int(str(raw_value).strip())
    except ValueError:
        return default
    if parsed <= 0:
        return default
    return parsed

EXPORT_URL_TEMPLATE = (
    "https://aim1.567okey.com/Rechargeorder/finished?"
    "act=expore&order_num=&user_id=&kind_id=&payment_key=&order_type="
    "&goods_id=&user_type=&tracker_token=&sdate=&edate="
    "&pay_sdate={date}&pay_edate={date}&os=&channel_type=&p=[PAGE]"
)
EXPORT_LOGIN_URL_4 = "https://aim1.567okey.com/Public/login.html"

EXPORT_DOWNLOAD_DIR_4 = OUTPUT_DIR / "4"
CHROME_PROFILE_DIR_4 = DATA_DIR / "browser_profile" / "4"
EXPORT_BATCH_SIZE_4 = get_mode4_batch_size()
EXPORT_BATCH_WAIT_SECONDS_4 = get_mode4_batch_wait_seconds()
EXPORT_RETRY_LIMIT_4 = get_mode4_retry_limit()
EXPORT_MISSING_CHECK_CHANCES_4 = get_mode4_missing_check_chances()
EXPORT_CHECK_INTERVAL_SECONDS_4 = get_mode4_check_interval_seconds()
EXPORT_COMPLETED_SUFFIXES_4 = (".xls", ".xlsx", ".csv")
