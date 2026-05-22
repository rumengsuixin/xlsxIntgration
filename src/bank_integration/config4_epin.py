"""代号4 子功能：1epin.com 浏览器自动化配置。"""
from .config import DATA_DIR, OUTPUT_DIR
from .config4 import _get_positive_int_config

TARGET_URL_EPIN = "https://www.1epin.com/siparislerim"
CHROME_PROFILE_DIR_EPIN = DATA_DIR / "browser_profile" / "4_epin"
OUTPUT_DIR_EPIN = OUTPUT_DIR / "4_epin"

MODE4_EPIN_DEBUG_PORT_ENV = "MODE4_EPIN_DEBUG_PORT"
MODE4_EPIN_ORDER_LOAD_INTERVAL_SECONDS_ENV = "MODE4_EPIN_ORDER_LOAD_INTERVAL_SECONDS"
DEFAULT_CHROME_DEBUG_PORT_EPIN = 9225
DEFAULT_EPIN_ORDER_LOAD_INTERVAL_SECONDS = 3


def get_epin_debug_port(
    env=None,
    env_path=None,
    default: int = DEFAULT_CHROME_DEBUG_PORT_EPIN,
) -> int:
    """Return 1epin.com Chrome remote debugging port from environment or .env."""
    return _get_positive_int_config(MODE4_EPIN_DEBUG_PORT_ENV, default, env, env_path)


def get_epin_order_load_interval_seconds(
    env=None,
    env_path=None,
    default: int = DEFAULT_EPIN_ORDER_LOAD_INTERVAL_SECONDS,
) -> int:
    """Return 1epin.com order list load interval seconds from environment or .env."""
    return _get_positive_int_config(
        MODE4_EPIN_ORDER_LOAD_INTERVAL_SECONDS_ENV,
        default,
        env,
        env_path,
    )


CHROME_DEBUG_PORT_EPIN = get_epin_debug_port()
EPIN_ORDER_LOAD_INTERVAL_SECONDS = get_epin_order_load_interval_seconds()

TARGET_URL_EPIN_PAYMENTS = "https://www.1epin.com/odemelerim"
