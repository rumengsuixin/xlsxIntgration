"""代号4 子功能：1epin.com 浏览器自动化配置。"""
from .config import DATA_DIR, OUTPUT_DIR
from .config4 import _get_positive_int_config

TARGET_URL_EPIN = "https://www.1epin.com/siparislerim"
CHROME_PROFILE_DIR_EPIN = DATA_DIR / "browser_profile" / "4_epin"
OUTPUT_DIR_EPIN = OUTPUT_DIR / "4_epin"

MODE4_EPIN_DEBUG_PORT_ENV = "MODE4_EPIN_DEBUG_PORT"
DEFAULT_CHROME_DEBUG_PORT_EPIN = 9225


def get_epin_debug_port(
    env=None,
    env_path=None,
    default: int = DEFAULT_CHROME_DEBUG_PORT_EPIN,
) -> int:
    """Return 1epin.com Chrome remote debugging port from environment or .env."""
    return _get_positive_int_config(MODE4_EPIN_DEBUG_PORT_ENV, default, env, env_path)


CHROME_DEBUG_PORT_EPIN = get_epin_debug_port()
