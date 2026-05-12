"""Mode 4 browser export configuration."""

from .config import DATA_DIR, OUTPUT_DIR

EXPORT_URL_TEMPLATE = (
    "https://aim1.567okey.com/Rechargeorder/finished?"
    "act=expore&order_num=&user_id=&kind_id=&payment_key=&order_type="
    "&goods_id=&user_type=&tracker_token=&sdate=&edate="
    "&pay_sdate={date}&pay_edate={date}&os=&channel_type=&p=[PAGE]"
)

EXPORT_DOWNLOAD_DIR_4 = OUTPUT_DIR / "4"
CHROME_PROFILE_DIR_4 = DATA_DIR / "browser_profile" / "4"
