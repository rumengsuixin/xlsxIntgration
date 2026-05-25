"""代号7：从 XE.com 抓取汇率并缓存到本地 JSON 文件。

核心思路来自 bahramkhanlarov/Currency-Exchange-Rate-Scraper：
  pd.read_html("https://www.xe.com/currencytables/?from=USD&date=YYYY-MM-DD")
  表格含 "USD per unit" 列，直接给出「1 单位指定货币 = X 美元」。
"""

import json
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .config7 import MAX_FALLBACK_DAYS_7, RATE_CACHE_FILE, XE_URL_TEMPLATE_7

logger = logging.getLogger(__name__)


# ── 缓存 I/O ──────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    if RATE_CACHE_FILE.exists():
        try:
            with open(RATE_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("加载汇率缓存失败，将重新抓取：%s", e)
    return {}


def _save_cache(cache: dict) -> None:
    RATE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(RATE_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ── 网络抓取 ──────────────────────────────────────────────────────────────────

def fetch_rates_from_xe(target_date: date) -> Dict[str, float]:
    """从 XE.com 抓取指定日期全部指定货币对 USD 的汇率。

    返回 {"TRY": 0.029, "EUR": 1.087, ...}（1 单位指定货币 = X 美元）。
    网络或解析失败时抛出异常。
    """
    url = XE_URL_TEMPLATE_7.format(date=target_date.isoformat())
    tables = pd.read_html(url)
    df = tables[0]
    df.columns = [str(c).strip() for c in df.columns]

    usd_col = next((c for c in df.columns if "usd per" in str(c).lower()), None)
    if usd_col is None:
        raise ValueError(f"XE.com 表格中未找到 'USD per unit' 列，实际列名：{list(df.columns)}")

    currency_col = df.columns[0]
    result: Dict[str, float] = {}
    for _, row in df.iterrows():
        code = str(row[currency_col]).strip().upper()
        try:
            result[code] = float(row[usd_col])
        except (ValueError, TypeError):
            pass
    return result


# ── 公共查询 API ──────────────────────────────────────────────────────────────

def get_usd_rate(currency: str, target_date: date) -> Optional[float]:
    """返回「1 单位 currency = ? USD」。

    优先读本地缓存；缓存无数据时抓取并写回。
    若当日无法获取（周末/假日/网络故障），向前最多回退 MAX_FALLBACK_DAYS_7 天。
    始终返回 float 或 None（失败时）。
    """
    currency = currency.strip().upper()
    if currency == "USD":
        return 1.0

    cache = _load_cache()

    for delta in range(MAX_FALLBACK_DAYS_7 + 1):
        d = target_date - timedelta(days=delta)
        key = d.isoformat()
        if key in cache and currency in cache[key]:
            if delta > 0:
                logger.debug("汇率 %s %s 回退至 %s", currency, target_date, d)
            return cache[key][currency]
        if key not in cache:
            try:
                rates = fetch_rates_from_xe(d)
                cache[key] = rates
                _save_cache(cache)
                logger.info("已抓取 %s 汇率（%d 种货币）", key, len(rates))
                if currency in rates:
                    return rates[currency]
            except Exception as e:
                logger.warning("抓取 %s 汇率失败：%s", key, e)

    logger.warning("无法获取 %s 在 %s 附近的汇率", currency, target_date)
    return None


def batch_fetch_rates(
    currencies: List[str],
    dates: List[date],
) -> pd.DataFrame:
    """批量抓取日期列表内的汇率，返回 DataFrame（行=日期，列=货币）。

    - 缓存命中的日期跳过网络请求
    - 无法获取的值填 None
    """
    currencies_upper = [c.strip().upper() for c in currencies]
    cache = _load_cache()

    fetched = 0
    hit = 0
    failed = 0

    # 先补全缓存中缺失的日期
    for d in sorted(dates):
        key = d.isoformat()
        if key in cache:
            hit += 1
            continue
        try:
            rates = fetch_rates_from_xe(d)
            cache[key] = rates
            fetched += 1
        except Exception as e:
            logger.warning("抓取 %s 汇率失败：%s", key, e)
            cache[key] = {}  # 标记为已尝试，避免重复请求
            failed += 1

    if fetched > 0 or failed > 0:
        _save_cache(cache)

    logger.info("汇率抓取完成：共 %d 天，缓存命中 %d 天，新抓取 %d 天，失败 %d 天",
                len(dates), hit, fetched, failed)

    # 构造结果 DataFrame
    rows = []
    for d in sorted(dates):
        key = d.isoformat()
        day_rates = cache.get(key, {})
        row: Dict = {"日期": key}
        for curr in currencies_upper:
            val = day_rates.get(curr)
            # 周末/假日回退：向前找最近有效值
            if val is None:
                for delta in range(1, MAX_FALLBACK_DAYS_7 + 1):
                    fb_key = (d - timedelta(days=delta)).isoformat()
                    val = cache.get(fb_key, {}).get(curr)
                    if val is not None:
                        break
            row[curr] = val
        rows.append(row)

    return pd.DataFrame(rows)
