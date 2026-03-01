"""
ETF 데이터 수집 모듈
pykrx 기반으로 국내 상장 ETF 전종목의 가격/등락률/섹터 데이터를 수집한다.
"""

import logging
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd

try:
    from pykrx import stock as krx
except ImportError:
    krx = None

logger = logging.getLogger(__name__)

# ─── 섹터 분류 키워드 맵 ────────────────────────────────────────────────────
SECTOR_MAP = {
    "반도체":       ["반도체", "Semiconductor", "SOX", "필라델피아"],
    "2차전지":      ["2차전지", "배터리", "Battery", "리튬", "이차전지"],
    "바이오헬스":   ["바이오", "헬스케어", "Healthcare", "제약", "의료", "바이오테크"],
    "IT기술":       ["IT", "테크", "Technology", "소프트웨어", "인터넷", "게임", "클라우드", "AI", "인공지능"],
    "친환경ESG":    ["친환경", "ESG", "그린", "태양광", "신재생", "수소", "탄소"],
    "방산우주":     ["방산", "우주", "항공", "Defense", "항공우주"],
    "금융":         ["금융", "은행", "보험", "증권", "Finance"],
    "소비재유통":   ["소비재", "유통", "음식료", "화장품", "뷰티", "미디어", "엔터"],
    "에너지원자재": ["에너지", "원자재", "원유", "Oil", "천연가스", "석유"],
    "금귀금속":     ["금", "Gold", "귀금속", "Silver"],
    "부동산리츠":   ["리츠", "REIT", "부동산", "인프라", "Infrastructure"],
    "국내대형주":   ["코스피200", "KOSPI200", "KOSPI100", "코리아", "KRX300"],
    "국내중소형":   ["중소형", "코스닥150", "KOSDAQ150", "코스닥"],
    "미국주식":     ["미국", "S&P", "나스닥", "NASDAQ", "다우", "미국주"],
    "중국주식":     ["중국", "China", "CSI", "항셍", "홍콩"],
    "신흥국해외":   ["신흥국", "EM", "Emerging", "인도", "베트남", "일본", "유럽", "글로벌"],
    "채권":         ["국채", "채권", "Bond", "KTB", "통안채", "단기", "머니마켓", "회사채", "크레딧"],
    "레버리지":     ["레버리지", "2X", "3X", "Leverage", "2배", "레버"],
    "인버스":       ["인버스", "Inverse", "BEAR", "Short", "인버"],
}


def classify_sector(name: str) -> str:
    """ETF 이름 기반 섹터 자동 분류 (우선순위: 레버리지 > 인버스 > 테마형 > 자산유형)"""
    # 레버리지/인버스 최우선
    for sector in ("레버리지", "인버스"):
        for kw in SECTOR_MAP[sector]:
            if kw.upper() in name.upper():
                return sector
    # 나머지 순서대로
    for sector, keywords in SECTOR_MAP.items():
        if sector in ("레버리지", "인버스"):
            continue
        for kw in keywords:
            if kw.upper() in name.upper():
                return sector
    return "기타"


def _nearest_biz_date(target: date, look_back: int = 7) -> str:
    """target 이전 가장 가까운 영업일(월~금) 반환 (YYYYMMDD)"""
    for i in range(look_back + 1):
        d = target - timedelta(days=i)
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")
    return target.strftime("%Y%m%d")


def _safe_ohlcv(date_str: str, retries: int = 2) -> pd.DataFrame:
    """pykrx get_etf_ohlcv_by_ticker 호출 (재시도 포함)"""
    for attempt in range(retries + 1):
        try:
            df = krx.get_etf_ohlcv_by_ticker(date_str)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"[{date_str}] attempt {attempt+1} failed: {e}")
            time.sleep(1)
    return pd.DataFrame()


def _calc_returns(df_base: pd.DataFrame, df_curr: pd.DataFrame) -> dict:
    """두 날짜 OHLCV 데이터프레임으로 등락률(%) 계산"""
    try:
        if df_base.empty or df_curr.empty:
            return {}
        common = df_base.index.intersection(df_curr.index)
        if common.empty:
            return {}
        base = df_base.loc[common, "종가"]
        curr = df_curr.loc[common, "종가"]
        ret = ((curr - base) / base * 100).round(2)
        return ret.to_dict()
    except Exception as e:
        logger.warning(f"등락률 계산 실패: {e}")
        return {}


def collect_etf_data() -> dict:
    """
    국내 상장 ETF 전종목 데이터 수집 및 가공
    Returns: {collected_at, etf_list, sector_summary, sector_ranking, market_overview, ...}
    """
    if krx is None:
        raise RuntimeError("pykrx 미설치. pip install pykrx 실행 필요")

    today = date.today()
    today_str  = _nearest_biz_date(today)
    d1_str     = _nearest_biz_date(today - timedelta(days=1))
    d7_str     = _nearest_biz_date(today - timedelta(days=7))
    d30_str    = _nearest_biz_date(today - timedelta(days=30))
    ytd_str    = _nearest_biz_date(date(today.year, 1, 2))

    logger.info(f"수집 기준일 → today:{today_str}  1d:{d1_str}  1w:{d7_str}  1m:{d30_str}  YTD:{ytd_str}")

    # ─── OHLCV 수집 ────────────────────────────────────────────────────────
    df_today = _safe_ohlcv(today_str)
    if df_today.empty:
        # 당일 장 미개장 → 전일 데이터로 대체
        today_str = d1_str
        df_today  = _safe_ohlcv(today_str)

    df_d1   = _safe_ohlcv(d1_str)
    df_d7   = _safe_ohlcv(d7_str)
    df_d30  = _safe_ohlcv(d30_str)
    df_ytd  = _safe_ohlcv(ytd_str)

    if df_today.empty:
        raise RuntimeError("ETF OHLCV 데이터 수집 실패")

    # ─── 등락률 계산 ────────────────────────────────────────────────────────
    ret_1d  = _calc_returns(df_d1,  df_today)
    ret_1w  = _calc_returns(df_d7,  df_today)
    ret_1m  = _calc_returns(df_d30, df_today)
    ret_ytd = _calc_returns(df_ytd, df_today)

    # ─── ETF 이름 수집 ──────────────────────────────────────────────────────
    tickers = df_today.index.tolist()
    names = {}
    for tk in tickers:
        try:
            names[tk] = krx.get_market_ticker_name(tk)
        except Exception:
            names[tk] = tk

    # ─── ETF 리스트 조합 ────────────────────────────────────────────────────
    etf_list = []
    for tk in tickers:
        name = names.get(tk, tk)
        row  = df_today.loc[tk]
        etf_list.append({
            "ticker":        tk,
            "name":          name,
            "sector":        classify_sector(name),
            "price":         float(row.get("종가",   0)),
            "volume":        int(row.get("거래량", 0)),
            "trading_value": int(row.get("거래대금", 0)),
            "ret_1d":        ret_1d.get(tk),
            "ret_1w":        ret_1w.get(tk),
            "ret_1m":        ret_1m.get(tk),
            "ret_ytd":       ret_ytd.get(tk),
        })

    df = pd.DataFrame(etf_list)

    # ─── 섹터 집계 ──────────────────────────────────────────────────────────
    sector_summary = {}
    for sec in df["sector"].unique():
        sub = df[df["sector"] == sec]

        def _avg(col):
            vals = sub[col].dropna()
            return round(float(vals.mean()), 2) if len(vals) else None

        top3 = (
            sub.dropna(subset=["ret_1d"])
               .nlargest(3, "ret_1d")[["ticker", "name", "ret_1d", "trading_value"]]
               .to_dict("records")
        )
        sector_summary[sec] = {
            "count":              len(sub),
            "avg_ret_1d":         _avg("ret_1d"),
            "avg_ret_1w":         _avg("ret_1w"),
            "avg_ret_1m":         _avg("ret_1m"),
            "avg_ret_ytd":        _avg("ret_ytd"),
            "total_trading_value": int(sub["trading_value"].sum()),
            "top_etfs":           top3,
        }

    # 섹터 랭킹 (1일 기준)
    sector_ranking_1d = sorted(
        [{"sector": s, "avg_ret": v["avg_ret_1d"]}
         for s, v in sector_summary.items() if v["avg_ret_1d"] is not None],
        key=lambda x: x["avg_ret"], reverse=True,
    )

    # 전체 시장 개요
    adv = int((df["ret_1d"] > 0).sum())
    dec = int((df["ret_1d"] < 0).sum())
    unc = int((df["ret_1d"] == 0).sum())

    top_gainers = (
        df.dropna(subset=["ret_1d"])
          .nlargest(10, "ret_1d")[["ticker", "name", "sector", "ret_1d", "ret_1w", "trading_value"]]
          .to_dict("records")
    )
    top_losers = (
        df.dropna(subset=["ret_1d"])
          .nsmallest(10, "ret_1d")[["ticker", "name", "sector", "ret_1d", "ret_1w", "trading_value"]]
          .to_dict("records")
    )
    top_volume = (
        df.nlargest(10, "trading_value")[["ticker", "name", "sector", "ret_1d", "trading_value"]]
          .to_dict("records")
    )

    logger.info(f"ETF 수집 완료: {len(etf_list)}종목 / 상승:{adv} 하락:{dec} 보합:{unc}")

    return {
        "collected_at":      today_str,
        "total_etf_count":   len(etf_list),
        "etf_list":          etf_list,
        "sector_summary":    sector_summary,
        "sector_ranking_1d": sector_ranking_1d,
        "top_gainers_1d":    top_gainers,
        "top_losers_1d":     top_losers,
        "top_volume":        top_volume,
        "market_overview": {
            "total_trading_value": int(df["trading_value"].sum()),
            "avg_ret_1d":          round(float(df["ret_1d"].dropna().mean()), 2) if not df["ret_1d"].dropna().empty else 0,
            "advancing":           adv,
            "declining":           dec,
            "unchanged":           unc,
        },
    }
