"""
ETF 데이터 수집 모듈
Primary  : pykrx  (KRX 직접, 국내 환경 최적)
Fallback : yfinance (해외 IP·공휴일 대응, GitHub Actions 안정)
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

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

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

# ─── 주요 한국 ETF 내장 목록 (yfinance fallback용) ──────────────────────────
# 섹터별 대표 ETF: (ticker_6digit, name)
KOREAN_ETF_LIST = [
    # 국내대형주
    ("069500","KODEX 200"),("102110","TIGER 200"),("278540","KODEX MSCI Korea TR"),
    ("278530","KODEX 200TR"),("143460","KINDEX 200"),("251340","KODEX 코스피"),
    ("105190","TIGER MSCI Korea"),("379800","KODEX MSCI Korea"),
    # 국내중소형
    ("229200","KODEX 코스닥150"),("232080","TIGER 코스닥150"),("261240","KODEX 코스닥150레버리지"),
    ("278600","KODEX 중소형주"),("091180","TIGER 중형주"),("102970","TIGER 200중소형"),
    # 반도체
    ("091160","KODEX 반도체"),("266370","TIGER 반도체"),("364980","TIGER KRX반도체"),
    ("410870","TIGER 필라델피아반도체"),("448290","KODEX AI반도체"),("472620","TIGER AI반도체"),
    # 2차전지
    ("305720","KODEX 2차전지산업"),("371460","TIGER 2차전지테마"),("385510","KODEX 2차전지핵심소재"),
    ("462830","KODEX K-배터리"),("306620","KODEX 배터리소재"),
    # 바이오헬스
    ("244580","KODEX 바이오"),("227550","TIGER 헬스케어"),("203780","TIGER 200헬스케어"),
    ("364990","KODEX 바이오플러스헬스케어"),("139220","TIGER 헬스케어"),
    # IT기술
    ("152100","ARIRANG 200IT"),("228800","KODEX 200IT TR"),("371150","TIGER KRX게임K-뉴딜"),
    ("411060","TIGER AI코리아그로스"),("448300","KODEX AI테크놀로지"),
    # 친환경ESG
    ("375270","KODEX K-신재생에너지"),("381180","TIGER KRX클린에너지지수"),
    ("395160","TIGER 탄소효율그린뉴딜"),("385540","TIGER글로벌탄소배출권선물ICE"),
    # 방산우주
    ("466940","KODEX K-방산"),("468380","TIGER K-방산&우주"),("458760","ACE K방산&우주"),
    # 금융
    ("102960","KODEX 은행"),("091170","KODEX 증권"),("140710","KODEX 보험"),
    ("203130","TIGER 200금융"),("290080","TIGER KRX300금융"),
    # 소비재유통
    ("140720","KODEX 소비재"),("203160","TIGER 200경기소비재"),
    ("091200","TIGER 200생활소비재"),
    # 에너지원자재
    ("261220","KODEX WTI원유선물"),("243880","TIGER 원유선물"),
    ("319640","KODEX 미국S&P에너지산업"),("396520","TIGER 미국S&P500에너지"),
    # 금귀금속
    ("132030","KODEX 골드선물"),("214980","TIGER 골드선물"),
    ("319660","KODEX 미국S&P귀금속산업"),
    # 부동산리츠
    ("182480","TIGER 부동산인프라고배당"),("395750","KODEX 부동산리츠인프라"),
    ("214890","TIGER 미국MSCI리츠"),("332620","KODEX 미국부동산리츠"),
    # 미국주식
    ("133690","TIGER 미국S&P500선물"),("143850","TIGER 미국나스닥100"),
    ("360750","TIGER 미국S&P500"),("379810","KODEX 미국S&P500TR"),
    ("453850","TIGER 미국배당다우존스"),("487190","KODEX 미국나스닥100TR"),
    ("385590","TIGER 미국테크TOP10INDXX"),("411980","TIGER 미국AI빅테크TOP10 INDXX"),
    # 중국주식
    ("100910","ARIRANG 선진국MSCI"),("238720","TIGER 차이나CSI300"),
    ("168580","KODEX 중국본토CSI300"),("371270","TIGER 차이나항셍테크"),
    # 신흥국해외
    ("143460","KINDEX 200"),("245710","KODEX 일본TOPIX100"),
    ("195980","KODEX 선진국MSCI World"),("278540","KODEX MSCI Korea TR"),
    ("360200","TIGER 유로스탁스50"),("329200","TIGER 인도니프티50"),
    ("396520","TIGER 미국S&P500에너지"),
    # 채권
    ("114820","TIGER 국채3년"),("157450","TIGER 미국채10년선물"),
    ("152380","TIGER 단기채권"),("136340","KINDEX 국고채10년"),
    ("308620","TIGER 단기통안채"),("385560","TIGER 미국달러단기채권"),
    ("411060","TIGER AI코리아그로스"),
    # 레버리지
    ("122630","KODEX 레버리지"),("233740","KODEX 코스닥150레버리지"),
    ("105010","TIGER 차이나A300레버리지"),
    # 인버스
    ("114800","KODEX 인버스"),("219390","KODEX 코스닥150인버스"),
    ("252670","KODEX 200선물인버스2X"),("251340","KODEX 코스피"),
]
# 중복 제거
_seen = set()
KOREAN_ETF_LIST = [(c, n) for c, n in KOREAN_ETF_LIST if not (c in _seen or _seen.add(c))]


def classify_sector(name: str) -> str:
    """ETF 이름 기반 섹터 자동 분류 (우선순위: 레버리지 > 인버스 > 테마형 > 자산유형)"""
    for sector in ("레버리지", "인버스"):
        for kw in SECTOR_MAP[sector]:
            if kw.upper() in name.upper():
                return sector
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


# ─── pykrx 수집 ─────────────────────────────────────────────────────────────

def _safe_ohlcv(date_str: str, retries: int = 2) -> pd.DataFrame:
    """pykrx get_etf_ohlcv_by_ticker 호출 (지수 백오프 재시도 포함)"""
    if krx is None:
        return pd.DataFrame()
    for attempt in range(retries + 1):
        try:
            df = krx.get_etf_ohlcv_by_ticker(date_str)
            if df is not None and not df.empty:
                return df
            logger.warning("[%s] 빈 데이터 반환 (attempt %d/%d)", date_str, attempt + 1, retries + 1)
        except Exception as e:
            logger.warning("[%s] attempt %d/%d 실패: %s", date_str, attempt + 1, retries + 1, str(e)[:120])
        wait_sec = 2 * (attempt + 1)
        if attempt < retries:
            logger.info("  %d초 후 재시도...", wait_sec)
            time.sleep(wait_sec)
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
        logger.warning("등락률 계산 실패: %s", e)
        return {}


def _collect_via_pykrx(today_str, d1_str, d7_str, d30_str, ytd_str) -> pd.DataFrame | None:
    """pykrx 기반 ETF 데이터 수집. 실패 시 None 반환"""
    df_today = _safe_ohlcv(today_str)
    if df_today.empty:
        today_str_fb = d1_str
        df_today = _safe_ohlcv(today_str_fb)
    if df_today.empty:
        return None, None, None, None, None, None

    df_d1  = _safe_ohlcv(d1_str)
    df_d7  = _safe_ohlcv(d7_str)
    df_d30 = _safe_ohlcv(d30_str)
    df_ytd = _safe_ohlcv(ytd_str)

    ret_1d  = _calc_returns(df_d1,  df_today)
    ret_1w  = _calc_returns(df_d7,  df_today)
    ret_1m  = _calc_returns(df_d30, df_today)
    ret_ytd = _calc_returns(df_ytd, df_today)

    tickers = df_today.index.tolist()
    names = {}
    for tk in tickers:
        try:
            names[tk] = krx.get_market_ticker_name(tk)
        except Exception:
            names[tk] = tk

    etf_list = []
    for tk in tickers:
        name = names.get(tk, tk)
        row  = df_today.loc[tk]
        etf_list.append({
            "ticker":        tk,
            "name":          name,
            "sector":        classify_sector(name),
            "price":         float(row.get("종가", 0)),
            "volume":        int(row.get("거래량", 0)),
            "trading_value": int(row.get("거래대금", 0)),
            "ret_1d":        ret_1d.get(tk),
            "ret_1w":        ret_1w.get(tk),
            "ret_1m":        ret_1m.get(tk),
            "ret_ytd":       ret_ytd.get(tk),
        })

    logger.info("pykrx 수집 완료: %d종목", len(etf_list))
    return etf_list


# ─── yfinance fallback ───────────────────────────────────────────────────────

def _collect_via_yfinance(today: date) -> list | None:
    """yfinance 기반 ETF 데이터 수집 (KRX 차단 시 fallback)"""
    if not HAS_YF:
        return None

    logger.info("yfinance fallback 수집 시작 (%d종목 대상)", len(KOREAN_ETF_LIST))

    tickers_ks  = [f"{c}.KS" for c, _ in KOREAN_ETF_LIST]
    code_name   = {c: n for c, n in KOREAN_ETF_LIST}

    # 최근 35 영업일치 다운로드 (1d/1w/1m/YTD 계산용)
    import warnings
    warnings.filterwarnings('ignore')
    try:
        raw = yf.download(
            tickers_ks,
            period="60d",
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        logger.error("yfinance download 실패: %s", e)
        return None

    # Close/Volume 추출
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            close_df  = raw["Close"]
            volume_df = raw["Volume"]
        else:
            close_df  = raw[["Close"]]
            volume_df = raw[["Volume"]]
    except Exception as e:
        logger.error("yfinance 데이터 파싱 실패: %s", e)
        return None

    # 유효한 날짜 목록 (NaN 아닌 날)
    close_df = close_df.dropna(how="all")
    if close_df.empty:
        logger.warning("yfinance: 유효 데이터 없음")
        return None

    dates_sorted = sorted(close_df.index)
    last_date    = dates_sorted[-1]
    ytd_start    = date(today.year, 1, 2)

    def _get_closest_idx(target_date):
        """target_date 이전 가장 가까운 날짜 인덱스"""
        td = pd.Timestamp(target_date)
        past = [d for d in dates_sorted if d <= td]
        return past[-1] if past else dates_sorted[0]

    idx_today = last_date
    idx_1d    = _get_closest_idx(last_date - timedelta(days=1))
    idx_1w    = _get_closest_idx(last_date - timedelta(days=7))
    idx_1m    = _get_closest_idx(last_date - timedelta(days=30))
    idx_ytd   = _get_closest_idx(ytd_start)

    def _ret(base_idx, curr_idx, col):
        try:
            b = close_df.loc[base_idx, col]
            c = close_df.loc[curr_idx, col]
            if pd.isna(b) or pd.isna(c) or b == 0:
                return None
            return round(float((c - b) / b * 100), 2)
        except Exception:
            return None

    etf_list = []
    for code, name in KOREAN_ETF_LIST:
        ks = f"{code}.KS"
        if ks not in close_df.columns:
            continue
        try:
            price = float(close_df.loc[idx_today, ks])
            if pd.isna(price) or price == 0:
                continue
            vol = 0
            try:
                vol = int(volume_df.loc[idx_today, ks]) if ks in volume_df.columns else 0
            except Exception:
                pass

            etf_list.append({
                "ticker":        code,
                "name":          name,
                "sector":        classify_sector(name),
                "price":         price,
                "volume":        vol,
                "trading_value": int(price * vol) if vol else 0,
                "ret_1d":        _ret(idx_1d,  idx_today, ks),
                "ret_1w":        _ret(idx_1w,  idx_today, ks),
                "ret_1m":        _ret(idx_1m,  idx_today, ks),
                "ret_ytd":       _ret(idx_ytd, idx_today, ks),
            })
        except Exception as e:
            logger.debug("yfinance 파싱 오류 %s: %s", code, e)

    logger.info("yfinance 수집 완료: %d종목 (마지막 거래일: %s)", len(etf_list), idx_today.date())
    return etf_list if etf_list else None


# ─── 메인 함수 ───────────────────────────────────────────────────────────────

def collect_etf_data() -> dict:
    """
    국내 상장 ETF 전종목 데이터 수집 및 가공
    pykrx 실패 시 yfinance fallback 자동 전환
    Returns: {collected_at, etf_list, sector_summary, sector_ranking_1d, market_overview, ...}
    """
    today = date.today()
    today_str  = _nearest_biz_date(today)
    d1_str     = _nearest_biz_date(today - timedelta(days=1))
    d7_str     = _nearest_biz_date(today - timedelta(days=7))
    d30_str    = _nearest_biz_date(today - timedelta(days=30))
    ytd_str    = _nearest_biz_date(date(today.year, 1, 2))

    logger.info(
        "수집 기준일 → today:%s  1d:%s  1w:%s  1m:%s  YTD:%s",
        today_str, d1_str, d7_str, d30_str, ytd_str,
    )

    # ── 1차 시도: pykrx ──────────────────────────────────────────────────────
    etf_list = None
    if krx is not None:
        logger.info("pykrx 수집 시도 중...")
        etf_list = _collect_via_pykrx(today_str, d1_str, d7_str, d30_str, ytd_str)

    # ── 2차 시도: yfinance fallback ─────────────────────────────────────────
    if not etf_list:
        logger.info("pykrx 실패 → yfinance fallback 전환")
        etf_list = _collect_via_yfinance(today)

    if not etf_list:
        raise RuntimeError("ETF 데이터 수집 실패: pykrx & yfinance 모두 응답 없음")

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
            "count":               len(sub),
            "avg_ret_1d":          _avg("ret_1d"),
            "avg_ret_1w":          _avg("ret_1w"),
            "avg_ret_1m":          _avg("ret_1m"),
            "avg_ret_ytd":         _avg("ret_ytd"),
            "total_trading_value": int(sub["trading_value"].sum()),
            "top_etfs":            top3,
        }

    sector_ranking_1d = sorted(
        [{"sector": s, "avg_ret": v["avg_ret_1d"]}
         for s, v in sector_summary.items() if v["avg_ret_1d"] is not None],
        key=lambda x: x["avg_ret"], reverse=True,
    )

    adv = int((df["ret_1d"] > 0).sum())
    dec = int((df["ret_1d"] < 0).sum())
    unc = int((df["ret_1d"].isna() | (df["ret_1d"] == 0)).sum())

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

    logger.info(
        "ETF 수집 완료: %d종목 / 상승:%d 하락:%d 보합:%d",
        len(etf_list), adv, dec, unc,
    )

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
            "avg_ret_1d":          round(float(df["ret_1d"].dropna().mean()), 2)
                                   if not df["ret_1d"].dropna().empty else 0.0,
            "advancing":           adv,
            "declining":           dec,
            "unchanged":           unc,
        },
    }
