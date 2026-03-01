"""
ETF 매매전략가 — 전체 파이프라인 진입점

실행 흐름:
  1) ETF 데이터 수집 (pykrx)
  2) 4인 에이전트 독립 분석 (Phase 1)
  3) 교차 반론 (Phase 2)
  4) Moderator 종합 판단 (Phase 3)
  5) JSON 리포트 저장 (docs/data/)
  6) Telegram 알림 발송
"""

import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import anthropic

# 프로젝트 루트를 sys.path에 추가 (GitHub Actions 호환)
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from agents import TrendAgent, SectorAgent, MacroAgent, RiskAgent
from orchestrator import DebateEngine, Moderator
from scripts.collect_etf_data import collect_etf_data

# ── 로깅 ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("run_pipeline")

# ── 설정 ────────────────────────────────────────────────────────────────────
MODEL = "claude-haiku-4-5-20251001"   # 비용 최적화 (Haiku)


def load_config() -> dict:
    cfg = {
        "anthropic_api_key": os.environ.get("ANTHROPIC_API_KEY", ""),
        "telegram_token":    os.environ.get("TELEGRAM_TOKEN",    ""),
        "telegram_chat_id":  os.environ.get("TELEGRAM_CHAT_ID",  ""),
        "pages_url":         os.environ.get("PAGES_URL",         ""),
    }
    config_path = ROOT / "config.json"
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            for k, v in json.load(f).items():
                if not cfg.get(k):
                    cfg[k] = v
    return cfg


def send_telegram(verdict: dict, etf_data: dict, date_str: str,
                  token: str, chat_id: str, pages_url: str = "") -> None:
    """Telegram 요약 알림"""
    import requests

    stance_emoji = {"집중": "🟢", "유지": "🟡", "관망": "🔴"}.get(
        verdict.get("final_stance", "유지"), "⚪"
    )

    top_sec_lines = ""
    for s in verdict.get("top_sectors", [])[:3]:
        top_sec_lines += f"  {s['rank']}. {s['sector']} — {s['reason'][:40]}\n"

    mo = etf_data.get("market_overview", {})
    market_line = (
        f"전체:{etf_data.get('total_etf_count',0)}종목  "
        f"상승:{mo.get('advancing',0)}  하락:{mo.get('declining',0)}"
    )

    msg = (
        f"📊 <b>ETF 매매전략가 — {date_str}</b>\n\n"
        f"{stance_emoji} 종합 스탠스: <b>{verdict.get('final_stance','유지')}</b> "
        f"(확신도 {verdict.get('confidence_score',50)}%)\n\n"
        f"🏆 <b>주도섹터 TOP3</b>\n{top_sec_lines}\n"
        f"📈 <b>시장 현황</b>\n  {market_line}\n\n"
        f"💡 <b>핵심 인사이트</b>\n"
    )
    for ins in verdict.get("key_insights", [])[:3]:
        msg += f"  • {ins[:60]}\n"

    if verdict.get("risk_factors"):
        msg += f"\n⚠️ <b>리스크</b>: {verdict['risk_factors'][0][:60]}"

    if pages_url:
        msg += f"\n\n📎 <a href='{pages_url}'>대시보드 보기</a>"

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=20,
        )
        r.raise_for_status()
        logger.info("Telegram 알림 발송 완료")
    except Exception as e:
        logger.warning(f"Telegram 알림 실패: {e}")


def save_report(report: dict, today_str: str) -> None:
    """docs/data/daily_report.json + 날짜별 히스토리 저장"""
    docs_data = ROOT / "docs" / "data"
    docs_data.mkdir(parents=True, exist_ok=True)
    history   = docs_data / "history"
    history.mkdir(exist_ok=True)

    latest_path  = docs_data / "daily_report.json"
    history_path = history   / f"{today_str}.json"

    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    latest_path.write_text(payload,  encoding="utf-8")
    history_path.write_text(payload, encoding="utf-8")
    logger.info(f"리포트 저장 완료: {latest_path}")


def main() -> dict:
    cfg       = load_config()
    today_str = date.today().strftime("%Y-%m-%d")

    if not cfg["anthropic_api_key"]:
        logger.error("ANTHROPIC_API_KEY 미설정")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=cfg["anthropic_api_key"])

    # ── 1) 데이터 수집 ────────────────────────────────────────────────────
    logger.info("━━━ Phase 0: ETF 데이터 수집 ━━━")
    try:
        etf_data = collect_etf_data()
    except Exception as exc:
        err_msg = str(exc)
        logger.error("ETF 데이터 수집 실패: %s", err_msg)
        # Telegram 오류 알림 후 graceful exit (워크플로우는 success로 종료)
        if cfg["telegram_token"] and cfg["telegram_chat_id"]:
            import requests as _req
            try:
                _req.post(
                    f"https://api.telegram.org/bot{cfg['telegram_token']}/sendMessage",
                    json={
                        "chat_id": cfg["telegram_chat_id"],
                        "text": (
                            f"⚠️ <b>ETF 매매전략가 — 데이터 수집 실패</b> ({today_str})\n\n"
                            f"KRX API 응답 없음 또는 오류:\n<code>{err_msg[:200]}</code>\n\n"
                            f"이전 리포트 유지 중입니다."
                        ),
                        "parse_mode": "HTML",
                    },
                    timeout=15,
                )
            except Exception as te:
                logger.warning("Telegram 알림도 실패: %s", te)
        sys.exit(0)   # graceful exit → 워크플로우 success로 처리

    logger.info(
        "수집 완료: %d종목 / 상승:%d 하락:%d",
        etf_data["total_etf_count"],
        etf_data["market_overview"]["advancing"],
        etf_data["market_overview"]["declining"],
    )

    # ── 2~3) 에이전트 토론 ────────────────────────────────────────────────
    logger.info("━━━ Phase 1+2: 에이전트 토론 ━━━")
    agents = [
        TrendAgent(client, MODEL),
        SectorAgent(client, MODEL),
        MacroAgent(client, MODEL),
        RiskAgent(client, MODEL),
    ]
    engine       = DebateEngine(agents)
    debate_result = engine.run(etf_data)

    # ── 4) Moderator 종합 ─────────────────────────────────────────────────
    logger.info("━━━ Phase 3: Moderator 종합 ━━━")
    moderator = Moderator(client, MODEL)
    verdict   = moderator.synthesize(
        reports  = debate_result["phase1_reports"],
        critiques = debate_result["phase2_critiques"],
        etf_data  = etf_data,
    )

    logger.info(
        f"✅ 최종 스탠스: {verdict['final_stance']} "
        f"(확신도 {verdict['confidence_score']}%)  "
        f"주도섹터: {[s['sector'] for s in verdict.get('top_sectors', [])]}"
    )

    # ── 5) 리포트 저장 ────────────────────────────────────────────────────
    report = {
        "date":         today_str,
        "generated_at": etf_data.get("collected_at", today_str),
        "etf_data":     etf_data,
        "debate":       debate_result,
        "verdict":      verdict,
    }
    save_report(report, today_str)

    # ── 6) Telegram 알림 ─────────────────────────────────────────────────
    if cfg["telegram_token"] and cfg["telegram_chat_id"]:
        send_telegram(
            verdict   = verdict,
            etf_data  = etf_data,
            date_str  = today_str,
            token     = cfg["telegram_token"],
            chat_id   = cfg["telegram_chat_id"],
            pages_url = cfg.get("pages_url", ""),
        )

    return report


if __name__ == "__main__":
    main()
