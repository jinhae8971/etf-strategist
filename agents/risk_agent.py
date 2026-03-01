"""
🛡️ 리스크 관리자 (Risk Agent)
변동성·하방 리스크·레버리지 신호에 집중하는 방어형 분석가.
시장이 흥분할 때 냉정하게 리스크를 경고하고, 포트폴리오 관점에서 조언한다.
"""

from .base_agent import AgentReport, AgentCritique, BaseAgent
import anthropic

SYSTEM_PROMPT = """당신은 '리스크(Risk Manager)'라는 이름의 방어적 리스크 관리자입니다.

[페르소나]
- 항상 "지금 얼마나 잃을 수 있는가"를 먼저 묻습니다.
- 레버리지·인버스 ETF의 거래대금 비율은 시장 과열/공포 신호입니다.
- 상승 섹터의 지속성보다 반전 리스크를 더 강조합니다.
- 반론 시: 상대 주장의 낙관적 가정과 잠재적 리스크를 지적합니다.

[출력 형식] 한국어, 반드시 JSON만 반환"""


class RiskAgent(BaseAgent):
    def __init__(self, client: anthropic.Anthropic, model: str):
        super().__init__(client, model)
        self.name          = "리스크 관리자"
        self.role          = "변동성·하방 리스크·레버리지 신호 전문"
        self.avatar        = "🛡️"
        self.system_prompt = SYSTEM_PROMPT

    # ── Phase 1 ─────────────────────────────────────────────────────────────
    def analyze(self, etf_data: dict) -> AgentReport:
        ctx = self._build_base_context(etf_data)

        # 레버리지/인버스 신호 추가
        sec_summary = etf_data.get("sector_summary", {})
        lev_info    = sec_summary.get("레버리지", {})
        inv_info    = sec_summary.get("인버스",   {})
        total_tv    = etf_data.get("market_overview", {}).get("total_trading_value", 1)
        lev_tv      = lev_info.get("total_trading_value", 0)
        inv_tv      = inv_info.get("total_trading_value", 0)
        lev_ratio   = lev_tv / total_tv * 100 if total_tv else 0
        inv_ratio   = inv_tv / total_tv * 100 if total_tv else 0

        risk_ctx = (
            f"[레버리지·인버스 신호]\n"
            f"  레버리지 ETF 거래대금 비중: {lev_ratio:.1f}%  "
            f"평균등락: {lev_info.get('avg_ret_1d') or 0:+.2f}%\n"
            f"  인버스   ETF 거래대금 비중: {inv_ratio:.1f}%  "
            f"평균등락: {inv_info.get('avg_ret_1d') or 0:+.2f}%"
        )

        prompt = f"""{ctx}

{risk_ctx}

[분석 과제]
당신은 리스크 관리자입니다. 위 데이터에서:
1. 현재 시장의 과열·공포 신호(레버리지/인버스 비중 등)는?
2. 하락 폭이 크거나 집중 위험이 높은 섹터·ETF는?
3. 안전한 포트폴리오 관점에서 비중을 줄여야 할 영역은?

반드시 아래 JSON 형식만 반환:
{{
  "analysis": "300자 이상의 리스크 중심 분석",
  "key_points": ["리스크1", "리스크2", "리스크3"],
  "confidence_score": 65,
  "stance": "관망",
  "top_sectors": ["그나마 안전한 섹터1", "섹터2"],
  "watch_etfs": [
    {{"ticker": "티커", "name": "ETF명", "reason": "주목 이유 (리스크 헤지 포함)"}}
  ]
}}
stance는 반드시 "집중"/"유지"/"관망" 중 하나"""

        raw = self._call_llm([{"role": "user", "content": prompt}])
        d   = self._parse_json(raw)

        return AgentReport(
            agent_name       = self.name,
            role             = self.role,
            avatar           = self.avatar,
            analysis         = d.get("analysis", raw[:600]),
            key_points       = d.get("key_points", []),
            confidence_score = max(0, min(100, int(d.get("confidence_score", 50)))),
            stance           = d.get("stance", "관망"),
            top_sectors      = d.get("top_sectors", []),
            watch_etfs       = d.get("watch_etfs", []),
        )

    # ── Phase 2 ─────────────────────────────────────────────────────────────
    def critique(self, other: AgentReport, etf_data: dict) -> AgentCritique:
        prompt = f"""당신의 리스크 관리자 관점에서 아래 분석에 핵심 반론을 제시하세요.

[{other.agent_name}의 분석]
스탠스: {other.stance} (확신도: {other.confidence_score})
주장: {other.analysis[:400]}
주목 섹터: {', '.join(other.top_sectors)}

[반론 가이드]
- 상대 주장의 낙관적 가정에 숨겨진 하방 리스크와 반전 가능성을 지적하세요
- 150~250자, 방어적 관점으로만
- 반론만 작성 (JSON 불필요)"""

        raw = self._call_llm([{"role": "user", "content": prompt}], max_tokens=512)
        return AgentCritique(
            from_agent = self.name,
            to_agent   = other.agent_name,
            critique   = raw.strip()[:400],
        )
