"""
🌐 매크로 분석가 (Macro Agent)
거시경제 맥락에서 ETF 흐름을 해석하는 탑다운 분석가.
지수 흐름, 업종 로직, 글로벌 상관관계를 통해 주도 섹터의 배경을 설명한다.
"""

from .base_agent import AgentReport, AgentCritique, BaseAgent
import anthropic

SYSTEM_PROMPT = """당신은 '매크로(Macro)'라는 이름의 거시경제 분석가입니다.

[페르소나]
- 시장을 위에서 아래로(탑다운) 바라봅니다.
- 코스피/코스닥 지수, 원달러 환율, 글로벌 증시 방향이 핵심 변수입니다.
- 섹터의 '왜 오르는가'를 경제 논리로 설명합니다.
- 반론 시: 상대의 바텀업 논리가 거시 환경과 충돌하는 지점을 지적합니다.

[출력 형식] 한국어, 반드시 JSON만 반환"""


class MacroAgent(BaseAgent):
    def __init__(self, client: anthropic.Anthropic, model: str):
        super().__init__(client, model)
        self.name          = "매크로 분석가"
        self.role          = "거시경제·탑다운 시장 해석 전문"
        self.avatar        = "🌐"
        self.system_prompt = SYSTEM_PROMPT

    # ── Phase 1 ─────────────────────────────────────────────────────────────
    def analyze(self, etf_data: dict) -> AgentReport:
        ctx = self._build_base_context(etf_data)

        # 국내 지수 ETF vs 해외 ETF 비교 보강
        sec_summary = etf_data.get("sector_summary", {})
        kospi_info  = sec_summary.get("국내대형주", {})
        us_info     = sec_summary.get("미국주식", {})
        em_info     = sec_summary.get("신흥국해외", {})
        bond_info   = sec_summary.get("채권", {})

        macro_ctx = (
            f"[주요 자산군 성과]\n"
            f"  국내대형주 1d:{kospi_info.get('avg_ret_1d') or 0:+.2f}%  YTD:{kospi_info.get('avg_ret_ytd') or 0:+.2f}%\n"
            f"  미국주식   1d:{us_info.get('avg_ret_1d') or 0:+.2f}%  YTD:{us_info.get('avg_ret_ytd') or 0:+.2f}%\n"
            f"  신흥국     1d:{em_info.get('avg_ret_1d') or 0:+.2f}%  YTD:{em_info.get('avg_ret_ytd') or 0:+.2f}%\n"
            f"  채권       1d:{bond_info.get('avg_ret_1d') or 0:+.2f}%  YTD:{bond_info.get('avg_ret_ytd') or 0:+.2f}%"
        )

        prompt = f"""{ctx}

{macro_ctx}

[분석 과제]
당신은 거시경제 분석가입니다. 위 데이터에서:
1. 현재 시장 국면(Risk-on/Risk-off/혼조)은?
2. 강한 섹터의 움직임이 거시 논리와 일치하는가?
3. 향후 주목해야 할 거시 변수와 그 영향 섹터는?

반드시 아래 JSON 형식만 반환:
{{
  "analysis": "300자 이상의 거시경제 관점 분석",
  "key_points": ["핵심1", "핵심2", "핵심3"],
  "confidence_score": 68,
  "stance": "유지",
  "top_sectors": ["섹터1", "섹터2", "섹터3"],
  "watch_etfs": [
    {{"ticker": "티커", "name": "ETF명", "reason": "거시 논리에 맞는 이유"}}
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
            stance           = d.get("stance", "유지"),
            top_sectors      = d.get("top_sectors", []),
            watch_etfs       = d.get("watch_etfs", []),
        )

    # ── Phase 2 ─────────────────────────────────────────────────────────────
    def critique(self, other: AgentReport, etf_data: dict) -> AgentCritique:
        prompt = f"""당신의 거시경제 관점에서 아래 분석에 핵심 반론을 제시하세요.

[{other.agent_name}의 분석]
스탠스: {other.stance} (확신도: {other.confidence_score})
주장: {other.analysis[:400]}
주목 섹터: {', '.join(other.top_sectors)}

[반론 가이드]
- 거시 환경(지수·금리·환율·글로벌 흐름)과 상대 주장이 충돌하는 지점을 지적하세요
- 150~250자, 탑다운 논리로만
- 반론만 작성 (JSON 불필요)"""

        raw = self._call_llm([{"role": "user", "content": prompt}], max_tokens=512)
        return AgentCritique(
            from_agent = self.name,
            to_agent   = other.agent_name,
            critique   = raw.strip()[:400],
        )
