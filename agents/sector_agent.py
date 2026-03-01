"""
🏭 섹터 전략가 (Sector Agent)
섹터 로테이션과 자금 흐름을 읽는 바텀업 분석가.
어떤 섹터에 돈이 몰리고 어디서 빠지는지를 포착해 주도섹터를 선별한다.
"""

from .base_agent import AgentReport, AgentCritique, BaseAgent
import anthropic

SYSTEM_PROMPT = """당신은 '섹터매니저(Sector Manager)'라는 이름의 섹터 로테이션 전략가입니다.

[페르소나]
- 개별 ETF보다 섹터 전체의 흐름과 자금 이동에 집중합니다.
- "어디서 돈이 들어오고, 어디서 빠지는가"가 핵심 질문입니다.
- 단기(1일)보다 중기(1주·1개월) 흐름이 더 의미 있다고 봅니다.
- 반론 시: 상대의 섹터 해석 논리와 지속성 근거를 검증합니다.

[출력 형식] 한국어, 반드시 JSON만 반환"""


class SectorAgent(BaseAgent):
    def __init__(self, client: anthropic.Anthropic, model: str):
        super().__init__(client, model)
        self.name          = "섹터 전략가"
        self.role          = "섹터 로테이션·자금 흐름 전문"
        self.avatar        = "🏭"
        self.system_prompt = SYSTEM_PROMPT

    # ── Phase 1 ─────────────────────────────────────────────────────────────
    def analyze(self, etf_data: dict) -> AgentReport:
        ctx = self._build_base_context(etf_data)

        # 섹터별 중기 흐름 보강
        sec_lines = ["[섹터 중기 흐름 (1w · 1m · YTD)]"]
        for sec, info in etf_data.get("sector_summary", {}).items():
            if sec in ("레버리지", "인버스"):
                continue
            sec_lines.append(
                f"  {sec:12s}  "
                f"1w:{info.get('avg_ret_1w') or 0:+.2f}%  "
                f"1m:{info.get('avg_ret_1m') or 0:+.2f}%  "
                f"YTD:{info.get('avg_ret_ytd') or 0:+.2f}%  "
                f"거래대금:{info['total_trading_value']//1e8:.0f}억"
            )
        extra = "\n".join(sec_lines)

        prompt = f"""{ctx}

{extra}

[분석 과제]
당신은 섹터 로테이션 전략가입니다. 위 데이터에서:
1. 1일·1주·1개월 흐름이 일관되게 강한 주도 섹터는?
2. 자금 집중도(거래대금)와 수익률이 동시에 높은 섹터는?
3. 향후 로테이션 가능성이 있는 소외 섹터는?

반드시 아래 JSON 형식만 반환:
{{
  "analysis": "300자 이상의 상세 분석 (섹터 흐름 중심)",
  "key_points": ["핵심1", "핵심2", "핵심3"],
  "confidence_score": 72,
  "stance": "집중",
  "top_sectors": ["섹터1", "섹터2", "섹터3"],
  "watch_etfs": [
    {{"ticker": "티커", "name": "ETF명", "reason": "섹터 대표성 이유"}}
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
        prompt = f"""당신의 섹터 로테이션 관점에서 아래 분석에 핵심 반론을 제시하세요.

[{other.agent_name}의 분석]
스탠스: {other.stance} (확신도: {other.confidence_score})
주장: {other.analysis[:400]}
주목 섹터: {', '.join(other.top_sectors)}

[반론 가이드]
- 중기 섹터 흐름과 자금 지속성 측면에서 상대 주장의 허점을 지적하세요
- 150~250자, 논리적으로만
- 반론만 작성 (JSON 불필요)"""

        raw = self._call_llm([{"role": "user", "content": prompt}], max_tokens=512)
        return AgentCritique(
            from_agent = self.name,
            to_agent   = other.agent_name,
            critique   = raw.strip()[:400],
        )
