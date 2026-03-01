"""
📈 트렌드 분석가 (Trend Agent)
모멘텀·단기 가격 움직임·거래량 급증에 집중하는 퀀트형 에이전트.
뉴스보다 숫자를 신뢰하며, 가장 빠르게 움직이는 섹터와 ETF를 포착한다.
"""

from .base_agent import AgentReport, AgentCritique, BaseAgent
import anthropic

SYSTEM_PROMPT = """당신은 '트렌드(Trend)'라는 이름의 냉철한 모멘텀 분석가입니다.

[페르소나]
- 오직 가격과 거래량 데이터만 신뢰합니다. 뉴스·감정은 후행 지표로 취급합니다.
- 단기(1일·1주) 등락률과 거래대금 급증이 핵심 시그널입니다.
- "추세가 시작됐는가, 지속 가능한가"를 항상 묻습니다.
- 반론 시: 상대 주장의 데이터 취약점(기간, 표본, 인과)을 정밀히 공격합니다.

[출력 형식] 한국어, 반드시 JSON만 반환"""


class TrendAgent(BaseAgent):
    def __init__(self, client: anthropic.Anthropic, model: str):
        super().__init__(client, model)
        self.name          = "트렌드 분석가"
        self.role          = "모멘텀·단기 가격·거래량 전문"
        self.avatar        = "📈"
        self.system_prompt = SYSTEM_PROMPT

    # ── Phase 1 ─────────────────────────────────────────────────────────────
    def analyze(self, etf_data: dict) -> AgentReport:
        ctx = self._build_base_context(etf_data)

        # 거래대금 급증 섹터 추가 컨텍스트
        vol_lines = ["[거래대금 상위 ETF]"]
        for e in etf_data.get("top_volume", [])[:5]:
            val_b = e["trading_value"] / 1e8  # 억원
            vol_lines.append(
                f"  {e['name']} ({e['sector']}) {val_b:.0f}억 1d:{e.get('ret_1d') or 0:+.2f}%"
            )
        extra = "\n".join(vol_lines)

        prompt = f"""{ctx}

{extra}

[분석 과제]
당신은 모멘텀 분석가입니다. 위 데이터에서:
1. 가장 강한 단기 모멘텀을 보이는 섹터는?
2. 거래대금 급증과 가격 상승이 동반된 ETF는? (진짜 수급)
3. 현재 추세의 지속 가능성은?

반드시 아래 JSON 형식만 반환:
{{
  "analysis": "300자 이상의 상세 분석 (구체적 수치 포함)",
  "key_points": ["핵심1", "핵심2", "핵심3"],
  "confidence_score": 70,
  "stance": "집중",
  "top_sectors": ["섹터1", "섹터2", "섹터3"],
  "watch_etfs": [
    {{"ticker": "티커", "name": "ETF명", "reason": "이유"}},
    {{"ticker": "티커", "name": "ETF명", "reason": "이유"}}
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
        prompt = f"""당신의 모멘텀 분석 관점에서 아래 분석에 핵심 반론을 제시하세요.

[{other.agent_name}의 분석]
스탠스: {other.stance} (확신도: {other.confidence_score})
주장: {other.analysis[:400]}
주목 섹터: {', '.join(other.top_sectors)}

[반론 가이드]
- 상대 주장의 데이터 취약점을 가격·거래량 근거로 공격하세요
- 150~250자, 감정 없이 논리적으로
- 반론만 작성 (JSON 불필요)"""

        raw = self._call_llm([{"role": "user", "content": prompt}], max_tokens=512)
        return AgentCritique(
            from_agent = self.name,
            to_agent   = other.agent_name,
            critique   = raw.strip()[:400],
        )
