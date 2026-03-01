"""
Moderator (ETF 전략가): Phase 3 — 토론 종합 및 최종 전략 도출

하이브리드 판단:
  1) 규칙 기반 집계: stance 가중 투표 → 초안
  2) LLM 품질 평가: 토론 내용 기반 최종 판단 + 주도섹터 TOP3 + 주목 ETF 선정
"""

import json
import logging
import re

import anthropic

logger = logging.getLogger(__name__)

STANCE_SCORE = {"집중": 1, "유지": 0, "관망": -1}


class Moderator:
    def __init__(self, client: anthropic.Anthropic, model: str):
        self.client = client
        self.model  = model

    def synthesize(self, reports: list, critiques: list, etf_data: dict) -> dict:
        # ── 1) 규칙 기반 집계 ───────────────────────────────────────────────
        rule_stance, avg_conf = self._weighted_vote(reports)

        # ── 2) LLM 종합 판단 ────────────────────────────────────────────────
        debate_text = self._format_debate(reports, critiques)
        sector_text = self._format_sector_ranking(etf_data)

        prompt = f"""아래 에이전트 토론을 종합해 ETF 매매전략가로서 최종 판단을 내려주세요.

{debate_text}

{sector_text}

규칙 기반 선행 판단: {rule_stance}

반드시 아래 JSON 형식만 반환:
{{
  "final_stance": "집중",
  "confidence_score": 72,
  "summary": "200자 이상 종합 근거",
  "top_sectors": [
    {{"rank": 1, "sector": "섹터명", "reason": "이유", "representative_etfs": ["티커1", "티커2"]}},
    {{"rank": 2, "sector": "섹터명", "reason": "이유", "representative_etfs": ["티커1"]}},
    {{"rank": 3, "sector": "섹터명", "reason": "이유", "representative_etfs": ["티커1"]}}
  ],
  "avoid_sectors": ["섹터1", "섹터2"],
  "key_insights": ["인사이트1", "인사이트2", "인사이트3"],
  "risk_factors": ["리스크1", "리스크2"],
  "action_items": ["행동1", "행동2", "행동3"]
}}
final_stance는 반드시 "집중"/"유지"/"관망" 중 하나"""

        try:
            resp = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                system=(
                    "당신은 ETF 매매전략가(Moderator)입니다. "
                    "에이전트들의 토론을 종합해 최종 전략을 도출합니다. "
                    "논리의 질과 데이터 신뢰성으로 판단합니다. JSON만 반환."
                ),
                messages=[{"role": "user", "content": prompt}],
            )
            text = re.sub(r"```(?:json)?\s*|```\s*", "", resp.content[0].text).strip()
            m    = re.search(r"\{[\s\S]*\}", text)
            result = json.loads(m.group()) if m else {}
        except Exception as e:
            logger.error(f"Moderator LLM 실패: {e}")
            result = {}

        return {
            "final_stance":    result.get("final_stance", rule_stance),
            "confidence_score": result.get("confidence_score", int(avg_conf)),
            "summary":         result.get("summary", "집계 완료"),
            "top_sectors":     result.get("top_sectors", []),
            "avoid_sectors":   result.get("avoid_sectors", []),
            "key_insights":    result.get("key_insights", []),
            "risk_factors":    result.get("risk_factors", []),
            "action_items":    result.get("action_items", []),
            "stance_votes":    {r["agent_name"]: r["stance"] for r in reports},
        }

    # ── 내부 유틸 ────────────────────────────────────────────────────────────

    def _weighted_vote(self, reports: list) -> tuple[str, float]:
        total_w, weighted_sum, conf_sum = 0, 0.0, 0.0
        for r in reports:
            w = r.get("confidence_score", 50)
            s = STANCE_SCORE.get(r.get("stance", "유지"), 0)
            weighted_sum += s * w
            total_w      += w
            conf_sum     += w
        if total_w == 0:
            return "유지", 50.0
        score = weighted_sum / total_w
        stance = "집중" if score > 0.3 else ("관망" if score < -0.3 else "유지")
        return stance, conf_sum / max(len(reports), 1)

    def _format_debate(self, reports: list, critiques: list) -> str:
        lines = ["[Phase 1 — 에이전트 분석]"]
        for r in reports:
            lines.append(
                f"\n{r['avatar']} {r['agent_name']} ({r['stance']} / 확신도:{r['confidence_score']})\n"
                f"  분석: {r['analysis'][:300]}\n"
                f"  주목섹터: {', '.join(r.get('top_sectors', []))}"
            )
        lines.append("\n[Phase 2 — 교차 반론]")
        for c in critiques:
            lines.append(f"\n  {c['from_agent']} → {c['to_agent']}: {c['critique'][:200]}")
        return "\n".join(lines)

    def _format_sector_ranking(self, etf_data: dict) -> str:
        lines = ["[섹터 등락 랭킹 (1일 기준 TOP10)]"]
        for item in etf_data.get("sector_ranking_1d", [])[:10]:
            sec = item["sector"]
            avg = item["avg_ret"]
            info = etf_data.get("sector_summary", {}).get(sec, {})
            top  = ", ".join(e["name"] for e in info.get("top_etfs", [])[:2])
            lines.append(f"  {sec:12s} 1d:{avg:+.2f}%  대표ETF: {top}")
        return "\n".join(lines)
