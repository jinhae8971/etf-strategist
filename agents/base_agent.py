"""
BaseAgent: 모든 ETF 분석 에이전트의 공통 인터페이스
AgentReport / AgentCritique 데이터클래스 정의 포함
"""

import json
import re
from dataclasses import dataclass, field
from typing import List, Optional
import anthropic


# ─── 데이터클래스 ───────────────────────────────────────────────────────────

@dataclass
class AgentReport:
    agent_name:       str
    role:             str
    avatar:           str
    analysis:         str
    key_points:       List[str]
    confidence_score: int          # 0~100
    stance:           str          # 집중 / 유지 / 관망
    top_sectors:      List[str]    # 주목 섹터 (최대 3개)
    watch_etfs:       List[dict]   # 주목 ETF [{ticker, name, reason}]

    def to_dict(self) -> dict:
        return {
            "agent_name":       self.agent_name,
            "role":             self.role,
            "avatar":           self.avatar,
            "analysis":         self.analysis,
            "key_points":       self.key_points,
            "confidence_score": self.confidence_score,
            "stance":           self.stance,
            "top_sectors":      self.top_sectors,
            "watch_etfs":       self.watch_etfs,
        }


@dataclass
class AgentCritique:
    from_agent: str
    to_agent:   str
    critique:   str   # 반론 텍스트

    def to_dict(self) -> dict:
        return {
            "from_agent": self.from_agent,
            "to_agent":   self.to_agent,
            "critique":   self.critique,
        }


# ─── BaseAgent ──────────────────────────────────────────────────────────────

class BaseAgent:
    def __init__(self, client: anthropic.Anthropic, model: str):
        self.client        = client
        self.model         = model
        self.name:   str   = ""
        self.role:   str   = ""
        self.avatar: str   = "🤖"
        self.system_prompt: str = ""

    # ── 공개 인터페이스 ──────────────────────────────────────────────────────

    def analyze(self, etf_data: dict) -> AgentReport:
        raise NotImplementedError

    def critique(self, other: AgentReport, etf_data: dict) -> AgentCritique:
        raise NotImplementedError

    # ── 내부 유틸 ────────────────────────────────────────────────────────────

    def _call_llm(self, messages: list, max_tokens: int = 2048) -> str:
        resp = self.client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=self.system_prompt,
            messages=messages,
        )
        return resp.content[0].text

    def _parse_json(self, text: str) -> dict:
        """마크다운 코드블록 제거 후 JSON 파싱. 실패 시 빈 dict 반환."""
        text = re.sub(r"```(?:json)?\s*", "", text)
        text = re.sub(r"```\s*",          "", text).strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                pass
        return {}

    def _sector_summary_text(self, etf_data: dict) -> str:
        """섹터별 등락 현황을 압축 텍스트로 변환 (LLM 토큰 절약)"""
        lines = ["[섹터 등락 현황 (1일 평균 기준)]"]
        for item in etf_data.get("sector_ranking_1d", []):
            sec = item["sector"]
            avg = item["avg_ret"]
            info = etf_data["sector_summary"].get(sec, {})
            lines.append(
                f"  {sec:12s} 1d:{avg:+.2f}%  "
                f"1w:{info.get('avg_ret_1w') or 0:+.2f}%  "
                f"1m:{info.get('avg_ret_1m') or 0:+.2f}%  "
                f"YTD:{info.get('avg_ret_ytd') or 0:+.2f}%"
            )
        return "\n".join(lines)

    def _top_etf_text(self, etf_data: dict, n: int = 5) -> str:
        """상위/하위 ETF 요약 텍스트"""
        lines = [f"[상승 상위 {n}종목]"]
        for e in etf_data.get("top_gainers_1d", [])[:n]:
            lines.append(f"  {e['ticker']} {e['name']} 1d:{e['ret_1d']:+.2f}%")
        lines.append(f"[하락 상위 {n}종목]")
        for e in etf_data.get("top_losers_1d", [])[:n]:
            lines.append(f"  {e['ticker']} {e['name']} 1d:{e['ret_1d']:+.2f}%")
        return "\n".join(lines)

    def _market_overview_text(self, etf_data: dict) -> str:
        mo = etf_data.get("market_overview", {})
        return (
            f"[시장 개요] 전체:{etf_data.get('total_etf_count',0)}종목  "
            f"상승:{mo.get('advancing',0)}  하락:{mo.get('declining',0)}  "
            f"보합:{mo.get('unchanged',0)}  시장평균:{mo.get('avg_ret_1d',0):+.2f}%"
        )

    def _build_base_context(self, etf_data: dict) -> str:
        return "\n".join([
            self._market_overview_text(etf_data),
            "",
            self._sector_summary_text(etf_data),
            "",
            self._top_etf_text(etf_data),
        ])
