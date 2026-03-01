"""
DebateEngine: Phase 1(독립 분석) + Phase 2(교차 반론) 실행기

교차 반론 페어링:
  트렌드(0) ↔ 리스크(3)   : 모멘텀 낙관 vs 하방 리스크
  섹터(1)   ↔ 매크로(2)   : 바텀업 vs 탑다운
"""

import logging
from agents.base_agent import AgentReport, AgentCritique

logger = logging.getLogger(__name__)

# (from_idx, to_idx) — 읽는 법: from이 to의 분석을 반론
CRITIQUE_PAIRS = [(0, 3), (1, 2), (2, 1), (3, 0)]


class DebateEngine:
    def __init__(self, agents: list):
        self.agents = agents  # [TrendAgent, SectorAgent, MacroAgent, RiskAgent]

    def run(self, etf_data: dict) -> dict:
        # ── Phase 1: 독립 분석 ─────────────────────────────────────────────
        reports: list[AgentReport] = []
        for agent in self.agents:
            try:
                logger.info(f"[Phase 1] {agent.avatar} {agent.name} 분석 시작")
                report = agent.analyze(etf_data)
                reports.append(report)
                logger.info(
                    f"[Phase 1] {agent.name} 완료 — 스탠스:{report.stance} "
                    f"확신도:{report.confidence_score} 주목섹터:{report.top_sectors}"
                )
            except Exception as e:
                logger.error(f"[Phase 1] {agent.name} 실패: {e}")
                reports.append(AgentReport(
                    agent_name=agent.name, role=agent.role, avatar=agent.avatar,
                    analysis=f"분석 실패: {str(e)[:100]}",
                    key_points=["분석 불가"],
                    confidence_score=0, stance="유지",
                    top_sectors=[], watch_etfs=[],
                ))

        # ── Phase 2: 교차 반론 ─────────────────────────────────────────────
        critiques: list[AgentCritique] = []
        for from_idx, to_idx in CRITIQUE_PAIRS:
            if from_idx >= len(self.agents) or to_idx >= len(reports):
                continue
            from_agent  = self.agents[from_idx]
            target_rep  = reports[to_idx]
            try:
                logger.info(
                    f"[Phase 2] {from_agent.avatar} {from_agent.name} → "
                    f"{target_rep.agent_name} 반론"
                )
                critique = from_agent.critique(target_rep, etf_data)
                critiques.append(critique)
            except Exception as e:
                logger.error(f"[Phase 2] 반론 실패 ({from_agent.name}→{target_rep.agent_name}): {e}")
                critiques.append(AgentCritique(
                    from_agent=from_agent.name,
                    to_agent=target_rep.agent_name,
                    critique=f"반론 생성 실패: {str(e)[:80]}",
                ))

        return {
            "phase1_reports":   [r.to_dict() for r in reports],
            "phase2_critiques": [c.to_dict() for c in critiques],
        }
