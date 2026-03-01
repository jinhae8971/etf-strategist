import sys
sys.path.insert(0, '.')

from agents import MacroAgent, TrendAgent, SectorAgent, RiskAgent
from orchestrator import DebateEngine, Moderator
from scripts.collect_etf_data import collect_etf_data

def main():
    # Collect ETF data
    data = collect_etf_data()
    
    # Initialize agents
    agents = [
        MacroAgent(),
        TrendAgent(),
        SectorAgent(),
        RiskAgent()
    ]
    
    # Start debate
    debate_engine = DebateEngine()
    for agent in agents:
        debate_engine.add_agent(agent)
    
    analyses = debate_engine.start_debate(data)
    
    # Moderate results
    moderator = Moderator()
    final_recommendation = moderator.moderate(analyses)
    
    print("ETF Strategist Pipeline Results:")
    print(final_recommendation)

if __name__ == "__main__":
    main()
