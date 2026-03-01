class DebateEngine:
    def __init__(self):
        self.agents = []
    
    def add_agent(self, agent):
        self.agents.append(agent)
    
    def start_debate(self, data):
        analyses = []
        for agent in self.agents:
            analysis = agent.analyze(data)
            analyses.append(analysis)
        return analyses
