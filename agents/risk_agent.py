from .base_agent import BaseAgent

class RiskAgent(BaseAgent):
    def __init__(self):
        super().__init__("RiskAgent")
    
    def analyze(self, data):
        # Risk analysis
        return {"agent": self.name, "analysis": "risk analysis"}
    
    def get_recommendation(self):
        return "risk recommendation"
