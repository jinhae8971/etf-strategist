from .base_agent import BaseAgent

class TrendAgent(BaseAgent):
    def __init__(self):
        super().__init__("TrendAgent")
    
    def analyze(self, data):
        # Trend analysis
        return {"agent": self.name, "analysis": "trend analysis"}
    
    def get_recommendation(self):
        return "trend recommendation"
