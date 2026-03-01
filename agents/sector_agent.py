from .base_agent import BaseAgent

class SectorAgent(BaseAgent):
    def __init__(self):
        super().__init__("SectorAgent")
    
    def analyze(self, data):
        # Sector analysis
        return {"agent": self.name, "analysis": "sector analysis"}
    
    def get_recommendation(self):
        return "sector recommendation"
