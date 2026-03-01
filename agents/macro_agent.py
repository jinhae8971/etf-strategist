from .base_agent import BaseAgent

class MacroAgent(BaseAgent):
    def __init__(self):
        super().__init__("MacroAgent")
    
    def analyze(self, data):
        # Macro-economic analysis
        return {"agent": self.name, "analysis": "macro analysis"}
    
    def get_recommendation(self):
        return "macro recommendation"
