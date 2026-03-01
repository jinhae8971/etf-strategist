class BaseAgent:
    def __init__(self, name):
        self.name = name
    
    def analyze(self, data):
        raise NotImplementedError("Subclasses must implement analyze method")
    
    def get_recommendation(self):
        raise NotImplementedError("Subclasses must implement get_recommendation method")
