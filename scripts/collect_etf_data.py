import json
from datetime import datetime

def collect_etf_data():
    '''Collect ETF market data'''
    data = {
        "timestamp": datetime.now().isoformat(),
        "etfs": []
    }
    return data

if __name__ == "__main__":
    data = collect_etf_data()
    print(json.dumps(data, indent=2))
