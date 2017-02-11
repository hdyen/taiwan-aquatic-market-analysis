import json

with open('aquatic_trans_data.json') as f:
    aquatic_trans_data = json.load(f)[0]
    markets = aquatic_trans_data['markets'].items()
    fish_types = aquatic_trans_data['fish_types'].items()


