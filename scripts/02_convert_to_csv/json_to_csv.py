import json
import csv
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS

sys.path.insert(0, str(Path(__file__).parent.parent))

def main():

    if 'run_etl.py' in ' '.join(sys.argv):
        input_path = Path('../../data/raw/hh.json')
        output_path = Path('../../data/raw/hh.csv')
    else:
        input_path = PATHS['raw_json']
        output_path = PATHS['raw_csv']
    
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    headers = ["id", "created_at", "name", "has_test", "alternate_url"]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:     # prevents automatic newline conversion
        writer = csv.writer(f)
        writer.writerow(headers)
        
        for item in data.get('items', []):   # safely access 'items' with fallback to empty list
            row = [item.get(field) for field in headers]
            writer.writerow(row)
    


if __name__ == '__main__':
    main()