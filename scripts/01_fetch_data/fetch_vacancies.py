import sys
import requests
import json
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS


def main():
    if len(sys.argv) != 2:
        print("Write: python fetch_vacancies.py 'vacancy title'")
        exit(1)
    
    vacancy_name = sys.argv[1]
    
    vacancy_encoded = urllib.parse.quote(vacancy_name)   # encode for URL
    
    url = f"https://api.hh.ru/vacancies?text={vacancy_encoded}&per_page=20"
    
    response = requests.get(url)
    response.raise_for_status() 

    data = response.json()   # parse JSON response into dictionary
    
    if 'run_etl.py' in ' '.join(sys.argv):
        output_path = Path('../../data/raw/hh.json')
    else:
        output_path = PATHS['raw_json']
        
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2) 

        # ensure_ascii=False allow non-ASCII characters (Cyrillic, CJK, etc.)
    

if __name__ == '__main__':
    main()