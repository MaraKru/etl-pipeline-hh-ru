import csv
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS

sys.path.insert(0, str(Path(__file__).parent.parent))

def main():

    if 'run_etl.py' in ' '.join(sys.argv):
        input_file = Path('../../data/raw/hh.csv')
        output_file = Path('../../data/processed/hh_sorted.csv')
    else:
        input_file = PATHS['raw_csv']
        output_file = PATHS['processed_sorted']

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)


    data_sorted = sorted(data, key=lambda x: (x['created_at'], x['id']))     # sort by date and ID


    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()        # write header
        writer.writerows(data_sorted)


if __name__ == '__main__':
    main()
