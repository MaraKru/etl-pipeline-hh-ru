import csv
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS


def main():

    if 'run_etl.py' in ' '.join(sys.argv):
        input_file = Path('../../data/processed/hh_sorted.csv')
        output_file = Path('../../data/processed/hh_positions.csv')
    else:
        input_file = PATHS['processed_sorted']
        output_file = PATHS['processed_positions']

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)


    for row in rows:
        name = row['name'].lower()
        if 'junior' in name:
            row['name'] = 'Junior'
        elif 'middle' in name:
            row['name'] = 'Middle' 
        elif 'senior' in name:
            row['name'] = 'Senior'
        else:
            row['name'] = '-'


    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    main()

