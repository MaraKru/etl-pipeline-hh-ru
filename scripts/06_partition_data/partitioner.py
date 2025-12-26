import csv
import sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS

sys.path.insert(0, str(Path(__file__).parent.parent))


def main():

    if 'run_etl.py' in ' '.join(sys.argv):
        input_file = Path('../../data/processed/hh_positions.csv')
        output_dir = Path('../../data/partitioned')
    else:
        from config import PATHS
        input_file = PATHS['processed_positions']
        output_dir = Path('/tmp/airflow_data/partitioned')

    output_dir.mkdir(parents=True, exist_ok=True)


    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)  # loads ALL data into memory


    files_by_date = {}
    for row in data:
        date = row['created_at'].split('T')[0]
        
        if date not in files_by_date:
            files_by_date[date] = []
        files_by_date[date].append(row)


    for date, rows in files_by_date.items():
        output_file = output_dir / f'{date}.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(rows)



if __name__ == '__main__':
    main()

