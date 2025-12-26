from pathlib import Path

STORAGE_BASE = Path('/tmp/airflow_data')

PATHS = {
    'raw_json': STORAGE_BASE / 'raw' / 'hh.json',
    'raw_csv': STORAGE_BASE / 'raw' / 'hh.csv',
    'processed_sorted': STORAGE_BASE / 'processed' / 'hh_sorted.csv',
    'processed_positions': STORAGE_BASE / 'processed' / 'hh_positions.csv', 
    'processed_uniq': STORAGE_BASE / 'processed' / 'hh_uniq_positions.csv',
    'database': STORAGE_BASE / 'databases' / 'hh_vacancies.db'
}