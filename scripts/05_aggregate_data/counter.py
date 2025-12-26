import csv
import sys
import sqlite3
from pathlib import Path
import logging
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PATHS


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)



def save_to_sqlite(data, db_path):

    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancy_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_name TEXT,
                count INTEGER,
                percentage TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("DELETE FROM vacancy_stats")
        
        
        for position, count, percentage in data:
            cur.execute(
                "INSERT INTO vacancy_stats (position_name, count, percentage) VALUES (?, ?, ?)",
                (position, count, percentage)
            )
        
        conn.commit()
        
        
        cur.execute("SELECT COUNT(*) FROM vacancy_stats")
        count_in_db = cur.fetchone()[0]
        
        conn.close()
        
        logger.info(f"Saved {count_in_db} records")
        return True
        
    except Exception as e:
        logger.error(f"SQLite operation error: {e}")
        return False

def main():

    if 'run_etl.py' in ' '.join(sys.argv):
        input_file = Path('../../data/processed/hh_positions.csv')
        output_file = Path('../../data/processed/hh_uniq_positions.csv')
        db_path = Path('../../data/databases/hh_vacancies.db')
    else:
        input_file = PATHS['processed_positions']
        output_file = PATHS['processed_uniq']
        db_path = PATHS['database']

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        counts = {}
        for row in reader:
            position = row['name']
            counts[position] = counts.get(position, 0) + 1

    total = sum(counts.values())
    data_for_db = []
    data_for_csv = []
    
    for position, count in counts.items():
        percentage = f"{(count / total) * 100:.1f}%"
        data_for_db.append((position, count, percentage))
        data_for_csv.append([position, count, percentage])

    # CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['name', 'count', 'percentage'])
        writer.writerows(data_for_csv)

    # SQLite
    if save_to_sqlite(data_for_db, db_path):
        logger.info("Data successfully saved to SQLite database")
    else:
        logger.warning("Data saved only to CSV")

if __name__ == '__main__':
    main()