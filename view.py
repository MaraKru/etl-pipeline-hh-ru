import sqlite3
import pandas as pd
import sys
from pathlib import Path

def view_database():
    try:
        if 'run_etl.py' in ' '.join(sys.argv):
            db_path = 'data/databases/hh_vacancies.db'  # local path
        else:
            # for Airflow or direct execution
            db_path = '/tmp/airflow_data/databases/hh_vacancies.db'
        
        print(f"Looking for database at: {db_path}")
        
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM vacancy_stats", conn)
        
        print("Data from SQLite database:")
        print(df)
        print(f"Total records: {len(df)}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    view_database()